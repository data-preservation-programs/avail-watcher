package main

import (
	"avail-watcher/db"
	"avail-watcher/util"
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/cockroachdb/errors"
	commp "github.com/filecoin-project/go-fil-commp-hashhash"
	"github.com/google/uuid"
	"github.com/gotidy/ptr"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-car"
	_ "github.com/joho/godotenv/autoload"
	"github.com/rjNemo/underscore"
	"gorm.io/gorm"
)

var log = logging.Logger("main")

func main() {
	r, err := newRunner()
	if err != nil {
		log.Panicln(err)
	}
	err = r.run(context.TODO())
	if err != nil {
		log.Panicln(err)
	}
}

type runner struct {
	database        *gorm.DB
	s3Client        *s3.Client
	networkType     db.NetworkType
	bucket          string
	folder          string
	minCarSize      uint64
	maxCarSize      uint64
	interval        time.Duration
	manifestBucket  string
	manifestFolder  string
	targetPieceSize uint64
}

func newRunner() (*runner, error) {
	database, err := db.GetDB()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	var networkType db.NetworkType
	networkType.MustParse(os.Getenv("NETWORK_TYPE"))

	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return nil, errors.WithStack(err)
	}
	client := s3.NewFromConfig(cfg)

	minCarSize, err := strconv.ParseUint(os.Getenv("MIN_CAR_SIZE"), 10, 64)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	maxCarSize, err := strconv.ParseUint(os.Getenv("MAX_CAR_SIZE"), 10, 64)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	interval, err := time.ParseDuration(os.Getenv("INTERVAL"))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	targetPieceSize, err := strconv.ParseUint(os.Getenv("TARGET_PIECE_SIZE"), 10, 64)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if targetPieceSize != util.NextPowerOfTwo(targetPieceSize) {
		return nil, errors.Newf("target piece size must be a power of two: %d", targetPieceSize)
	}

	return &runner{
		database:        database,
		s3Client:        client,
		networkType:     networkType,
		bucket:          os.Getenv("S3_BUCKET"),
		folder:          os.Getenv("S3_PATH"),
		manifestBucket:  os.Getenv("MANIFEST_BUCKET"),
		manifestFolder:  os.Getenv("MANIFEST_PATH"),
		maxCarSize:      maxCarSize,
		minCarSize:      minCarSize,
		interval:        interval,
		targetPieceSize: targetPieceSize,
	}, nil
}

func (r runner) run(ctx context.Context) error {
	for {
		files, err := r.fetchFiles(ctx)
		if err != nil {
			return errors.WithStack(err)
		}
		if len(files) > 0 {
			r.createFile(ctx, files)
			if err != nil {
				return errors.WithStack(err)
			}
		} else {
			log.Info("Files not large enough to fill up a sector")
		}
		log.Infof("Going to sleep at %s, will try again in %s", r.interval.String())
		time.Sleep(r.interval)
	}

	return nil
}

func (r runner) fetchFiles(ctx context.Context) ([]types.Object, error) {
	var files []types.Object
	var continuationToken *string = nil
	var size uint64
	var full bool
	for {
		output, err := r.s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            &r.bucket,
			ContinuationToken: continuationToken,
			Prefix:            &r.folder,
		})
		if err != nil {
			return nil, errors.WithStack(err)
		}

		for _, object := range output.Contents {
			if !strings.HasSuffix(*object.Key, ".car") {
				continue
			}
			if size+uint64(*object.Size) > r.maxCarSize {
				full = true
				break
			}
			size += r.maxCarSize
			files = append(files, object)
			if size >= r.minCarSize {
				full = true
				break
			}
		}

		if output.NextContinuationToken != nil {
			continuationToken = output.NextContinuationToken
		} else {
			break
		}
	}

	log.Infof("Fetched %d files, total size: %d", len(files), size)
	if full {
		return files, nil
	}
	return nil, nil
}

func (r runner) createFile(ctx context.Context, files []types.Object) error {
	var manifests []db.Manifest
	log.Infof("Creating file with %d objects", len(files))
	var offset uint64
	id := uuid.NewString()
	path := fmt.Sprintf("%s/%s.car", r.manifestFolder, id)
	s3Url := fmt.Sprintf("s3://%s/%s", r.manifestBucket, path)
	httpsUrl := fmt.Sprintf("https://%s.s3.amazonaws.com/%s", r.manifestBucket, path)
	reader, writer := io.Pipe()
	defer reader.Close()
	calc := &commp.Calc{}
	dualReader := io.TeeReader(reader, calc)
	var rootCID cid.Cid
	go func() {
		for i, file := range files {
			log.Infof("Processing file %s", *file.Key)
			output, err := r.s3Client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: &r.bucket,
				Key:    file.Key,
			})
			if err != nil {
				writer.CloseWithError(err)
				return
			}
			carReader, err := car.NewCarReader(output.Body)
			if err != nil {
				writer.CloseWithError(err)
				output.Body.Close()
				return
			}
			if i == 0 {
				n, err := util.WriteCarHeader(writer, *carReader.Header)
				if err != nil {
					writer.CloseWithError(err)
					output.Body.Close()
					return
				}
				offset += uint64(n)
				rootCID = carReader.Header.Roots[0]
			}
			for {
				blk, err := carReader.Next()
				if err == io.EOF {
					break
				}
				if err != nil {
					writer.CloseWithError(err)
					output.Body.Close()
					return
				}
				n, dataOffset, err := util.WriteBlock(writer, blk.Cid(), blk.RawData())
				if err != nil {
					writer.CloseWithError(err)
					output.Body.Close()
					return
				}
				manifests = append(manifests, db.Manifest{
					Cid:    blk.Cid().String(),
					S3Url:  s3Url,
					Offset: offset + uint64(dataOffset),
					Length: uint64(len(blk.RawData())),
				})
				offset += uint64(n)
			}
			output.Body.Close()
		}
		writer.Close()
	}()
	_, err := r.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &r.manifestBucket,
		Key:    &path,
		Body:   dualReader,
	})
	if err != nil {
		return errors.WithStack(err)
	}
	log.Infof("Created file %s with %d objects", path, len(manifests))

	pieceCid, pieceSize, err := util.GetCommp(calc, r.targetPieceSize)
	if err != nil {
		return errors.WithStack(err)
	}

	err = r.database.CreateInBatches(manifests, 100).Error
	if err != nil {
		return errors.WithStack(err)
	}

	err = r.database.Create(&db.Piece{
		PieceCid:  pieceCid.String(),
		PieceSize: pieceSize,
		Url:       httpsUrl,
		Size:      offset,
		RootCid:   rootCID.String(),
		Network:   r.networkType,
	}).Error
	if err != nil {
		return errors.WithStack(err)
	}

	err = r.deleteFiles(ctx, files)
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (r runner) deleteFiles(ctx context.Context, files []types.Object) error {
	chunks := util.ChunkSlice(files, 100)
	for _, chunk := range chunks {
		err := r.database.
			Where("s3_url in ?", underscore.Map(chunk, func(file types.Object) string {
				return fmt.Sprintf("s3://%s/%s", r.bucket, *file.Key)
			})).
			Delete(&db.Manifest{}).Error
		if err != nil {
			return errors.WithStack(err)
		}
		_, err = r.s3Client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: &r.bucket,
			Delete: &types.Delete{
				Objects: underscore.Map(chunk, func(file types.Object) types.ObjectIdentifier {
					return types.ObjectIdentifier{
						Key: file.Key,
					}
				}),
				Quiet: ptr.Of(true),
			},
		})
		if err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}
