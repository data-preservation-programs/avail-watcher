package main

import (
	"avail-watcher/db"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	gsrpc "github.com/centrifuge/go-substrate-rpc-client/v4"
	"github.com/cockroachdb/errors"
	"github.com/ipfs/go-cid"
	u "github.com/ipfs/go-ipfs-util"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-car"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	"gorm.io/gorm"
)
import _ "github.com/joho/godotenv/autoload"

var log = logging.Logger("mai")

func main() {
	r, err := newRunner()
	if err != nil {
		log.Panicln(err)
	}
	err = r.run()
	if err != nil {
		log.Panicln(err)
	}
}

type runner struct {
	database    *gorm.DB
	api         *gsrpc.SubstrateAPI
	networkType db.NetworkType
	s3Client    *s3.Client
	bucket      string
	folder      string
}

func newRunner() (*runner, error) {
	database, err := db.GetDB()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	api, err := gsrpc.NewSubstrateAPI(os.Getenv("RPC_API"))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	var networkType db.NetworkType
	networkType.FromString(os.Getenv("NETWORK_TYPE"))

	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return nil, errors.WithStack(err)
	}
	client := s3.NewFromConfig(cfg)

	return &runner{
		database:    database,
		api:         api,
		networkType: networkType,
		s3Client:    client,
		bucket:      os.Getenv("S3_BUCKET"),
		folder:      os.Getenv("S3_PATH"),
	}, nil
}

func (r runner) run() error {
	ctx := context.Background()
	var lastBlock db.Block
	var startFrom uint64
	err := r.database.Model(&db.Block{}).
		Where("network = ?", r.networkType).
		Order("height desc").
		Limit(1).
		Take(&lastBlock).
		Error
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return errors.WithStack(err)
	}
	if err == nil {
		startFrom = lastBlock.Height + 1
	}

	subscription, err := r.api.RPC.Chain.SubscribeFinalizedHeads()
	if err != nil {
		return errors.WithStack(err)
	}

	for header := range subscription.Chan() {
		for startFrom <= uint64(header.Number) {
			err = r.processBlock(ctx, startFrom)
			if err != nil {
				return errors.WithStack(err)
			}
			startFrom++
		}
	}

	return nil
}

func (r runner) processBlock(ctx context.Context, blockNumber uint64) error {
	log.Infof("Chain is at block: #%d", blockNumber)
	blockHash, err := r.api.RPC.Chain.GetBlockHash(blockNumber)
	if err != nil {
		return errors.WithStack(err)
	}
	var block interface{}
	err = r.api.Client.Call(&block, "chain_getBlock", blockHash)
	if err != nil {
		return errors.WithStack(err)
	}

	node := bindnode.Wrap(block, nil)
	dataBuf := bytes.NewBuffer(nil)
	err = dagjson.Encode(node.Representation(), dataBuf)
	if err != nil {
		return errors.WithStack(err)
	}
	nodeCid := cid.NewCidV1(cid.DagJSON, u.Hash(dataBuf.Bytes()))
	key := fmt.Sprintf("%s/%d.car", r.folder, blockNumber)
	reader, writer := io.Pipe()
	var dataOffset uint64
	defer reader.Close()
	go func() {
		carHeader := car.CarHeader{
			Roots:   []cid.Cid{nodeCid},
			Version: 1,
		}

		headerBuf := new(bytes.Buffer)
		err := car.WriteHeader(&carHeader, headerBuf)
		if err != nil {
			writer.CloseWithError(err)
			return
		}
		dataOffset += uint64(headerBuf.Len())

		sum := uint64(dataBuf.Len() + nodeCid.ByteLen())
		buf := make([]byte, 8)
		n := binary.PutUvarint(buf, sum)
		_, err = writer.Write(buf[:n])
		if err != nil {
			writer.CloseWithError(err)
			return
		}
		dataOffset += uint64(n)

		_, err = writer.Write(nodeCid.Bytes())
		if err != nil {
			writer.CloseWithError(err)
			return
		}
		dataOffset += uint64(nodeCid.ByteLen())

		_, err = writer.Write(dataBuf.Bytes())
		if err != nil {
			writer.CloseWithError(err)
			return
		}

		writer.Close()
	}()

	_, err = r.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &r.bucket,
		Key:    &key,
		Body:   reader,
	})
	if err != nil {
		return errors.WithStack(err)
	}

	log.Infof("Uploaded to %s", key)
	err = r.database.Transaction(func(tx *gorm.DB) error {
		err := tx.Create(&db.Block{
			Hash:    blockHash.Hex(),
			Network: r.networkType,
			Height:  blockNumber,
			Cid:     nodeCid.String(),
		}).Error
		if err != nil {
			return errors.WithStack(err)
		}
		err = tx.Create(&db.Manifest{
			Cid:    nodeCid.String(),
			S3Url:  fmt.Sprintf("s3://%s/%s", r.bucket, key),
			Offset: dataOffset,
			Length: uint64(dataBuf.Len()),
		}).Error
		if err != nil {
			return errors.WithStack(err)
		}
		return nil
	})
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}
