package main

import (
	"avail-watcher/db"
	"avail-watcher/util"
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/cockroachdb/errors"
	"github.com/gin-gonic/gin"
	"github.com/ipfs/go-log/v2"
	_ "github.com/joho/godotenv/autoload"
)

var logger = log.Logger("api")

func main() {
	if err := start(); err != nil {
		logger.Fatalw("failed to start: %+v", err)
	}
}

func start() error {
	database, err := db.GetDB()
	if err != nil {
		return errors.WithStack(err)
	}
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return errors.WithStack(err)
	}
	s3Client := s3.NewFromConfig(cfg)

	route := gin.Default()
	route.GET("/:blockchain/:field/:value", func(c *gin.Context) {
		ctx := c.Request.Context()
		blockchain := c.Param("blockchain")
		field := c.Param("field")
		value := c.Param("value")

		var networkType db.NetworkType
		err := networkType.Parse(blockchain)
		if err != nil {
			c.JSON(404, gin.H{"error": err.Error()})
			return
		}

		var block db.Block
		statement := database.WithContext(ctx)
		switch field {
		case "height":
			statement = statement.First(&block, "network = ? AND height = ?", networkType, value)
		case "hash":
			statement = statement.First(&block, "network = ? AND hash = ?", networkType, value)
		default:
			c.JSON(404, gin.H{"error": "invalid field"})
			return
		}
		if err := statement.Error; err != nil {
			c.JSON(404, gin.H{"error": err.Error()})
			return
		}

		var manifest db.Manifest
		if err := database.WithContext(ctx).
			First(&manifest, "cid = ?", block.Cid).Error; err != nil {
			logger.Errorw("err", err, "cid", block.Cid)
			c.JSON(500, "internal error")
		}

		bucket, key, err := util.ParseS3URL(manifest.S3Url)
		if err != nil {
			logger.Errorw("err", err, "s3URL", manifest.S3Url)
			c.JSON(500, "internal error")
			return
		}
		rangeHeader := fmt.Sprintf("bytes=%d-%d", manifest.Offset, manifest.Offset+manifest.Length-1)
		response, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: &bucket,
			Key:    &key,
			Range:  &rangeHeader,
		})
		if err != nil {
			logger.Errorw("err", err, "bucket", bucket, "key", key)
			c.JSON(500, "internal error")
			return
		}
		defer response.Body.Close()
		c.DataFromReader(200, int64(manifest.Length), "application/json", response.Body, nil)
		return
	})

	return route.Run(":8080")
}
