package main

import (
	"github.com/cockroachdb/errors"
	"github.com/gin-gonic/gin"
	"github.com/ipfs/go-log/v2"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type Mapping struct {
	gorm.Model
	BlockNumber uint64
	BlockHash   string
	Cid         string
}

var logger = log.Logger("main")

func main() {
	log.SetAllLoggers(log.LevelInfo)
	if err := start(); err != nil {
		logger.Fatalw("failed to start: %+v", err)
	}
}

func start() error {
	db, err := gorm.Open(sqlite.Open("mapping.db"), &gorm.Config{})
	if err != nil {
		return errors.WithStack(err)
	}

	route := gin.Default()
	route.GET("/avail/blockNumber/:blockNumber", func(c *gin.Context) {
		blockNumber := c.Param("blockNumber")
		var mapping Mapping
		if err := db.First(&mapping, "block_number = ?", blockNumber).Error; err != nil {
			c.JSON(404, gin.H{"error": err.Error()})
			return
		}
		cid := mapping.Cid
		c.Redirect(302, "https://ipfs.io/ipfs/"+cid+"?format=dag-json")
	})

	route.GET("/avail/blockHash/:blockHash", func(c *gin.Context) {
		blockHash := c.Param("blockHash")
		var mapping Mapping
		if err := db.First(&mapping, "block_hash = ?", blockHash).Error; err != nil {
			c.JSON(404, gin.H{"error": err.Error()})
			return
		}
		cid := mapping.Cid
		c.Redirect(302, "https://ipfs.io/ipfs/"+cid+"?format=dag-json")
	})

	return route.Run(":8080")
}
