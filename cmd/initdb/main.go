package main

import (
	"avail-watcher/db"

	"github.com/cockroachdb/errors"
	logging "github.com/ipfs/go-log/v2"
	_ "github.com/joho/godotenv/autoload"
)

var log = logging.Logger("mai")

func main() {
	err := run()
	if err != nil {
		log.Panicln(err)
	}
}

func run() error {
	database, err := db.GetDB()
	if err != nil {
		return errors.WithStack(err)
	}
	err = database.AutoMigrate(&db.Block{}, &db.Manifest{})
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}
