package db

import (
	"fmt"
	"net/url"
	"os"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func GetDB() (*gorm.DB, error) {
	pgHost := os.Getenv("PG_HOST")
	pgPort := os.Getenv("PG_PORT")
	pgUser := os.Getenv("PG_USER")
	pgPassword := os.Getenv("PG_PASSWORD")
	pgDatabase := os.Getenv("PG_DATABASE")
	connStr := fmt.Sprintf("postgres://%s@%s:%s/%s", url.UserPassword(pgUser, pgPassword).String(), pgHost, pgPort, pgDatabase)
	return gorm.Open(postgres.Open(connStr))
}
