package main

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/OreCast/DataBookkeeping/dbs"
	"github.com/OreCast/DataBookkeeping/utils"
	"github.com/gin-gonic/gin"
	validator "github.com/go-playground/validator/v10"
)

func setupRouter() *gin.Engine {
	// Disable Console Color
	// gin.DisableConsoleColor()
	r := gin.Default()

	// GET routes
	r.GET("/dataset", DatasetHandler)

	// POST routes
	r.POST("/dataset", DatasetPostHandler)

	return r
}

// helper function to initialize DB access
func dbInit(dbtype, dburi string) (*sql.DB, error) {
	db, dberr := sql.Open(dbtype, dburi)
	if dberr != nil {
		log.Printf("unable to open %s, error %v", dbtype, dburi)
		return nil, dberr
	}
	dberr = db.Ping()
	if dberr != nil {
		log.Println("DB ping error", dberr)
		return nil, dberr
	}
	db.SetMaxOpenConns(Config.MaxDBConnections)
	db.SetMaxIdleConns(Config.MaxIdleConnections)
	// Disables connection pool for sqlite3. This enables some concurrency with sqlite3 databases
	// See https://stackoverflow.com/questions/57683132/turning-off-connection-pool-for-go-http-client
	// and https://sqlite.org/wal.html
	// This only will apply to sqlite3 databases
	if dbtype == "sqlite3" {
		db.Exec("PRAGMA journal_mode=WAL;")
	}
	return db, nil
}

func Server(configFile string) {
	// initialize record validator
	dbs.RecordValidator = validator.New()

	// set database connection once
	log.Println("parse Config.DBFile:", Config.DBFile)
	dbtype, dburi, dbowner := dbs.ParseDBFile(Config.DBFile)
	// for oci driver we know it is oracle backend
	if strings.HasPrefix(dbtype, "oci") {
		utils.ORACLE = true
	}
	log.Println("DBOWNER", dbowner)
	// setup DBS
	db, dberr := dbInit(dbtype, dburi)
	if dberr != nil {
		log.Fatal(dberr)
	}
	dbs.DB = db
	dbs.DBTYPE = dbtype
	defer dbs.DB.Close()

	r := setupRouter()
	sport := fmt.Sprintf(":%d", Config.Port)
	log.Printf("Start HTTP server %s", sport)
	r.Run(sport)
}
