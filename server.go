package main

// DBS server
// Copyright (c) 2023 - Valentin Kuznetsov <vkuznet@gmail.com>
//
//
// Some links:  http://www.alexedwards.net/blog/golang-response-snippets
//              http://blog.golang.org/json-and-go
// Go patterns: http://www.golangpatterns.info/home
// Templates:   http://gohugo.io/templates/go-templates/
//              http://golang.org/pkg/html/template/
// Go examples: https://gobyexample.com/
// for Go database API: http://go-database-sql.org/overview.html
// Oracle drivers:
//   _ "gopkg.in/rana/ora.v4"
//   _ "github.com/mattn/go-oci8"
// MySQL driver:
//   _ "github.com/go-sql-driver/mysql"
// SQLite driver:
//  _ "github.com/mattn/go-sqlite3"
//
// Get profile's output
// visit http://localhost:<port>/debug/pprof
// or generate png plots
// go tool pprof -png http://localhost:<port>/debug/pprof/heap > /tmp/heap.png
// go tool pprof -png http://localhost:<port>/debug/pprof/profile > /tmp/profile.png

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/OreCast/DataBookkeeping/dbs"
	"github.com/OreCast/DataBookkeeping/utils"
	authz "github.com/OreCast/common/authz"
	"github.com/gin-gonic/gin"
	validator "github.com/go-playground/validator/v10"

	// GO profiler
	_ "net/http/pprof"

	// imports for supported DB drivers
	// go-sqlite driver
	_ "github.com/mattn/go-sqlite3"
)

func setupRouter() *gin.Engine {
	// Disable Console Color
	// gin.DisableConsoleColor()
	r := gin.Default()

	// GET routes
	r.GET("/datasets", DatasetHandler)
	r.GET("/files", FileHandler)

	// individual routes
	r.GET("/dataset/:name", DatasetHandler)
	r.GET("/file/:name", FileHandler)

	// all POST/PUT/DELET methods ahould be authorized
	authorized := r.Group("/")
	authorized.Use(authz.TokenMiddleware(Config.AuthzClientId, Config.Verbose))
	{
		// POST routes
		authorized.POST("/dataset", DatasetHandler)
		authorized.POST("/file", FileHandler)

		// PUT routes
		authorized.PUT("/dataset/:name", DatasetHandler)
		authorized.PUT("/file/:name", FileHandler)

		// DELETE routes
		authorized.DELETE("/dataset/:name", DatasetHandler)
		authorized.DELETE("/file/:name", FileHandler)
	}

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
	// be verbose
	log.SetFlags(log.LstdFlags | log.Lshortfile)

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
	// set static dir
	utils.STATICDIR = Config.StaticDir
	utils.VERBOSE = Config.Verbose

	// setup DBS
	db, dberr := dbInit(dbtype, dburi)
	if dberr != nil {
		log.Fatal(dberr)
	}
	dbs.DB = db
	dbs.DBTYPE = dbtype
	dbsql := dbs.LoadSQL(dbowner)
	dbs.DBSQL = dbsql
	dbs.DBOWNER = dbowner
	defer dbs.DB.Close()

	r := setupRouter()
	sport := fmt.Sprintf(":%d", Config.Port)
	log.Printf("Start HTTP server %s", sport)
	r.Run(sport)
}
