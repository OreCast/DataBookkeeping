package main

import (
	"compress/gzip"
	"log"
	"net/http"
	"strings"

	"github.com/OreCast/DataBookkeeping/dbs"
	"github.com/OreCast/DataBookkeeping/utils"
	"github.com/gin-gonic/gin"
)

type DatasetRequest struct {
	Name string `json:"name"`
}

// DatasetHandler provives access to GET /datasets end-point
func DatasetHandler(c *gin.Context) {
	//     c.JSON(200, gin.H{"status": "ok"})
	r := c.Request
	w := c.Writer
	if r.Method == "POST" {
		DBSPostHandler(w, r, "datasets")
	} else if r.Method == "PUT" {
		DBSPutHandler(w, r, "datasets")
	} else {
		DBSGetHandler(w, r, "datasets")
	}

}

// DatasetPostHandler provides access to POST /datasets end-point
func DatasetPostHandler(c *gin.Context) {
	var data DatasetRequest
	err := c.BindJSON(&data)
	if err == nil {
		c.JSON(200, gin.H{"status": "ok"})
	} else {
		c.JSON(400, gin.H{"status": "fail", "error": err.Error()})
	}
}

// DBSGetHandler is a generic Get handler to call DBS Get APIs.
//
//gocyclo:ignore
func DBSGetHandler(w http.ResponseWriter, r *http.Request, a string) {
	// all outputs will be added to output list
	sep := ","
	if r.Header.Get("Accept") == "application/ndjson" {
		sep = ""
	}
	if sep != "" {
		w.Header().Add("Content-Type", "application/json")
	} else {
		w.Header().Add("Content-Type", "application/ndjson")
	}

	params, err := parseParams(r)
	if err != nil {
		responseMsg(w, r, err, http.StatusBadRequest)
		return
	}
	if utils.VERBOSE > 0 {
		dn, _ := r.Header["Cms-Authn-Dn"]
		log.Printf("DBSGetHandler: API=%s, dn=%s, uri=%+v, params: %+v", a, dn, requestURI(r), params)
	}
	api := &dbs.API{
		Writer:    w,
		Params:    params,
		Separator: sep,
		Api:       a,
	}
	if strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
		w.Header().Set("Content-Encoding", "gzip")
		gw := gzip.NewWriter(w)
		defer gw.Close()
		api.Writer = utils.GzipWriter{GzipWriter: gw, Writer: w}
	}
	if utils.VERBOSE > 0 {
		log.Println(api.String())
	}
	if a == "datasets" {
		err = api.Datasets()
	} else if a == "files" {
		err = api.Files()
	} else {
		err = dbs.NotImplementedApiErr
	}
	if err != nil {
		responseMsg(w, r, err, http.StatusBadRequest)
		return
	}
}

// DBSPostHandler is a generic Post Handler to call DBS Post APIs
//
//gocyclo:ignore
func DBSPostHandler(w http.ResponseWriter, r *http.Request, a string) {
}

// DBSPutHandler is a generic Post Handler to call DBS Post APIs
//
//gocyclo:ignore
func DBSPutHandler(w http.ResponseWriter, r *http.Request, a string) {
}
