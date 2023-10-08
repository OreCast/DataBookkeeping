package dbs

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"

	"github.com/OreCast/DataBookkeeping/utils"
	yaml "gopkg.in/yaml.v2"
)

// Datasets represents Datasets DBS DB table
type Datasets struct {
	DATASET_ID             int64  `json:"dataset_id"`
	DATASET                string `json:"dataset" validate:"required"`
	BUCKET_ID              int64  `json:"bucket_id" validate:"required"`
	META_ID                string `json:"meta_id" validate:"required"`
	SITE_ID                int64  `json:"site_id" validate:"required"`
	PROCESSING_ID          int64  `json:"processing_id" validate:"required"`
	PARENT_ID              int64  `json:"parent_id" validate:"required"`
	CREATION_DATE          int64  `json:"creation_date" validate:"required,number"`
	CREATE_BY              string `json:"create_by" validate:"required"`
	LAST_MODIFICATION_DATE int64  `json:"last_modification_date" validate:"required,number"`
	LAST_MODIFIED_BY       string `json:"last_modified_by" validate:"required"`
}

// DatasetRecord represents input dataset record from HTTP request
type DatasetRecord struct {
	Dataset    string   `json:"dataset" validate:"required"`
	Buckets    []string `json:"buckets" validate:"required"`
	Site       string   `json:"site" validate:"required"`
	Processing string   `json:"processing" validate:"required"`
	Parent     string   `json:"parent_dataset" validate:"required"`
	MetaId     string   `json:"meta_id" validate:"required"`
	Files      []string `json:"files" validate:"required"`
}

// Datasets API
//
//gocyclo:ignore
func (a *API) GetDataset() error {
	if utils.VERBOSE > 1 {
		log.Printf("datasets params %+v", a.Params)
	}
	var args []interface{}
	var conds []string
	tmpl := make(Record)
	tmpl["Owner"] = DBOWNER

	// get SQL statement from static area
	stm, err := LoadTemplateSQL("select_dataset", tmpl)
	if err != nil {
		return Error(err, LoadErrorCode, "", "dbs.datasets.Datasets")
	}
	cols := []string{
		"dataset_id",
		"dataset",
		"meta_id",
		"site_id",
		"processing_id",
		"creation_date",
		"create_by",
		"last_modification_date",
		"last_modified_by"}
	vals := []interface{}{
		new(sql.NullInt64),   // dataset_id
		new(sql.NullString),  // dataset
		new(sql.NullFloat64), // meta_id
		new(sql.NullFloat64), // site_id
		new(sql.NullFloat64), // processing_id
		new(sql.NullFloat64), // creation_date
		new(sql.NullString),  // create_by
		new(sql.NullFloat64), // last_modification_date
		new(sql.NullString)}  //last_modified_by
	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	err = execute(a.Writer, a.Separator, stm, cols, vals, args...)
	if err != nil {
		return Error(err, QueryErrorCode, "", "dbs.datasets.Datasets")
	}
	return nil
}

func (a *API) InsertDataset() error {
	// the API provides Reader which will be used by Decode function to load the HTTP payload
	// and cast it to Datasets data structure

	// read given input
	data, err := io.ReadAll(a.Reader)
	if err != nil {
		log.Println("fail to read data", err)
		return Error(err, ReaderErrorCode, "", "dbs.blocks.InsertBlocks")
	}
	rec := DatasetRecord{}
	if a.ContentType == "application/json" {
		err = json.Unmarshal(data, &rec)
	} else if a.ContentType == "application/yaml" {
		err = yaml.Unmarshal(data, &rec)
	} else {
		log.Println("Parser dataset record using default application/json mtime")
		err = json.Unmarshal(data, &rec)
	}
	if err != nil {
		log.Println("fail to decode data", err)
		return Error(err, UnmarshalErrorCode, "", "dbs.blocks.InsertBlocks")
	}
	log.Printf("### input DatasetRecord %+v", rec)
	// TODO:
	// parse incoming DatasetRequest
	// insert site, bucket, parent, processing, files

	record := Datasets{
		DATASET:          rec.Dataset,
		CREATE_BY:        a.CreateBy,
		LAST_MODIFIED_BY: a.CreateBy,
	}
	return insertRecord(&record, nil)
}
func (a *API) UpdateDataset() error {
	return nil
}
func (a *API) DeleteDataset() error {
	return nil
}

// Insert implementation of Datasets
func (r *Datasets) Insert(tx *sql.Tx) error {
	var tid int64
	var err error
	if r.DATASET_ID == 0 {
		if DBOWNER == "sqlite" {
			tid, err = LastInsertID(tx, "DATASETS", "dataset_id")
			r.DATASET_ID = tid + 1
		} else {
			tid, err = IncrementSequence(tx, "SEQ_DS")
			r.DATASET_ID = tid
		}
	}
	if err != nil {
		return Error(err, LastInsertErrorCode, "", "dbs.datasets.Insert")
	}
	// set defaults and validate the record
	r.SetDefaults()
	err = r.Validate()
	if err != nil {
		log.Println("unable to validate record", err)
		return Error(err, ValidateErrorCode, "", "dbs.datasets.Insert")
	}
	// get SQL statement from static area
	stm := getSQL("insert_dataset")
	if utils.VERBOSE > 0 {
		log.Printf("Insert Datasets\n%s\n%+v", stm, r)
	}
	// make final SQL statement to insert dataset record
	_, err = tx.Exec(
		stm,
		r.DATASET_ID,
		r.DATASET,
		r.BUCKET_ID,
		r.META_ID,
		r.SITE_ID,
		r.PROCESSING_ID,
		r.PARENT_ID,
		r.CREATION_DATE,
		r.CREATE_BY,
		r.LAST_MODIFICATION_DATE,
		r.LAST_MODIFIED_BY)
	if err != nil {
		if utils.VERBOSE > 0 {
			log.Printf("unable to insert Datasets %+v", err)
		}
		return Error(err, InsertErrorCode, "", "dbs.datasets.Insert")
	}
	return nil
}

// Validate implementation of Datasets
//
//gocyclo:ignore
func (r *Datasets) Validate() error {
	if err := CheckPattern("dataset", r.DATASET); err != nil {
		return Error(err, PatternErrorCode, "", "dbs.datasets.Validate")
	}
	if matched := unixTimePattern.MatchString(fmt.Sprintf("%d", r.CREATION_DATE)); !matched {
		msg := "invalid pattern for creation date"
		return Error(InvalidParamErr, PatternErrorCode, msg, "dbs.datasets.Validate")
	}
	if r.CREATION_DATE == 0 {
		msg := "missing creation_date"
		return Error(InvalidParamErr, ParametersErrorCode, msg, "dbs.datasets.Validate")
	}
	if r.CREATE_BY == "" {
		msg := "missing create_by"
		return Error(InvalidParamErr, ParametersErrorCode, msg, "dbs.datasets.Validate")
	}
	if r.LAST_MODIFICATION_DATE == 0 {
		msg := "missing last_modification_date"
		return Error(InvalidParamErr, ParametersErrorCode, msg, "dbs.datasets.Validate")
	}
	if r.LAST_MODIFIED_BY == "" {
		msg := "missing last_modified_by"
		return Error(InvalidParamErr, ParametersErrorCode, msg, "dbs.datasets.Validate")
	}
	return nil
}

// SetDefaults implements set defaults for Datasets
func (r *Datasets) SetDefaults() {
	if r.CREATION_DATE == 0 {
		r.CREATION_DATE = Date()
	}
	if r.LAST_MODIFICATION_DATE == 0 {
		r.LAST_MODIFICATION_DATE = Date()
	}
}

// Decode implementation for Datasets
func (r *Datasets) Decode(reader io.Reader) error {
	if reader == nil {
		return nil
	}
	// init record with given data record
	data, err := io.ReadAll(reader)
	if err != nil {
		log.Println("fail to read data", err)
		return Error(err, ReaderErrorCode, "", "dbs.datasets.Decode")
	}
	err = json.Unmarshal(data, &r)

	//     decoder := json.NewDecoder(r)
	//     err := decoder.Decode(&rec)
	if err != nil {
		log.Println("fail to decode data", err)
		return Error(err, UnmarshalErrorCode, "", "dbs.datasets.Decode")
	}
	return nil
}
