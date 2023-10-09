package dbs

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"

	"github.com/OreCast/DataBookkeeping/utils"
)

// Datasets represents Datasets DBS DB table
type Datasets struct {
	DATASET_ID             int64  `json:"dataset_id"`
	DATASET                string `json:"dataset" validate:"required"`
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
		"dataset",
		"meta_id",
		"site",
		"processing",
		"parent",
		"creation_date",
		"create_by",
		"last_modification_date",
		"last_modified_by"}
	vals := []interface{}{
		new(sql.NullString),  // dataset
		new(sql.NullFloat64), // meta_id
		new(sql.NullString),  // site
		new(sql.NullString),  // processing
		new(sql.NullString),  // parent
		new(sql.NullFloat64), // creation_date
		new(sql.NullString),  // create_by
		new(sql.NullFloat64), // last_modification_date
		new(sql.NullString)}  // last_modified_by
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
		return Error(err, ReaderErrorCode, "", "dbs.datasets.InsertDataset")
	}
	rec := DatasetRecord{}
	if a.ContentType == "application/json" {
		err = json.Unmarshal(data, &rec)
	} else {
		log.Println("Parser dataset record using default application/json mtime")
		err = json.Unmarshal(data, &rec)
	}
	if err != nil {
		log.Println("reading", a.ContentType)
		log.Println("reading data", string(data))
		log.Println("fail to decode data", err)
		return Error(err, UnmarshalErrorCode, "", "dbs.datasets.InsertDataset")
	}
	log.Printf("### input DatasetRecord %+v", rec)

	// parse incoming DatasetRequest and insert relationships, e.g.
	// site, bucket, parent, processing, files
	record := Datasets{
		DATASET:          rec.Dataset,
		META_ID:          rec.MetaId,
		CREATE_BY:        a.CreateBy,
		LAST_MODIFIED_BY: a.CreateBy,
	}
	err = insertParts(&rec, &record)
	if err != nil {
		return Error(err, CommitErrorCode, "", "dbs.insertRecord")
	}
	return nil
}

// helper function to insert parts of the dataset relationships
func insertParts(rec *DatasetRecord, record *Datasets) error {
	// start transaction
	tx, err := DB.Begin()
	if err != nil {
		return Error(err, TransactionErrorCode, "", "dbs.insertRecord")
	}
	defer tx.Rollback()
	var siteId, processingId, parentId, datasetId int64

	// insert site info
	siteId, err = GetID(tx, "SITES", "SITE_ID", "site", rec.Site)
	if err != nil {
		site := Sites{SITE: rec.Site}
		if err = site.Insert(tx); err != nil {
			return err
		}
		siteId, err = GetID(tx, "SITES", "SITE_ID", "site", rec.Site)
		if err != nil {
			return err
		}
	}
	record.SITE_ID = siteId

	// insert processing info
	processingId, err = GetID(tx, "PROCESSING", "PROCESSING_ID", "processing", rec.Processing)
	if err != nil {
		processing := Processing{PROCESSING: rec.Processing}
		if err = processing.Insert(tx); err != nil {
			return err
		}
		processingId, err = GetID(tx, "PROCESSING", "PROCESSING_ID", "processing", rec.Processing)
		if err != nil {
			return err
		}
	}
	record.PROCESSING_ID = processingId

	// insert parent info
	parentId, err = GetID(tx, "PARENTS", "PARENT_ID", "parent", rec.Parent)
	if err != nil {
		if rec.Parent != "" {
			parent := Parents{PARENT: rec.Parent}
			if err = parent.Insert(tx); err != nil {
				return err
			}
			parentId, err = GetID(tx, "PARENTS", "PARENT_ID", "parent", rec.Parent)
			if err != nil {
				return err
			}
		}
	}
	record.PARENT_ID = parentId

	// insert dataset info
	datasetId, err = GetID(tx, "DATASETS", "DATASET_ID", "dataset", rec.Dataset)
	if err != nil {
		record.SITE_ID = siteId
		record.PARENT_ID = parentId
		record.PROCESSING_ID = processingId
		if err = record.Insert(tx); err != nil {
			return err
		}
		datasetId, err = GetID(tx, "DATASETS", "DATASET_ID", "dataset", rec.Dataset)
		if err != nil {
			return err
		}
	}

	// insert all buckets
	for _, b := range rec.Buckets {
		bucket := Buckets{
			BUCKET:     b,
			DATASET_ID: datasetId,
			META_ID:    rec.MetaId,
		}
		if err = bucket.Insert(tx); err != nil {
			log.Printf("Bucket %+v already exist", bucket)
		}
	}

	// insert all files
	for _, f := range rec.Files {
		file := Files{
			LOGICAL_FILE_NAME: f,
			DATASET_ID:        datasetId,
			META_ID:           rec.MetaId,
			CREATE_BY:         record.CREATE_BY,
			LAST_MODIFIED_BY:  record.CREATE_BY,
		}
		if err = file.Insert(tx); err != nil {
			log.Printf("File %+v already exist", file)
		}
	}

	// commit all transactions
	err = tx.Commit()
	return err
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
