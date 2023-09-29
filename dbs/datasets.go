package dbs

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"

	"github.com/OreCast/DataBookkeeping/utils"
)

// Datasets API
//
//gocyclo:ignore
func (a *API) Datasets() error {
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
		"creation_date",
		"create_by",
		"last_modification_date",
		"last_modified_by"}
	vals := []interface{}{
		new(sql.NullInt64),
		new(sql.NullString),
		new(sql.NullFloat64),
		new(sql.NullString),
		new(sql.NullFloat64),
		new(sql.NullString)}
	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	err = execute(a.Writer, a.Separator, stm, cols, vals, args...)
	if err != nil {
		return Error(err, QueryErrorCode, "", "dbs.datasets.Datasets")
	}
	return nil
}

// Datasets represents Datasets DBS DB table
type Datasets struct {
	DATASET_ID             int64  `json:"dataset_id"`
	DATASET                string `json:"dataset" validate:"required"`
	CREATION_DATE          int64  `json:"creation_date" validate:"required,number"`
	CREATE_BY              string `json:"create_by" validate:"required"`
	LAST_MODIFICATION_DATE int64  `json:"last_modification_date" validate:"required,number"`
	LAST_MODIFIED_BY       string `json:"last_modified_by" validate:"required"`
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
		if err != nil {
			return Error(err, LastInsertErrorCode, "", "dbs.datasets.Insert")
		}
	}
	// set defaults and validate the record
	r.SetDefaults()
	err = r.Validate()
	if err != nil {
		log.Println("unable to validate record", err)
		return Error(err, ValidateErrorCode, "", "dbs.datasets.Insert")
	}
	// get SQL statement from static area
	stm := getSQL("insert_datasets")
	if utils.VERBOSE > 0 {
		log.Printf("Insert Datasets\n%s\n%+v", stm, r)
	}
	_, err = tx.Exec(
		stm,
		r.DATASET_ID,
		r.DATASET,
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

// DatasetRecord we receive for InsertDatasets API
type DatasetRecord struct {
	DATASET                string `json:"dataset" validate:"required"`
	CREATION_DATE          int64  `json:"creation_date" validate:"required,number,gt=0"`
	CREATE_BY              string `json:"create_by" validate:"required"`
	LAST_MODIFICATION_DATE int64  `json:"last_modification_date" validate:"required,number,gt=0"`
	LAST_MODIFIED_BY       string `json:"last_modified_by" validate:"required"`
}
