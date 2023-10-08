package dbs

// nolint: gocyclo

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"

	"github.com/OreCast/DataBookkeeping/utils"
)

// Processing represents Processing DBS DB table
type Processing struct {
	PROCESSING_ID          int64  `json:"processing_id"`
	PROCESSING             string `json:"processing" validate:"required"`
	CREATION_DATE          int64  `json:"creation_date"`
	CREATE_BY              string `json:"create_by"`
	LAST_MODIFICATION_DATE int64  `json:"last_modification_date"`
	LAST_MODIFIED_BY       string `json:"last_modified_by"`
}

// Processing DBS API
//
//gocyclo:ignore
func (a *API) GetProcessing() error {
	var args []interface{}
	var conds []string
	var err error

	tmpl := make(Record)
	tmpl["Owner"] = DBOWNER
	stm, err := LoadTemplateSQL("select_processing", tmpl)
	if err != nil {
		return Error(err, LoadErrorCode, "", "dbs.processing.Processing")
	}

	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	err = executeAll(a.Writer, a.Separator, stm, args...)
	if err != nil {
		return Error(err, QueryErrorCode, "", "dbs.processing.Processing")
	}
	return nil
}

// InsertProcessing inserts processing record into DB
func (a *API) InsertProcessing() error {
	// the API provides Reader which will be used by Decode function to load the HTTP payload
	// and cast it to Processing data structure
	return insertRecord(&Processing{}, a.Reader)
}

// UpdateProcessing inserts processing record in DB
func (a *API) UpdateProcessing() error {
	return nil
}

// DeleteProcessing deletes processing record in DB
func (a *API) DeleteProcessing() error {
	return nil
}

// helper function to get next available ProcessingID
func getProcessingID(tx *sql.Tx) (int64, error) {
	var err error
	var tid int64
	if DBOWNER == "sqlite" {
		tid, err = LastInsertID(tx, "PROCESSING", "PROCESSING_id")
		tid += 1
	} else {
		tid, err = IncrementSequence(tx, "SEQ_FL")
	}
	if err != nil {
		return tid, Error(err, LastInsertErrorCode, "", "dbs.processing.getProcessingID")
	}
	return tid, nil
}

// Insert implementation of Processing
func (r *Processing) Insert(tx *sql.Tx) error {
	var err error
	if r.PROCESSING_ID == 0 {
		processingID, err := getProcessingID(tx)
		if err != nil {
			log.Println("unable to get processingID", err)
			return Error(err, ParametersErrorCode, "", "dbs.processing.Insert")
		}
		r.PROCESSING_ID = processingID
	}
	// set defaults and validate the record
	r.SetDefaults()
	err = r.Validate()
	if err != nil {
		log.Println("unable to validate record", err)
		return Error(err, ValidateErrorCode, "", "dbs.processing.Insert")
	}
	// get SQL statement from static area
	stm := getSQL("insert_processing")
	if utils.VERBOSE > 0 {
		log.Printf("Insert Processing record %+v", r)
	} else if utils.VERBOSE > 1 {
		log.Printf("Insert Processing\n%s\n%+v", stm, r)
	}
	_, err = tx.Exec(
		stm,
		r.PROCESSING_ID,
		r.PROCESSING,
		r.CREATION_DATE,
		r.CREATE_BY,
		r.LAST_MODIFICATION_DATE,
		r.LAST_MODIFIED_BY)
	if err != nil {
		if utils.VERBOSE > 0 {
			log.Println("unable to insert processing, error", err)
		}
		return Error(err, InsertErrorCode, "", "dbs.processing.Insert")
	}
	return nil
}

// Validate implementation of Processing
func (r *Processing) Validate() error {
	if err := RecordValidator.Struct(*r); err != nil {
		return DecodeValidatorError(r, err)
	}
	if matched := unixTimePattern.MatchString(fmt.Sprintf("%d", r.CREATION_DATE)); !matched {
		msg := "invalid pattern for creation date"
		return Error(InvalidParamErr, PatternErrorCode, msg, "dbs.processing.Validate")
	}
	if matched := unixTimePattern.MatchString(fmt.Sprintf("%d", r.LAST_MODIFICATION_DATE)); !matched {
		msg := "invalid pattern for last modification date"
		return Error(InvalidParamErr, PatternErrorCode, msg, "dbs.processing.Validate")
	}
	return nil
}

// SetDefaults implements set defaults for Processing
func (r *Processing) SetDefaults() {
	if r.CREATE_BY == "" {
		r.CREATE_BY = "Server"
	}
	if r.CREATION_DATE == 0 {
		r.CREATION_DATE = Date()
	}
	if r.LAST_MODIFIED_BY == "" {
		r.LAST_MODIFIED_BY = "Server"
	}
	if r.LAST_MODIFICATION_DATE == 0 {
		r.LAST_MODIFICATION_DATE = Date()
	}
}

// Decode implementation for Processing
func (r *Processing) Decode(reader io.Reader) error {
	// init record with given data record
	data, err := io.ReadAll(reader)
	if err != nil {
		log.Println("fail to read data", err)
		return Error(err, ReaderErrorCode, "", "dbs.processing.Decode")
	}
	err = json.Unmarshal(data, &r)

	//     decoder := json.NewDecoder(r)
	//     err := decoder.Decode(&rec)
	if err != nil {
		log.Println("fail to decode data", err)
		return Error(err, UnmarshalErrorCode, "", "dbs.processing.Decode")
	}
	return nil
}
