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

// Sites represents Sites DBS DB table
type Sites struct {
	SITE_ID                int64  `json:"site_id"`
	SITE                   string `json:"site" validate:"required"`
	CREATION_DATE          int64  `json:"creation_date"`
	CREATE_BY              string `json:"create_by"`
	LAST_MODIFICATION_DATE int64  `json:"last_modification_date"`
	LAST_MODIFIED_BY       string `json:"last_modified_by"`
}

// Sites DBS API
//
//gocyclo:ignore
func (a *API) GetSite() error {
	var args []interface{}
	var conds []string
	var err error

	tmpl := make(Record)
	tmpl["Owner"] = DBOWNER
	stm, err := LoadTemplateSQL("select_site", tmpl)
	if err != nil {
		return Error(err, LoadErrorCode, "", "dbs.sites.Sites")
	}

	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	err = executeAll(a.Writer, a.Separator, stm, args...)
	if err != nil {
		return Error(err, QueryErrorCode, "", "dbs.sites.Sites")
	}
	return nil
}

// InsertSite inserts site record into DB
func (a *API) InsertSite() error {
	// the API provides Reader which will be used by Decode function to load the HTTP payload
	// and cast it to Sites data structure
	return insertRecord(&Sites{}, a.Reader)
}

// UpdateSite inserts site record in DB
func (a *API) UpdateSite() error {
	return nil
}

// DeleteSite deletes site record in DB
func (a *API) DeleteSite() error {
	return nil
}

// Insert implementation of Sites
func (r *Sites) Insert(tx *sql.Tx) error {
	var err error
	if r.SITE_ID == 0 {
		siteID, err := getTableId(tx, "SITES", "SITE_ID")
		if err != nil {
			log.Println("unable to get siteID", err)
			return Error(err, ParametersErrorCode, "", "dbs.sites.Insert")
		}
		r.SITE_ID = siteID
	}
	// set defaults and validate the record
	r.SetDefaults()
	err = r.Validate()
	if err != nil {
		log.Println("unable to validate record", err)
		return Error(err, ValidateErrorCode, "", "dbs.sites.Insert")
	}
	// get SQL statement from static area
	stm := getSQL("insert_site")
	if utils.VERBOSE > 0 {
		log.Printf("Insert Sites record %+v", r)
	} else if utils.VERBOSE > 1 {
		log.Printf("Insert Sites\n%s\n%+v", stm, r)
	}
	_, err = tx.Exec(
		stm,
		r.SITE_ID,
		r.SITE,
		r.CREATION_DATE,
		r.CREATE_BY,
		r.LAST_MODIFICATION_DATE,
		r.LAST_MODIFIED_BY)
	if err != nil {
		if utils.VERBOSE > 0 {
			log.Println("unable to insert sites, error", err)
		}
		return Error(err, InsertErrorCode, "", "dbs.sites.Insert")
	}
	return nil
}

// Validate implementation of Sites
func (r *Sites) Validate() error {
	if err := RecordValidator.Struct(*r); err != nil {
		return DecodeValidatorError(r, err)
	}
	if matched := unixTimePattern.MatchString(fmt.Sprintf("%d", r.CREATION_DATE)); !matched {
		msg := "invalid pattern for creation date"
		return Error(InvalidParamErr, PatternErrorCode, msg, "dbs.sites.Validate")
	}
	if matched := unixTimePattern.MatchString(fmt.Sprintf("%d", r.LAST_MODIFICATION_DATE)); !matched {
		msg := "invalid pattern for last modification date"
		return Error(InvalidParamErr, PatternErrorCode, msg, "dbs.sites.Validate")
	}
	return nil
}

// SetDefaults implements set defaults for Sites
func (r *Sites) SetDefaults() {
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

// Decode implementation for Sites
func (r *Sites) Decode(reader io.Reader) error {
	// init record with given data record
	data, err := io.ReadAll(reader)
	if err != nil {
		log.Println("fail to read data", err)
		return Error(err, ReaderErrorCode, "", "dbs.sites.Decode")
	}
	err = json.Unmarshal(data, &r)

	//     decoder := json.NewDecoder(r)
	//     err := decoder.Decode(&rec)
	if err != nil {
		log.Println("fail to decode data", err)
		return Error(err, UnmarshalErrorCode, "", "dbs.sites.Decode")
	}
	return nil
}
