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

// Users represents Users DBS DB table
type Users struct {
	USER_ID                int64  `json:"user_id"`
	LOGIN                  string `json:"login" validate:"required"`
	PASSWORD               string `json:"password" validate:"required"`
	FIRST_NAME             string `json:"first_name"`
	LAST_NAME              string `json:"last_name"`
	CREATION_DATE          int64  `json:"creation_date"`
	CREATE_BY              string `json:"create_by"`
	LAST_MODIFICATION_DATE int64  `json:"last_modification_date"`
	LAST_MODIFIED_BY       string `json:"last_modified_by"`
}

// Users DBS API
//gocyclo:ignore
func (a *API) GetUser() error {
	var args []interface{}
	var conds []string
	var err error

	tmpl := make(Record)
	tmpl["Owner"] = DBOWNER
	stm, err := LoadTemplateSQL("select_user", tmpl)
	if err != nil {
		return Error(err, LoadErrorCode, "", "dbs.users.Users")
	}

	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	err = executeAll(a.Writer, a.Separator, stm, args...)
	if err != nil {
		return Error(err, QueryErrorCode, "", "dbs.users.Users")
	}
	return nil
}

// InsertUser inserts user record into DB
func (a *API) InsertUser() error {
	/*
		userRecord := Users{
			LOGIN:                  getString(a.Params, "login"),
			FIRST_NAME:             getString(a.Params, "first_name"),
			LAST_NAME:              getString(a.Params, "last_name"),
			CREATION_DATE:          getInt64(a.Params, "creation_date"),
			CREATE_BY:              getString(a.Params, "create_by"),
			LAST_MODIFICATION_DATE: getInt64(a.Params, "last_modification_date"),
			LAST_MODIFIED_BY:       getString(a.Params, "last_modified_by"),
		}
		return insertRecord(&userRecord, a.Reader)
	*/
	return insertRecord(&Users{}, a.Reader)
}

// UpdateUser inserts user record in DB
func (a *API) UpdateUser() error {
	return nil
}

// DeleteUser deletes user record in DB
func (a *API) DeleteUser() error {
	return nil
}

// helper function to get next available UserID
func getUserID(tx *sql.Tx) (int64, error) {
	var err error
	var tid int64
	if DBOWNER == "sqlite" {
		tid, err = LastInsertID(tx, "USERS", "user_id")
		tid += 1
	} else {
		tid, err = IncrementSequence(tx, "SEQ_FL")
	}
	if err != nil {
		return tid, Error(err, LastInsertErrorCode, "", "dbs.users.getUserID")
	}
	return tid, nil
}

// Insert implementation of Users
func (r *Users) Insert(tx *sql.Tx) error {
	var err error
	if r.USER_ID == 0 {
		userID, err := getUserID(tx)
		if err != nil {
			log.Println("unable to get userID", err)
			return Error(err, ParametersErrorCode, "", "dbs.users.Insert")
		}
		r.USER_ID = userID
	}
	// set defaults and validate the record
	r.SetDefaults()
	err = r.Validate()
	if err != nil {
		log.Println("unable to validate record", err)
		return Error(err, ValidateErrorCode, "", "dbs.users.Insert")
	}
	// get SQL statement from static area
	stm := getSQL("insert_user")
	if utils.VERBOSE > 0 {
		log.Printf("Insert Users record %+v", r)
	} else if utils.VERBOSE > 1 {
		log.Printf("Insert Users\n%s\n%+v", stm, r)
	}
	_, err = tx.Exec(
		stm,
		r.USER_ID,
		r.LOGIN,
		r.FIRST_NAME,
		r.LAST_NAME,
		r.PASSWORD,
		r.CREATION_DATE,
		r.CREATE_BY,
		r.LAST_MODIFICATION_DATE,
		r.LAST_MODIFIED_BY)
	if err != nil {
		if utils.VERBOSE > 0 {
			log.Println("unable to insert users, error", err)
		}
		return Error(err, InsertErrorCode, "", "dbs.users.Insert")
	}
	return nil
}

// Validate implementation of Users
func (r *Users) Validate() error {
	if err := RecordValidator.Struct(*r); err != nil {
		return DecodeValidatorError(r, err)
	}
	if matched := unixTimePattern.MatchString(fmt.Sprintf("%d", r.CREATION_DATE)); !matched {
		msg := "invalid pattern for creation date"
		return Error(InvalidParamErr, PatternErrorCode, msg, "dbs.users.Validate")
	}
	if matched := unixTimePattern.MatchString(fmt.Sprintf("%d", r.LAST_MODIFICATION_DATE)); !matched {
		msg := "invalid pattern for last modification date"
		return Error(InvalidParamErr, PatternErrorCode, msg, "dbs.users.Validate")
	}
	return nil
}

// SetDefaults implements set defaults for Users
func (r *Users) SetDefaults() {
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

// Decode implementation for Users
func (r *Users) Decode(reader io.Reader) error {
	// init record with given data record
	data, err := io.ReadAll(reader)
	if err != nil {
		log.Println("fail to read data", err)
		return Error(err, ReaderErrorCode, "", "dbs.users.Decode")
	}
	err = json.Unmarshal(data, &r)

	//     decoder := json.NewDecoder(r)
	//     err := decoder.Decode(&rec)
	if err != nil {
		log.Println("fail to decode data", err)
		return Error(err, UnmarshalErrorCode, "", "dbs.users.Decode")
	}
	return nil
}
