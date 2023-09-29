package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/OreCast/DataBookkeeping/dbs"
	"github.com/OreCast/DataBookkeeping/utils"
)

// helper function to get request URI
func requestURI(r *http.Request) string {
	uri, err := url.QueryUnescape(r.RequestURI)
	if err != nil {
		log.Println("unable to unescape request uri", r.RequestURI, "error", err)
		uri = r.RequestURI
	}
	return uri
}

// HTTPError represents HTTP error structure
type HTTPError struct {
	Method         string `json:"method"`           // HTTP method
	HTTPCode       int    `json:"code"`             // HTTP status code from IANA
	Timestamp      string `json:"timestamp"`        // timestamp of the error
	Path           string `json:"path"`             // URL path
	UserAgent      string `json:"user_agent"`       // http user-agent field
	XForwardedHost string `json:"x_forwarded_host"` // http.Request X-Forwarded-Host
	XForwardedFor  string `json:"x_forwarded_for"`  // http.Request X-Forwarded-For
	RemoteAddr     string `json:"remote_addr"`      // http.Request remote address
}

// ServerError represents HTTP server error structure
type ServerError struct {
	DBSError  error     `json:"error"`     // DBS error
	HTTPError HTTPError `json:"http"`      // HTTP section of the error
	Exception int       `json:"exception"` // for compatibility with Python server
	Type      string    `json:"type"`      // for compatibility with Python server
	Message   string    `json:"message"`   // for compatibility with Python server
}

// responseMsg helper function to provide response to end-user
func responseMsg(w http.ResponseWriter, r *http.Request, err error, code int) int64 {
	path := r.RequestURI
	uri, e := url.QueryUnescape(r.RequestURI)
	if e == nil {
		path = uri
	}
	hrec := HTTPError{
		Method:         r.Method,
		Timestamp:      time.Now().String(),
		HTTPCode:       code,
		Path:           path,
		RemoteAddr:     r.RemoteAddr,
		XForwardedFor:  r.Header.Get("X-Forwarded-For"),
		XForwardedHost: r.Header.Get("X-Forwarded-Host"),
		UserAgent:      r.Header.Get("User-agent"),
	}
	rec := ServerError{
		HTTPError: hrec,
		DBSError:  err,
		Exception: code,        // for compatibility with Python server
		Type:      "HTTPError", // for compatibility with Python server
		Message:   err.Error(), // for compatibility with Python server
	}

	var dbsError *dbs.DBSError
	if errors.As(err, &dbsError) {
		log.Printf(dbsError.ErrorStacktrace())
	} else {
		log.Printf(err.Error())
	}
	// if we want to use JSON record output we'll use
	//     data, _ := json.Marshal(rec)
	// otherwise we'll use list of JSON records
	var out []ServerError
	out = append(out, rec)
	data, _ := json.Marshal(out)
	w.Header().Add("Content-Type", "application/json")
	w.WriteHeader(code)
	w.Write(data)
	return int64(len(data))
}

// helper function to parse POST HTTP request payload
func parseParams(r *http.Request) (dbs.Record, error) {
	params := make(dbs.Record)
	// r.URL.Query() returns map[string][]string
	for k, values := range r.URL.Query() {
		var vals []string
		for _, v := range values {
			if strings.Contains(v, "[") {
				if strings.ToLower(k) == "run_num" {
					params["runList"] = true
				}
				v = v[1 : len(v)-1]
				for _, x := range strings.Split(v, ",") {
					x = strings.Trim(x, " ")
					x = strings.Replace(x, "'", "", -1)
					vals = append(vals, x)
				}
				continue
			}
			v = strings.Replace(v, "'", "", -1)
			vals = append(vals, v)
		}
		params[k] = vals
	}
	return params, nil
}

// helper function to parse POST HTTP request payload
func parsePayload(r *http.Request) (dbs.Record, error) {
	defer r.Body.Close()
	decoder := json.NewDecoder(r.Body)
	params := make(dbs.Record)
	err := decoder.Decode(&params)
	if err != nil {
		return nil, dbs.Error(err, dbs.DecodeErrorCode, "unable to decode HTTP post payload", "web.parsePayload")
	}
	if utils.VERBOSE > 0 {
		log.Println("HTTP POST payload\n", params)
	}
	for k, v := range params {
		s := fmt.Sprintf("%v", v)
		if strings.ToLower(k) == "run_num" && strings.Contains(s, "[") {
			params["runList"] = true
		}
		s = strings.Replace(s, "[", "", -1)
		s = strings.Replace(s, "]", "", -1)
		var out []string
		for _, vv := range strings.Split(s, " ") {
			ss := strings.Trim(vv, " ")
			if ss != "" {
				out = append(out, ss)
			}
		}
		if utils.VERBOSE > 1 {
			log.Printf("payload: key=%s val='%v' out=%v", k, v, out)
		}
		params[k] = out
	}
	return params, nil
}
