package main

import (
	"github.com/gin-gonic/gin"
)

type DatasetRequest struct {
	Name string `json:"name"`
}

// DatasetHandler provives access to GET /datasets end-point
func DatasetHandler(c *gin.Context) {
	c.JSON(200, gin.H{"status": "ok"})
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
