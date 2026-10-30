package main

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

type HandlerInfo struct {
	Method string `json:"method"`
	Path   string `json:"path"`
}

func getHandlers(router *gin.Engine) gin.HandlerFunc {
	return func(c *gin.Context) {
		routes := router.Routes()
		handlers := make([]HandlerInfo, 0, len(routes))

		for _, route := range routes {
			if route.Path == "/health" ||
				route.Path == "/metrics" ||
				route.Path == "/handlers" {
				continue
			}

			handlers = append(handlers, HandlerInfo{
				Method: route.Method,
				Path:   route.Path,
			})
		}

		c.IndentedJSON(http.StatusOK, gin.H{
			"handlers": handlers,
		})
	}
}
