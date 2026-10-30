package main

import (
	"net/http"
	"sync"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type album struct {
	ID     string  `json:"id"`
	Title  string  `json:"title"`
	Artist string  `json:"artist"`
	Price  float64 `json:"price"`
}

type Store struct {
	mu    sync.RWMutex
	items map[string]album
}

func NewStore() *Store {
	return &Store{
		items: make(map[string]album),
	}
}

func main() {
	store := NewStore()
	metrics := NewMetrics()

	if err := metrics.Register(prometheus.DefaultRegisterer); err != nil {
		panic(err)
	}

	router := gin.Default()
	router.Use(metrics.Middleware())
	router.POST("/items", store.createItem)
	router.GET("/items/:id", store.getItem)
	router.PUT("/items/:id", store.updateItem)
	router.GET("/health", health)
	router.GET("/metrics", gin.WrapH(promhttp.Handler()))
	router.GET("/handlers", getHandlers(router))

	if err := router.Run("0.0.0.0:8080"); err != nil {
		panic(err)
	}
}

func health(c *gin.Context) {
	c.Status(http.StatusOK)
}

func (s *Store) createItem(c *gin.Context) {
	var item album

	if err := c.ShouldBindJSON(&item); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Invalid JSON or missing ID",
		})
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.items[item.ID]; exists {
		c.JSON(http.StatusConflict, gin.H{
			"error": "Item already exists",
		})
		return
	}

	s.items[item.ID] = item

	c.JSON(http.StatusCreated, item)
}

func (s *Store) getItem(c *gin.Context) {
	id := c.Param("id")

	s.mu.RLock()
	item, exists := s.items[id]
	s.mu.RUnlock()

	if !exists {
		c.JSON(http.StatusNotFound, gin.H{
			"error": "Item not found",
		})
		return
	}

	c.JSON(http.StatusOK, item)
}

func (s *Store) updateItem(c *gin.Context) {
	id := c.Param("id")

	var item album
	if err := c.ShouldBindJSON(&item); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Invalid JSON",
		})
		return
	}

	if item.ID != "" && item.ID != id {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "ID in body must match ID in URL",
		})
		return
	}

	item.ID = id

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.items[id]; !exists {
		c.JSON(http.StatusNotFound, gin.H{
			"error": "Item not found",
		})
		return
	}

	s.items[id] = item

	c.JSON(http.StatusOK, item)
}
