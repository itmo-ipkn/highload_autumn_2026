package main

import (
	"log"
	"math"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	if len(os.Args) == 2 && os.Args[1] == "healthcheck" {
		os.Exit(checkHealth())
	}

	podID := strings.TrimSpace(os.Getenv("POD_ID"))
	datacenter := strings.TrimSpace(os.Getenv("DATACENTER"))
	if podID == "" || datacenter == "" {
		log.Fatal("POD_ID and DATACENTER environment variables are required")
	}
	failureProbability, err := strconv.ParseFloat(env("INJECT_500_PROBABILITY", "0"), 64)
	if err != nil || math.IsNaN(failureProbability) || failureProbability < 0 || failureProbability > 1 {
		log.Fatal("INJECT_500_PROBABILITY must be a number between 0 and 1")
	}

	routes := itemRoutes(NewStore(), failureProbability)
	metrics := NewMetrics(podID, datacenter, routes)
	server := newRouter(routes, metrics)

	httpServer := &http.Server{
		Addr:              ":8080",
		Handler:           server,
		ReadTimeout:       10 * time.Second,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	log.Printf("listening on %s", httpServer.Addr)
	log.Fatal(httpServer.ListenAndServe())
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func checkHealth() int {
	client := http.Client{Timeout: 2 * time.Second}
	response, err := client.Get("http://127.0.0.1:8080/health")
	if err != nil {
		return 1
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return 1
	}
	return 0
}
