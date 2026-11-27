package main

import (
	"errors"
	"log"
	"net/http"
	"os"
	"time"

	"my-service/internal/app"
	"my-service/internal/metrics"
	"my-service/internal/storage"
)

const (
	appName = "my_app"
)

func main() {
	port := os.Getenv("PORT")
	podID := os.Getenv("POD_ID")
	datacenter := os.Getenv("DATACENTER")

	if port == "" || podID == "" || datacenter == "" {
		log.Fatalf("Not specified environment variables port: %s, podID: %s, datacenter: %s", port, podID, datacenter)
	}

	application := app.NewApplication(
		app.Config{
			Port:              port,
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       10 * time.Second,
			WriteTimeout:      15 * time.Second,
			IdleTimeout:       60 * time.Second,
		},
		metrics.NewHttpServerMetrics(appName, podID, datacenter),
		storage.NewInMemoryStorage(),
	)

	log.Printf("Starting items service on :%s (pod_id=%s, datacenter=%s)", port, podID, datacenter)
	if err := application.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("HTTP server failed: %v", err)
	}
}
