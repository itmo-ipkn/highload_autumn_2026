package main

import (
	"log"
	"os"

	dashboard "my-service/internal/dashboard"
)

func main() {
	appURL := os.Getenv("APP_URL")
	grafanaURL := os.Getenv("GRAFANA_URL")
	grafanaUser := os.Getenv("GF_SECURITY_ADMIN_USER")
	grafanaPassword := os.Getenv("GF_SECURITY_ADMIN_PASSWORD")

	if appURL == "" || grafanaURL == "" || grafanaUser == "" || grafanaPassword == "" {
		log.Fatalf("Not specified environment variables appURL: %s, grafanaURL: %s, grafanaUser: %s, grafanaPassword: %s",
			appURL, grafanaURL, grafanaUser, grafanaPassword)
	}

	log.Println("Fetching handlers from application...")
	handlers := dashboard.FetchHandlers(appURL)

	log.Printf("Generating dashboard JSON...")
	dashboardJSON := dashboard.Build(handlers)

	log.Println("Uploading dashboard to Grafana...")
	dashboard.Upload(grafanaURL, grafanaUser, grafanaPassword, dashboardJSON)

	log.Println("Dashboard successfully generated and provisioned!")
}
