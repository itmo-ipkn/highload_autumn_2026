package main

import (
	"fmt"
	"log"
	"my-service/internal/loadtester"
	"os"
	"time"
)

func main() {
	appUrlDc1 := os.Getenv("APP_DC1_URL")
	appUrlDc2 := os.Getenv("APP_DC2_URL")

	if appUrlDc1 == "" || appUrlDc2 == "" {
		log.Fatalf("Not specified environment variables appUrlDc1: %s, appUrlDc2: %s", appUrlDc1, appUrlDc2)
	}

	targetRPS := 40
	rampUpDuration := 120 * time.Second
	startTime := time.Now()

	fmt.Printf("Starting load generation with ramp-up over %v targeting %d RPS...\n", rampUpDuration, targetRPS)

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	sem := make(chan struct{}, 100)
	for range ticker.C {
		elapsed := time.Since(startTime)
		var currentRPS int
		if elapsed < rampUpDuration {
			currentRPS = int(float64(targetRPS) * (float64(elapsed) / float64(rampUpDuration)))
			if currentRPS < 1 {
				currentRPS = 1
			}
		} else {
			currentRPS = targetRPS
		}
		for i := 0; i < currentRPS; i++ {
			sem <- struct{}{}
			go func() {
				defer func() { <-sem }()
				loadtester.SendRequest([]string{appUrlDc1, appUrlDc2})
			}()
		}
	}
}
