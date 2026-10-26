// Package chaos - искусственные задержки и сбои для демонстрации дашборда.
package chaos

import (
	"math/rand/v2"
	"net/http"
	"time"
)

// Middleware отвечает 500 с вероятностью failRate и перед обработкой
// ждёт случайное время от 0 до maxDelay.
func Middleware(failRate float64, maxDelay time.Duration) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if maxDelay > 0 {
				time.Sleep(rand.N(maxDelay)) //nolint:gosec // не криптография
			}
			if failRate > 0 && rand.Float64() < failRate { //nolint:gosec // не криптография
				http.Error(w, `{"error":"injected failure"}`, http.StatusInternalServerError)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
