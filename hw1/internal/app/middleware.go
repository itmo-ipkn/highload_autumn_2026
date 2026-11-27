package app

import (
	"log"
	"net/http"
	"strconv"
	"time"
)

type middlewares struct {
	metrics httpServerMetrics
}

func NewMiddlewares(metrics httpServerMetrics) *middlewares {
	return &middlewares{
		metrics: metrics,
	}
}

type responseWriterWrapper struct {
	http.ResponseWriter
	statusCode      int
	isHeaderWritten bool
}

func (w *responseWriterWrapper) WriteHeader(code int) {
	if w.isHeaderWritten {
		return
	}

	w.statusCode = code
	w.isHeaderWritten = true
	w.ResponseWriter.WriteHeader(code)
}

func (w *responseWriterWrapper) Write(data []byte) (int, error) {
	if !w.isHeaderWritten {
		w.WriteHeader(http.StatusOK)
	}

	return w.ResponseWriter.Write(data)
}

func (m *middlewares) Middleware(method, path string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		startedAt := time.Now()
		rec := &responseWriterWrapper{ResponseWriter: w, statusCode: http.StatusOK}

		defer func() {
			if recovered := recover(); recovered != nil {
				log.Printf("panic while handling %s %s: %v\n", method, path, recovered)

				if !rec.isHeaderWritten {
					writeResponse(rec, http.StatusInternalServerError, errorResponse{Error: "internal server error"})
				}
			}

			if !rec.isHeaderWritten {
				rec.WriteHeader(http.StatusOK)
			}

			statusCode := strconv.Itoa(rec.statusCode)
			m.metrics.Inc(method, path, statusCode)
			m.metrics.WriteTiming(startedAt, method, path, statusCode)
		}()

		next.ServeHTTP(rec, r)
	})
}
