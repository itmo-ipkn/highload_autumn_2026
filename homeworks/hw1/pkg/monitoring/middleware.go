package monitoring

import (
	"net/http"
	"time"
)

// PanicHandler writes the response after a panic. wroteHeader reports whether a
// final status was already committed; the handler must leave that status unchanged.
type PanicHandler func(w http.ResponseWriter, r *http.Request, recovered any, wroteHeader bool)

// StatusRecorder remembers the first final HTTP status. Informational 1xx
// statuses are forwarded and do not become the recorded code. Later WriteHeader
// calls do not replace it.
type StatusRecorder struct {
	http.ResponseWriter
	status    int
	committed bool
}

// NewStatusRecorder wraps w. Metrics and the outer panic recovery share this type.
func NewStatusRecorder(w http.ResponseWriter) *StatusRecorder {
	return &StatusRecorder{ResponseWriter: w}
}

// WroteHeader reports whether a final status was already committed.
func (s *StatusRecorder) WroteHeader() bool {
	return s.committed
}

// StatusCode is the committed final status. A handler that wrote nothing is 200,
// which is the status net/http sends in that case.
func (s *StatusRecorder) StatusCode() int {
	if !s.committed {
		return http.StatusOK
	}
	return s.status
}

func (s *StatusRecorder) WriteHeader(code int) {
	if code >= 100 && code < 200 {
		s.ResponseWriter.WriteHeader(code)
		return
	}
	if s.committed {
		return
	}
	s.committed = true
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

func (s *StatusRecorder) Write(p []byte) (int, error) {
	if !s.committed {
		s.committed = true
		s.status = http.StatusOK
	}
	return s.ResponseWriter.Write(p)
}

func (s *StatusRecorder) Unwrap() http.ResponseWriter {
	return s.ResponseWriter
}

func (m *Metrics) wrap(method, route string, next http.Handler, onPanic PanicHandler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		recorder := NewStatusRecorder(w)
		// observe is registered first so it runs after panic recovery and sees the final status.
		defer func() {
			m.observe(method, route, recorder.StatusCode(), time.Since(start))
		}()
		defer func() {
			recovered := recover()
			if recovered == nil {
				return
			}
			if onPanic == nil {
				panic(recovered)
			}
			onPanic(recorder, r, recovered, recorder.WroteHeader())
		}()
		next.ServeHTTP(recorder, r)
	})
}
