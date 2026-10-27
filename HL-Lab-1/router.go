package main

import (
	"net/http"
	"sort"
)

type handlerInfo struct {
	Method string `json:"method"`
	Path   string `json:"path"`
}

type route struct {
	info    handlerInfo
	handler http.Handler
}

func newRouter(routes []route, metrics *Metrics) http.Handler {
	mux := http.NewServeMux()
	for _, item := range routes {
		item := item
		mux.Handle(item.info.Method+" "+item.info.Path, metrics.instrument(item, item.handler))
	}
	mux.HandleFunc("GET /handlers", func(w http.ResponseWriter, _ *http.Request) {
		handlers := make([]handlerInfo, 0, len(routes))
		for _, item := range routes {
			handlers = append(handlers, item.info)
		}
		sort.Slice(handlers, func(i, j int) bool {
			if handlers[i].Path == handlers[j].Path {
				return handlers[i].Method < handlers[j].Method
			}
			return handlers[i].Path < handlers[j].Path
		})
		writeJSON(w, http.StatusOK, handlers)
	})
	mux.Handle("GET /metrics", metrics.handler())
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	return mux
}
