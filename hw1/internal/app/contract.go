package app

import (
	"my-service/internal/model"
	"net/http"
	"time"
)

type httpServerMetrics interface {
	Inc(method, path, status_code string)
	WriteTiming(start time.Time, method, path, status_code string)
	HandleRegistry() http.Handler
}

type serverStorage interface {
	Create(data any) model.Item
	Get(id int) (model.Item, bool)
	Update(id int, data interface{}) (model.Item, bool)
}
