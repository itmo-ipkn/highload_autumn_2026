package model

import "time"

type Item struct {
	ID        int       `json:"id"`
	Data      any       `json:"data"`
	UpdatedAt time.Time `json:"updated_at"`
}
