// Package kafkalog пишет логи клиента franz-go в slog.
package kafkalog

import (
	"context"
	"errors"
	"log/slog"
	"regexp"
	"strings"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// BatchFailedMsg - сообщение franz-go, когда батч в produce-запросе окончательно не прошёл.
const BatchFailedMsg = "batch in a produce request failed"

// producedMsg - debug-сообщение franz-go с итогом produce-запроса по партициям. Ретраибельные
// ошибки брокера (NOT_ENOUGH_REPLICAS, REQUEST_TIMED_OUT) клиент повторяет сам и пишет
// только сюда: "events[0{retrying@-1,12(NOT_ENOUGH_REPLICAS: ...)}, ...]".
const producedMsg = "produced"

var retryRe = regexp.MustCompile(`(\d+)\{retrying@-?\d+,(\d+)\(([A-Z_]+)`)

// Retry - батч, который брокер отклонил ретраибельной ошибкой.
type Retry struct {
	Topic     string
	Partition string
	Records   string
	Code      string
}

// ParseRetries достаёт ретраи из поля "to" debug-сообщения "produced".
func ParseRetries(update string) []Retry {
	var out []Retry
	topic := ""
	for _, part := range strings.Split(update, "]") {
		part = strings.TrimLeft(part, ", ")
		if i := strings.Index(part, "["); i >= 0 {
			topic = part[:i]
		}
		for _, m := range retryRe.FindAllStringSubmatch(part, -1) {
			out = append(out, Retry{Topic: topic, Partition: m[1], Records: m[2], Code: m[3]})
		}
	}
	return out
}

// Logger - kgo.Logger поверх slog. OnBatchError, если задан, вызывается с кодом ошибки
// брокера на каждый отклонённый produce-батч, и ретраибельный, и окончательный.
type Logger struct {
	Out          *slog.Logger
	MinLevel     kgo.LogLevel
	OnBatchError func(code string)
}

var _ kgo.Logger = (*Logger)(nil)

// Level - уровень, начиная с которого franz-go присылает сообщения. С OnBatchError нужен
// debug: только там видны ретраи; в slog уходит всё не подробнее MinLevel.
func (l *Logger) Level() kgo.LogLevel {
	if l.OnBatchError != nil {
		return kgo.LogLevelDebug
	}
	return l.MinLevel
}

// Log пишет сообщение franz-go.
func (l *Logger) Log(level kgo.LogLevel, msg string, keyvals ...any) {
	if l.OnBatchError != nil {
		switch msg {
		case BatchFailedMsg:
			l.OnBatchError(ErrCode(errFrom(keyvals)))
		case producedMsg:
			to, _ := value(keyvals, "to").(string)
			for _, r := range ParseRetries(to) {
				l.OnBatchError(r.Code)
				l.Out.Warn("produce batch rejected, retrying", "component", "kgo", "broker", value(keyvals, "broker"),
					"topic", r.Topic, "partition", r.Partition, "records", r.Records, "code", r.Code)
			}
		}
	}
	if level > l.MinLevel {
		return
	}
	var lv slog.Level
	switch level {
	case kgo.LogLevelError:
		lv = slog.LevelError
	case kgo.LogLevelWarn:
		lv = slog.LevelWarn
	case kgo.LogLevelInfo:
		lv = slog.LevelInfo
	default:
		lv = slog.LevelDebug
	}
	l.Out.Log(context.Background(), lv, msg, append([]any{"component", "kgo"}, keyvals...)...)
}

func value(keyvals []any, key string) any {
	for i := 0; i+1 < len(keyvals); i += 2 {
		if k, _ := keyvals[i].(string); k == key {
			return keyvals[i+1]
		}
	}
	return nil
}

func errFrom(keyvals []any) error {
	err, _ := value(keyvals, "err").(error)
	return err
}

// ErrCode - код ошибки Kafka (NOT_ENOUGH_REPLICAS и т.п.) или "other".
func ErrCode(err error) string {
	var ke *kerr.Error
	if errors.As(err, &ke) {
		return ke.Message
	}
	if err == nil {
		return "none"
	}
	return "other"
}
