package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Steplet/HL-lab-2"
	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
)

func makeEvent(userID uuid.UUID, seq uint64) lab2.Event {
	types := [...]lab2.EventType{lab2.PageView, lab2.AddToCart, lab2.Purchase}
	return lab2.Event{
		EventID:    uuid.New(),
		UserID:     userID,
		Type:       types[(seq-1)%uint64(len(types))],
		Seq:        seq,
		ProducedAt: time.Now().UTC(),
		Payload:    json.RawMessage(`{"sample":true}`),
		Poison:     seq%50 == 0,
	}
}

func run() error {
	podID := os.Getenv("POD_ID")
	if podID == "" {
		return fmt.Errorf("POD_ID is required")
	}
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		return fmt.Errorf("KAFKA_BROKERS is required")
	}
	rate, err := strconv.Atoi(os.Getenv("EVENTS_PER_SECOND"))
	if err != nil || rate < 1 || time.Second/time.Duration(rate) < 1 {
		return fmt.Errorf("EVENTS_PER_SECOND must be a positive integer")
	}
	keyMode := os.Getenv("KEY_MODE")
	if keyMode != "user_id" && keyMode != "none" {
		return fmt.Errorf("KEY_MODE must be user_id or none")
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(brokers, ",")...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),
		kgo.RecordDeliveryTimeout(30*time.Second),
	)
	if err != nil {
		return err
	}
	defer client.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	ticker := time.NewTicker(time.Second / time.Duration(rate))
	defer ticker.Stop()

	userID := uuid.New()
	log.Printf("producer pod=%s user_id=%s rate=%d/s key_mode=%s", podID, userID, rate, keyMode)
	var seq uint64
	for {
		select {
		case <-ctx.Done():
			log.Printf("producer pod=%s flushing buffered events", podID)
			flushCtx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
			err := client.Flush(flushCtx)
			cancel()
			return err
		case <-ticker.C:
			event := makeEvent(userID, seq+1)
			value, err := json.Marshal(event)
			if err != nil {
				return err
			}
			var key []byte
			if keyMode == "user_id" {
				key = []byte(userID.String())
			}
			for {
				err := client.ProduceSync(ctx, &kgo.Record{Topic: "events", Key: key, Value: value}).FirstErr()
				if err == nil {
					seq = event.Seq
					break
				}
				log.Printf("delivery failed pod=%s user_id=%s seq=%d: %v", podID, userID, event.Seq, err)
				select {
				case <-ctx.Done():
					log.Printf("producer pod=%s flushing buffered events", podID)
					flushCtx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
					err := client.Flush(flushCtx)
					cancel()
					return err
				case <-time.After(time.Second):
				}
			}
		}
	}
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
