package main

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

const topic = "events.dlt"

func (c compose) offsets(position string) (map[int]int64, error) {
	data, err := c.kafka("kafka-get-offsets", "--topic", topic, "--time", position)
	if err != nil {
		return nil, err
	}
	offsets := make(map[int]int64)
	err = lines(data, func(line string) error {
		fields := strings.Split(line, ":")
		if len(fields) != 3 {
			return fmt.Errorf("invalid Kafka offset: %q", line)
		}
		if fields[0] != topic {
			return nil
		}
		partition, pErr := strconv.Atoi(fields[1])
		offset, oErr := strconv.ParseInt(fields[2], 10, 64)
		if pErr != nil || oErr != nil || partition < 0 || offset < 0 {
			return fmt.Errorf("invalid Kafka offset: %q", line)
		}
		offsets[partition] = offset
		return nil
	})
	return offsets, err
}

type dltEvent struct {
	User       int64           `json:"user_id"`
	LegacyUser int64           `json:"userId"`
	Seq        int64           `json:"seq"`
	ID         string          `json:"event_id"`
	LegacyID   string          `json:"eventId"`
	Payload    json.RawMessage `json:"payload"`
}

func (s *snapshot) addDLT(line string) error {
	if strings.HasPrefix(line, "\"") {
		var body string
		if err := json.Unmarshal([]byte(line), &body); err != nil {
			return err
		}
		line = body
	}
	var event dltEvent
	if err := json.Unmarshal([]byte(line), &event); err != nil {
		return err
	}
	if event.User == 0 {
		event.User = event.LegacyUser
	}
	if event.ID == "" {
		event.ID = event.LegacyID
	}
	if event.User < 1 || event.Seq < 1 || event.ID == "" {
		return fmt.Errorf("invalid user_id, seq or event_id in DLT")
	}
	s.add(event.User, event.Seq)
	s.dlt[event.ID] = struct{}{}
	var payload struct {
		Poison bool `json:"poison"`
	}
	if json.Unmarshal(event.Payload, &payload) != nil || !payload.Poison {
		s.badDLT++
	}
	return nil
}

func (s *snapshot) readDLT(c compose) error {
	ends, err := c.offsets("latest")
	if err != nil {
		return err
	}
	starts, err := c.offsets("earliest")
	if err != nil {
		return err
	}
	if len(ends) == 0 || len(ends) != len(starts) {
		return fmt.Errorf("could not read all DLT partition boundaries")
	}
	for partition, end := range ends {
		start, ok := starts[partition]
		if !ok || end < start {
			return fmt.Errorf("invalid DLT boundaries on partition %d", partition)
		}
		expected := end - start
		if expected == 0 {
			continue
		}
		data, err := c.kafka("kafka-console-consumer", "--topic", topic, "--partition", strconv.Itoa(partition),
			"--offset", strconv.FormatInt(start, 10), "--max-messages", strconv.FormatInt(expected, 10),
			"--timeout-ms", "10000", "--consumer-property", "enable.auto.commit=false")
		if err != nil {
			return err
		}
		var count int64
		err = lines(data, func(line string) error {
			count++
			return s.addDLT(line)
		})
		if err != nil {
			return fmt.Errorf("DLT partition %d: %w", partition, err)
		}
		if count != expected {
			return fmt.Errorf("incomplete DLT read on partition %d: expected %d, read %d", partition, expected, count)
		}
	}
	return nil
}

type partitionProgress struct {
	committed, end int64
}

func (c compose) progress() ([3]partitionProgress, error) {
	var partitions [3]partitionProgress
	var seen [3]bool
	data, err := c.kafka("kafka-consumer-groups", "--describe", "--group", "events-processor", "--timeout", "5000")
	if err != nil {
		return partitions, err
	}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 6 || fields[0] != "events-processor" || fields[1] != "events" {
			continue
		}
		partition, pErr := strconv.Atoi(fields[2])
		committed, cErr := strconv.ParseInt(fields[3], 10, 64)
		end, eErr := strconv.ParseInt(fields[4], 10, 64)
		if pErr != nil || cErr != nil || eErr != nil || partition < 0 || partition >= len(partitions) || committed < 0 || end < committed {
			return partitions, fmt.Errorf("unavailable or invalid offsets for events partition %s", fields[2])
		}
		partitions[partition] = partitionProgress{committed: committed, end: end}
		seen[partition] = true
	}
	for partition, found := range seen {
		if !found {
			return partitions, fmt.Errorf("no offsets for events partition %d", partition)
		}
	}
	return partitions, nil
}
