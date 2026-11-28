package consumer

import (
	"fmt"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
)

type partitionState struct {
	lastCommitted *kgo.Record
	resumeLogged  bool
}

type consumerState struct {
	mu         sync.Mutex
	partitions map[string]*partitionState
}

func PartitionKey(topic string, partition int32) string {
	return fmt.Sprintf("%s/%d", topic, partition)
}

func NewConsumerState() *consumerState {
	return &consumerState{
		partitions: make(map[string]*partitionState),
	}
}

func (s *consumerState) MarkAssigned(partitionsByTopic map[string][]int32) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for topic, topicPartitions := range partitionsByTopic {
		for _, partition := range topicPartitions {
			key := PartitionKey(topic, partition)
			s.partitions[key] = &partitionState{}
		}
	}
}

func (s *consumerState) RecordsForCommit(partitionsByTopic map[string][]int32) []*kgo.Record {
	s.mu.Lock()
	defer s.mu.Unlock()

	var records []*kgo.Record

	for topic, topicPartitions := range partitionsByTopic {
		for _, partition := range topicPartitions {
			key := PartitionKey(topic, partition)
			if p, ok := s.partitions[key]; ok && p.lastCommitted != nil {
				records = append(records, p.lastCommitted)
			}
		}
	}

	return records
}

func (s *consumerState) DeletePartitions(partitionsByTopic map[string][]int32) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for topic, topicPartitions := range partitionsByTopic {
		for _, partition := range topicPartitions {
			delete(s.partitions, PartitionKey(topic, partition))
		}
	}
}

func (s *consumerState) ShouldLogResume(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	p, ok := s.partitions[key]
	if !ok || p.resumeLogged {
		return false
	}

	p.resumeLogged = true
	return true
}

func (s *consumerState) SetLastCommitted(key string, record *kgo.Record) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	p, ok := s.partitions[key]
	if !ok {
		return false
	}

	p.lastCommitted = record
	return true
}
