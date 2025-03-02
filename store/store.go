package store

import (
	"log"
	"sync"
	"time"
)

type Message struct {
	Topic   string
	Payload []byte
}

type SubscriberFunc func(msg Message)

type Store struct {
	mu          sync.RWMutex
	subscribers map[string][]SubscriberFunc
	metrics     *Metrics
}

// NewStore creates a new in-memory store
func NewStore() *Store {
	return &Store{
		subscribers: make(map[string][]SubscriberFunc),
		metrics:     NewMetrics(),
	}
}

// Publish stores the message and notifies subscribers
func (s *Store) Publish(topic string, payload []byte) {
	s.mu.RLock()
	subs, ok := s.subscribers[topic]
	s.mu.RUnlock()

	log.Printf("[Store] Publishing message to topic %s with %d subscribers", topic, len(subs))

	// Record the received message in metrics
	if s.metrics != nil {
		s.metrics.RecordMessage(false) // false = received
		log.Printf("[Store] Recorded message receipt in metrics for topic %s", topic)
	}

	if ok {
		msg := Message{Topic: topic, Payload: payload}
		// Use WaitGroup to ensure all subscribers are notified
		var wg sync.WaitGroup
		wg.Add(len(subs))

		// Notify all subscribers
		for _, subFn := range subs {
			go func(fn SubscriberFunc) {
				defer wg.Done()
				log.Printf("[Store] Delivering message to subscriber for topic %s", topic)
				fn(msg)
				// Record the sent message in metrics
				if s.metrics != nil {
					s.metrics.RecordMessage(true) // true = sent
					log.Printf("[Store] Recorded message delivery in metrics for topic %s", topic)
				}
			}(subFn)
		}

		// Wait for all subscribers to be notified
		wg.Wait()
		log.Printf("[Store] All subscribers notified for topic %s", topic)
	} else {
		log.Printf("[Store] No subscribers found for topic %s", topic)
	}
}

// Subscribe registers a callback function for a topic
func (s *Store) Subscribe(topic string, subFn SubscriberFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()

	log.Printf("[Store] Adding subscriber for topic %s", topic)

	if s.subscribers[topic] == nil {
		s.subscribers[topic] = make([]SubscriberFunc, 0)
	}
	s.subscribers[topic] = append(s.subscribers[topic], subFn)

	// Record subscription in metrics
	if s.metrics != nil {
		s.metrics.RecordMessage(true) // true = subscription
		log.Printf("[Store] Recorded subscription in metrics for topic %s", topic)
	}

	log.Printf("[Store] Topic %s now has %d subscribers", topic, len(s.subscribers[topic]))
}

// StoreMetrics represents metrics for the store
type StoreMetrics struct {
	MessagesStored int64     `json:"messages_stored"`
	BytesStored    int64     `json:"bytes_stored"`
	Topics         []string  `json:"topics"`
	LastWriteTime  time.Time `json:"last_write_time"`
	StorageType    string    `json:"storage_type"`
}

// GetMetrics returns the current metrics for the store
func (s *Store) GetMetrics() StoreMetrics {
	s.mu.RLock()
	defer s.mu.RUnlock()

	topics := make([]string, 0, len(s.subscribers))
	for topic := range s.subscribers {
		topics = append(topics, topic)
	}

	return StoreMetrics{
		MessagesStored: s.metrics.messagesReceived.Load(),
		BytesStored:    0, // We don't track bytes in memory store
		Topics:         topics,
		LastWriteTime:  time.Now(),
		StorageType:    "memory",
	}
}
