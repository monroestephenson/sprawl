package store

import (
	"sync"
)

type Message struct {
	Topic   string
	Payload []byte
}

type SubscriberFunc func(msg Message)

type Store struct {
	mu          sync.RWMutex
	subscribers map[string][]SubscriberFunc
}

// NewStore creates a new in-memory store
func NewStore() *Store {
	return &Store{
		subscribers: make(map[string][]SubscriberFunc),
	}
}

// Publish stores the message and notifies subscribers
func (s *Store) Publish(topic string, payload []byte) {
	s.mu.RLock()
	subs, ok := s.subscribers[topic]
	s.mu.RUnlock()

	if ok {
		msg := Message{Topic: topic, Payload: payload}
		// Notify all subscribers
		for _, subFn := range subs {
			go subFn(msg)
		}
	}
}

// Subscribe registers a callback function for a topic
func (s *Store) Subscribe(topic string, subFn SubscriberFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.subscribers[topic] = append(s.subscribers[topic], subFn)
}
