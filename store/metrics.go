package store

import (
	"sync"
	"sync/atomic"
)

// Metrics provides a way to track store metrics
type Metrics interface {
	RecordMessage(sent bool)
	RecordMessageReceived(topic string)
	RecordMessageDelivered(topic string)
	RecordSubscriptionAdded(topic string)
	RecordSubscriptionRemoved(topic string)

	// New methods to support the API
	GetSnapshot() map[string]interface{}
	GetMessageCount() int64
	GetTopics() []string
	GetMessageCountForTopic(topic string) int64
}

type MetricsImpl struct {
	messagesSent     atomic.Int64
	messagesReceived atomic.Int64

	// Add topic-specific metrics
	mu                sync.RWMutex
	topics            []string
	topicMessageCount map[string]int64
}

func NewMetrics() *MetricsImpl {
	return &MetricsImpl{}
}

func (m *MetricsImpl) RecordMessage(sent bool) {
	if sent {
		m.messagesSent.Add(1)
	} else {
		m.messagesReceived.Add(1)
	}
}

func (m *MetricsImpl) RecordMessageReceived(topic string) {
	m.messagesReceived.Add(1)

	m.mu.Lock()
	defer m.mu.Unlock()

	// Initialize map if needed
	if m.topicMessageCount == nil {
		m.topicMessageCount = make(map[string]int64)
	}

	// Update topic count
	m.topicMessageCount[topic]++

	// Add to topics list if not already present
	found := false
	for _, t := range m.topics {
		if t == topic {
			found = true
			break
		}
	}

	if !found {
		m.topics = append(m.topics, topic)
	}
}

func (m *MetricsImpl) RecordMessageDelivered(topic string)    {}
func (m *MetricsImpl) RecordSubscriptionAdded(topic string)   {}
func (m *MetricsImpl) RecordSubscriptionRemoved(topic string) {}

func (m *MetricsImpl) GetSnapshot() map[string]interface{} {
	return map[string]interface{}{
		"messages_sent":     m.messagesSent.Load(),
		"messages_received": m.messagesReceived.Load(),
	}
}

// New methods implementations
func (m *MetricsImpl) GetMessageCount() int64 { return m.messagesSent.Load() }

func (m *MetricsImpl) GetTopics() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Return a copy to avoid concurrent modification
	result := make([]string, len(m.topics))
	copy(result, m.topics)
	return result
}

func (m *MetricsImpl) GetMessageCountForTopic(topic string) int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	count, exists := m.topicMessageCount[topic]
	if !exists {
		return 0
	}
	return count
}

// UpdateTopics updates the list of known topics
func (m *MetricsImpl) UpdateTopics(topics []string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.topics = make([]string, len(topics))
	copy(m.topics, topics)
}

// UpdateTopicMessageCount updates the message count for a specific topic
func (m *MetricsImpl) UpdateTopicMessageCount(topic string, count int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.topicMessageCount[topic] = count

	// Ensure the topic is in our topics list
	found := false
	for _, t := range m.topics {
		if t == topic {
			found = true
			break
		}
	}

	if !found {
		m.topics = append(m.topics, topic)
	}
}

// NoOpMetrics is a no-op implementation of Metrics
type NoOpMetrics struct{}

func (m *NoOpMetrics) RecordMessageReceived(topic string)     {}
func (m *NoOpMetrics) RecordMessageDelivered(topic string)    {}
func (m *NoOpMetrics) RecordSubscriptionAdded(topic string)   {}
func (m *NoOpMetrics) RecordSubscriptionRemoved(topic string) {}

// New methods implementations
func (m *NoOpMetrics) GetMessageCount() int64                     { return 0 }
func (m *NoOpMetrics) GetTopics() []string                        { return []string{} }
func (m *NoOpMetrics) GetMessageCountForTopic(topic string) int64 { return 0 }
