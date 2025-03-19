package store

import (
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

func (m *MetricsImpl) RecordMessageReceived(topic string)     {}
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
func (m *MetricsImpl) GetMessageCount() int64                     { return m.messagesSent.Load() }
func (m *MetricsImpl) GetTopics() []string                        { return []string{} }
func (m *MetricsImpl) GetMessageCountForTopic(topic string) int64 { return 0 }

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
