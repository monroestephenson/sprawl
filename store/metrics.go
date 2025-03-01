package store

import (
	"sync/atomic"
)

type Metrics struct {
	messagesSent     atomic.Int64
	messagesReceived atomic.Int64
}

func NewMetrics() *Metrics {
	return &Metrics{}
}

func (m *Metrics) RecordMessage(sent bool) {
	if sent {
		m.messagesSent.Add(1)
	} else {
		m.messagesReceived.Add(1)
	}
}

func (m *Metrics) GetSnapshot() map[string]interface{} {
	return map[string]interface{}{
		"messages_sent":     m.messagesSent.Load(),
		"messages_received": m.messagesReceived.Load(),
	}
}
