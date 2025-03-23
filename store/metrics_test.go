package store

import (
	"testing"
)

func TestNewMetrics(t *testing.T) {
	metrics := NewMetrics()
	if metrics == nil {
		t.Fatal("NewMetrics returned nil")
	}
}

func TestRecordMessage(t *testing.T) {
	metrics := NewMetrics()

	// Record some messages
	metrics.RecordMessage(true)  // sent message
	metrics.RecordMessage(true)  // sent message
	metrics.RecordMessage(false) // received message

	// Check the message count
	stats := metrics.GetSnapshot()

	sentMessages, ok := stats["messages_sent"].(int64)
	if !ok {
		t.Fatal("messages_sent not found in snapshot or wrong type")
	}
	if sentMessages != 2 {
		t.Errorf("Expected 2 sent messages, got %d", sentMessages)
	}

	receivedMessages, ok := stats["messages_received"].(int64)
	if !ok {
		t.Fatal("messages_received not found in snapshot or wrong type")
	}
	if receivedMessages != 1 {
		t.Errorf("Expected 1 received message, got %d", receivedMessages)
	}
}

func TestMessageEventMethods(t *testing.T) {
	metrics := NewMetrics()

	// These methods should not panic
	metrics.RecordMessageReceived("test-topic")
	metrics.RecordMessageDelivered("test-topic")
	metrics.RecordSubscriptionAdded("test-topic")
	metrics.RecordSubscriptionRemoved("test-topic")
}

func TestMetricsGetters(t *testing.T) {
	metrics := NewMetrics()

	// Add some data
	metrics.RecordMessage(true)  // sent
	metrics.RecordMessage(true)  // sent
	metrics.RecordMessage(false) // received

	// Test GetMessageCount (should return sent messages)
	if count := metrics.GetMessageCount(); count != 2 {
		t.Errorf("Expected 2 sent messages, got %d", count)
	}

	// Test GetTopics - should return empty array in the base implementation
	topics := metrics.GetTopics()
	if len(topics) != 0 {
		t.Errorf("Expected 0 topics, got %d", len(topics))
	}

	// Test GetMessageCountForTopic - should return 0 in the base implementation
	if count := metrics.GetMessageCountForTopic("any-topic"); count != 0 {
		t.Errorf("Expected 0 messages for topic, got %d", count)
	}
}

func TestNoOpMetrics(t *testing.T) {
	// Test with NoOpMetrics
	var metrics NoOpMetrics

	// These should not panic
	metrics.RecordMessageReceived("topic")
	metrics.RecordMessageDelivered("topic")
	metrics.RecordSubscriptionAdded("topic")
	metrics.RecordSubscriptionRemoved("topic")

	// These should return sensible defaults
	if count := metrics.GetMessageCount(); count != 0 {
		t.Errorf("Expected 0 from NoOpMetrics, got %d", count)
	}

	if topics := metrics.GetTopics(); len(topics) != 0 {
		t.Errorf("Expected empty topics list from NoOpMetrics, got %d topics", len(topics))
	}

	if count := metrics.GetMessageCountForTopic("topic"); count != 0 {
		t.Errorf("Expected 0 from NoOpMetrics, got %d", count)
	}
}
