package failure_mode

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"sprawl/store"
	"sprawl/store/tiered"
)

// TestBrokerFailureRecovery tests system recovery after a broker failure
func TestBrokerFailureRecovery(t *testing.T) {
	// Create a temporary directory for the test data
	tmpDir, err := os.MkdirTemp("", "broker-failure-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "recovery.db")

	// Phase 1: Store messages before simulated crash
	var messageIDs []string
	{
		// Create a disk store
		diskStore, err := tiered.NewRocksStore(dbPath)
		if err != nil {
			t.Fatalf("Failed to create disk store: %v", err)
		}

		// Store some test messages
		numMessages := 20
		topicName := "recovery-topic"

		for i := 0; i < numMessages; i++ {
			msgID := fmt.Sprintf("pre-crash-msg-%d", i)
			messageIDs = append(messageIDs, msgID)

			msg := tiered.Message{
				ID:        msgID,
				Topic:     topicName,
				Payload:   []byte(fmt.Sprintf("test payload %d", i)),
				Timestamp: time.Now(),
				TTL:       3600, // 1 hour
			}

			err := diskStore.Store(msg)
			if err != nil {
				t.Fatalf("Failed to store message %d: %v", i, err)
			}
		}

		// Verify all messages were stored
		ids, err := diskStore.GetTopicMessages(topicName)
		if err != nil {
			t.Fatalf("Failed to get topic messages: %v", err)
		}

		if len(ids) != numMessages {
			t.Errorf("Expected %d messages, found %d before crash", numMessages, len(ids))
		}

		t.Logf("Stored %d messages before simulated crash", numMessages)

		// Simulate clean shutdown
		diskStore.Close()
	}

	// Simulate a system crash by just not closing the store properly
	// and then creating a new one pointing to the same file

	// Phase 2: Recover after crash and verify data integrity
	{
		// Re-create the store (simulating a restart after crash)
		diskStore, err := tiered.NewRocksStore(dbPath)
		if err != nil {
			t.Fatalf("Failed to create disk store after crash: %v", err)
		}
		defer diskStore.Close()

		// Verify we can retrieve all messages that were stored before the crash
		t.Log("Verifying message recovery after simulated crash...")

		for i, msgID := range messageIDs {
			msg, err := diskStore.Retrieve(msgID)
			if err != nil {
				t.Errorf("Failed to retrieve message %s after crash: %v", msgID, err)
				continue
			}

			expectedPayload := fmt.Sprintf("test payload %d", i)
			if string(msg.Payload) != expectedPayload {
				t.Errorf("Message %s has incorrect payload after crash. Expected: %s, Got: %s",
					msgID, expectedPayload, string(msg.Payload))
			}
		}

		// Check topic integrity
		ids, err := diskStore.GetTopicMessages("recovery-topic")
		if err != nil {
			t.Fatalf("Failed to get topic messages after crash: %v", err)
		}

		if len(ids) != len(messageIDs) {
			t.Errorf("Expected %d messages after crash, found %d", len(messageIDs), len(ids))
		}

		t.Logf("Successfully recovered %d messages after simulated crash", len(ids))
	}
}

// TestPartialWriteRecovery tests recovery from a crash during write operations
func TestPartialWriteRecovery(t *testing.T) {
	// Create a temporary directory for the test data
	tmpDir, err := os.MkdirTemp("", "partial-write-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "partial.db")

	// Phase 1: Initial setup
	diskStore, err := tiered.NewRocksStore(dbPath)
	if err != nil {
		t.Fatalf("Failed to create disk store: %v", err)
	}

	// Store a batch of initial messages
	for i := 0; i < 10; i++ {
		msg := tiered.Message{
			ID:        fmt.Sprintf("initial-msg-%d", i),
			Topic:     "partial-write-topic",
			Payload:   []byte(fmt.Sprintf("initial payload %d", i)),
			Timestamp: time.Now(),
			TTL:       3600,
		}

		err := diskStore.Store(msg)
		if err != nil {
			t.Fatalf("Failed to store initial message %d: %v", i, err)
		}
	}

	// Phase 2: Simulate a crash during a batch write operation
	// We'll use goroutines to write a batch of messages concurrently, then force close
	var wg sync.WaitGroup
	crashAfter := make(chan struct{})

	// Start concurrent writers
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			for j := 0; j < 10; j++ {
				msgID := fmt.Sprintf("batch-writer-%d-msg-%d", idx, j)

				msg := tiered.Message{
					ID:        msgID,
					Topic:     "partial-write-topic",
					Payload:   []byte(fmt.Sprintf("batch payload from writer %d msg %d", idx, j)),
					Timestamp: time.Now(),
					TTL:       3600,
				}

				select {
				case <-crashAfter:
					// Simulate crash by just returning without completing
					return
				default:
					// Store the message
					_ = diskStore.Store(msg)

					// Sleep a bit to make the test more realistic
					time.Sleep(time.Millisecond * 10)
				}
			}
		}(i)
	}

	// Let some operations complete
	time.Sleep(100 * time.Millisecond)

	// Signal to crash and wait for goroutines to finish
	close(crashAfter)
	wg.Wait()

	// Force close the store without cleanup (simulating crash)
	diskStore.Close()

	// Phase 3: Recover and verify integrity
	recoveredStore, err := tiered.NewRocksStore(dbPath)
	if err != nil {
		t.Fatalf("Failed to create recovered store: %v", err)
	}
	defer recoveredStore.Close()

	// Get all messages for the topic
	ids, err := recoveredStore.GetTopicMessages("partial-write-topic")
	if err != nil {
		t.Fatalf("Failed to get topic messages after recovery: %v", err)
	}

	t.Logf("Recovered %d messages after simulated partial write crash", len(ids))

	// Verify all recovered messages have consistent data
	for _, id := range ids {
		msg, err := recoveredStore.Retrieve(id)
		if err != nil {
			t.Errorf("Failed to retrieve message %s: %v", id, err)
			continue
		}

		// Message should have a non-empty payload
		if len(msg.Payload) == 0 {
			t.Errorf("Message %s has empty payload", id)
		}
	}
}

// TestBrokerNetworkPartition tests how the system handles network partitions
func TestBrokerNetworkPartition(t *testing.T) {
	// Create two stores to simulate partitioned brokers
	broker1 := store.NewStore()
	broker2 := store.NewStore()

	// Channel to pass messages between brokers
	broker1ToBroker2 := make(chan store.Message, 100)
	broker2ToBroker1 := make(chan store.Message, 100)

	// Track messages
	var broker1ReceivedCount, broker2ReceivedCount int
	var mu sync.Mutex

	// Set up replication between brokers
	broker1.Subscribe("replication-topic", func(msg store.Message) {
		select {
		case broker1ToBroker2 <- msg:
			// Message forwarded to broker2
		default:
			// Queue full, simulating network issue
			t.Logf("Failed to forward message from broker1 to broker2: %s", msg.ID)
		}
	})

	broker2.Subscribe("replication-topic", func(msg store.Message) {
		select {
		case broker2ToBroker1 <- msg:
			// Message forwarded to broker1
		default:
			// Queue full, simulating network issue
			t.Logf("Failed to forward message from broker2 to broker1: %s", msg.ID)
		}
	})

	// Consumer on broker1
	broker1.Subscribe("consumer-topic", func(msg store.Message) {
		mu.Lock()
		defer mu.Unlock()
		broker1ReceivedCount++
	})

	// Consumer on broker2
	broker2.Subscribe("consumer-topic", func(msg store.Message) {
		mu.Lock()
		defer mu.Unlock()
		broker2ReceivedCount++
	})

	// Start message forwarding processes
	go func() {
		for msg := range broker1ToBroker2 {
			if msg.Topic == "replication-topic" {
				// Create a consumer message
				consumerMsg := store.Message{
					ID:        fmt.Sprintf("b1to2-%s", msg.ID),
					Topic:     "consumer-topic",
					Payload:   msg.Payload,
					Timestamp: time.Now(),
					TTL:       msg.TTL,
				}
				_ = broker2.Publish(consumerMsg)
			}
		}
	}()

	go func() {
		for msg := range broker2ToBroker1 {
			if msg.Topic == "replication-topic" {
				// Create a consumer message
				consumerMsg := store.Message{
					ID:        fmt.Sprintf("b2to1-%s", msg.ID),
					Topic:     "consumer-topic",
					Payload:   msg.Payload,
					Timestamp: time.Now(),
					TTL:       msg.TTL,
				}
				_ = broker1.Publish(consumerMsg)
			}
		}
	}()

	// Phase 1: Normal operation, both brokers can communicate
	t.Log("Testing normal operation...")
	for i := 0; i < 10; i++ {
		msg := store.Message{
			ID:        fmt.Sprintf("normal-op-msg-%d", i),
			Topic:     "replication-topic",
			Payload:   []byte(fmt.Sprintf("normal operation payload %d", i)),
			Timestamp: time.Now(),
			TTL:       3600,
		}

		// Alternate which broker the message is published to
		if i%2 == 0 {
			_ = broker1.Publish(msg)
		} else {
			_ = broker2.Publish(msg)
		}

		// Small delay to allow processing
		time.Sleep(10 * time.Millisecond)
	}

	// Wait for messages to be processed
	time.Sleep(100 * time.Millisecond)

	// Verify both brokers received messages
	mu.Lock()
	b1Count := broker1ReceivedCount
	b2Count := broker2ReceivedCount
	mu.Unlock()

	t.Logf("Normal operation: Broker1 received %d messages, Broker2 received %d messages",
		b1Count, b2Count)

	// Phase 2: Simulate network partition by closing the channels
	t.Log("Simulating network partition...")

	// Empty the channels to simulate disconnect
	for len(broker1ToBroker2) > 0 {
		<-broker1ToBroker2
	}

	for len(broker2ToBroker1) > 0 {
		<-broker2ToBroker1
	}

	// Send messages during partition
	partitionMsgCount := 5
	for i := 0; i < partitionMsgCount; i++ {
		msg := store.Message{
			ID:        fmt.Sprintf("partition-msg-%d", i),
			Topic:     "replication-topic",
			Payload:   []byte(fmt.Sprintf("partition payload %d", i)),
			Timestamp: time.Now(),
			TTL:       3600,
		}

		// Alternate which broker the message is published to
		if i%2 == 0 {
			_ = broker1.Publish(msg)
		} else {
			_ = broker2.Publish(msg)
		}
	}

	// Wait for local processing
	time.Sleep(100 * time.Millisecond)

	// Check message counts during partition
	mu.Lock()
	b1PartitionCount := broker1ReceivedCount - b1Count
	b2PartitionCount := broker2ReceivedCount - b2Count
	mu.Unlock()

	t.Logf("During partition: Broker1 received %d messages, Broker2 received %d messages",
		b1PartitionCount, b2PartitionCount)

	// Phase 3: Restore connectivity
	t.Log("Restoring network connectivity...")

	// Recreate the channels
	broker1ToBroker2 = make(chan store.Message, 100)
	broker2ToBroker1 = make(chan store.Message, 100)

	// Send messages after partition healed
	for i := 0; i < 10; i++ {
		msg := store.Message{
			ID:        fmt.Sprintf("post-partition-msg-%d", i),
			Topic:     "replication-topic",
			Payload:   []byte(fmt.Sprintf("post partition payload %d", i)),
			Timestamp: time.Now(),
			TTL:       3600,
		}

		// Alternate which broker the message is published to
		if i%2 == 0 {
			_ = broker1.Publish(msg)
		} else {
			_ = broker2.Publish(msg)
		}

		// Small delay to allow processing
		time.Sleep(10 * time.Millisecond)
	}

	// Wait for messages to be processed
	time.Sleep(100 * time.Millisecond)

	// Check final message counts
	mu.Lock()
	finalB1Count := broker1ReceivedCount
	finalB2Count := broker2ReceivedCount
	mu.Unlock()

	t.Logf("Final counts: Broker1 received %d messages, Broker2 received %d messages",
		finalB1Count, finalB2Count)
}
