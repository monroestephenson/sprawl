package registry

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func TestSubscriberStateReplication(t *testing.T) {
	reg := NewRegistry("test-node")
	fsm := NewRegistryFSM(reg)

	// Test subscriber registration
	subState := &SubscriberState{
		ID:       "sub1",
		Topics:   []string{"test-topic"},
		LastSeen: time.Now(),
		NodeID:   "node1",
		IsActive: true,
		GroupID:  "group1",
	}

	cmd := Command{
		Op:    OpRegisterSubscriber,
		State: subState,
	}

	data, err := encodeCommand(cmd)
	if err != nil {
		t.Fatalf("Failed to encode command: %v", err)
	}

	// Apply the command
	result := fsm.Apply(&raft.Log{Data: data})
	if result != nil {
		t.Errorf("Apply returned error: %v", result)
	}

	// Verify subscriber state
	reg.mu.RLock()
	stored := reg.subscribers[subState.ID]
	reg.mu.RUnlock()

	if stored == nil {
		t.Fatal("Subscriber state not stored")
	}
	if stored.ID != subState.ID {
		t.Errorf("Got subscriber ID %s, want %s", stored.ID, subState.ID)
	}
}

func TestConsumerGroupManagement(t *testing.T) {
	reg := NewRegistry("test-node")
	fsm := NewRegistryFSM(reg)

	// Test consumer group creation
	group := &ConsumerGroup{
		ID:         "group1",
		Topics:     []string{"test-topic"},
		Members:    []string{"sub1", "sub2"},
		Generation: 1,
		Leader:     "sub1",
	}

	cmd := Command{
		Op:    OpUpdateConsumerGroup,
		Group: group,
	}

	data, err := encodeCommand(cmd)
	if err != nil {
		t.Fatalf("Failed to encode command: %v", err)
	}

	// Apply the command
	result := fsm.Apply(&raft.Log{Data: data})
	if result != nil {
		t.Errorf("Apply returned error: %v", result)
	}

	// Verify consumer group state
	reg.mu.RLock()
	stored := reg.consumerGroups[group.ID]
	reg.mu.RUnlock()

	if stored == nil {
		t.Fatal("Consumer group not stored")
	}
	if stored.ID != group.ID {
		t.Errorf("Got group ID %s, want %s", stored.ID, group.ID)
	}
	if len(stored.Members) != len(group.Members) {
		t.Errorf("Got %d members, want %d", len(stored.Members), len(group.Members))
	}
}

func TestOffsetTracking(t *testing.T) {
	reg := NewRegistry("test-node")
	fsm := NewRegistryFSM(reg)

	// Test offset tracking
	offset := &TopicOffset{
		Topic:          "test-topic",
		Partition:      0,
		CurrentOffset:  100,
		CommitedOffset: 90,
		GroupID:        "group1",
		LastUpdate:     time.Now(),
	}

	cmd := Command{
		Op:     OpUpdateOffset,
		Topic:  offset.Topic,
		Offset: offset,
	}

	data, err := encodeCommand(cmd)
	if err != nil {
		t.Fatalf("Failed to encode command: %v", err)
	}

	// Apply the command
	result := fsm.Apply(&raft.Log{Data: data})
	if result != nil {
		t.Errorf("Apply returned error: %v", result)
	}

	// Verify offset state
	reg.mu.RLock()
	stored := reg.topicOffsets[offset.Topic]
	reg.mu.RUnlock()

	if stored == nil {
		t.Fatal("Offset not stored")
	}
	if stored.CurrentOffset != offset.CurrentOffset {
		t.Errorf("Got current offset %d, want %d", stored.CurrentOffset, offset.CurrentOffset)
	}
	if stored.CommitedOffset != offset.CommitedOffset {
		t.Errorf("Got committed offset %d, want %d", stored.CommitedOffset, offset.CommitedOffset)
	}
}

func TestRebalancingProtocol(t *testing.T) {
	reg := NewRegistry("test-node")
	fsm := NewRegistryFSM(reg)

	// Create initial consumer group
	group := &ConsumerGroup{
		ID:         "group1",
		Topics:     []string{"test-topic"},
		Members:    []string{"sub1", "sub2"},
		Generation: 1,
		Leader:     "sub1",
	}

	cmd := Command{
		Op:    OpUpdateConsumerGroup,
		Group: group,
	}

	data, err := encodeCommand(cmd)
	if err != nil {
		t.Fatalf("Failed to encode command: %v", err)
	}

	// Apply the command
	result := fsm.Apply(&raft.Log{Data: data})
	if result != nil {
		t.Errorf("Apply returned error: %v", result)
	}

	// Add a new member and trigger rebalance
	group.Members = append(group.Members, "sub3")
	group.Generation++

	cmd = Command{
		Op:    OpUpdateConsumerGroup,
		Group: group,
	}

	data, err = encodeCommand(cmd)
	if err != nil {
		t.Fatalf("Failed to encode command: %v", err)
	}

	// Apply the rebalance
	result = fsm.Apply(&raft.Log{Data: data})
	if result != nil {
		t.Errorf("Apply returned error: %v", result)
	}

	// Verify updated group state
	reg.mu.RLock()
	stored := reg.consumerGroups[group.ID]
	reg.mu.RUnlock()

	if stored == nil {
		t.Fatal("Consumer group not stored after rebalance")
	}
	if stored.Generation != group.Generation {
		t.Errorf("Got generation %d, want %d", stored.Generation, group.Generation)
	}
	if len(stored.Members) != 3 {
		t.Errorf("Got %d members after rebalance, want 3", len(stored.Members))
	}
}

func encodeCommand(cmd Command) ([]byte, error) {
	return json.Marshal(cmd)
}
