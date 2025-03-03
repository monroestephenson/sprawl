package registry

import (
	"encoding/json"
	"fmt"
	"io"

	"github.com/hashicorp/raft"
)

// Command operation types
const (
	OpRegisterSubscriber  = "register_subscriber"
	OpUpdateConsumerGroup = "update_consumer_group"
	OpUpdateOffset        = "update_offset"
)

// Command represents a state change command
type Command struct {
	Op     string           `json:"op"`
	State  *SubscriberState `json:"state,omitempty"`
	Group  *ConsumerGroup   `json:"group,omitempty"`
	Topic  string           `json:"topic,omitempty"`
	Offset *TopicOffset     `json:"offset,omitempty"`
}

// RegistryFSM implements the Raft FSM interface
type RegistryFSM struct {
	registry *Registry
}

// NewRegistryFSM creates a new FSM for the registry
func NewRegistryFSM(registry *Registry) *RegistryFSM {
	return &RegistryFSM{
		registry: registry,
	}
}

// Apply applies a Raft log entry to the registry
func (f *RegistryFSM) Apply(log *raft.Log) interface{} {
	var cmd Command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		fmt.Printf("FSM: Failed to unmarshal command: %v\n", err)
		return fmt.Errorf("failed to unmarshal command: %w", err)
	}

	f.registry.mu.Lock()
	defer f.registry.mu.Unlock()

	switch cmd.Op {
	case OpRegisterSubscriber:
		if cmd.State == nil {
			fmt.Printf("FSM: Subscriber state is nil\n")
			return fmt.Errorf("subscriber state is nil")
		}
		f.registry.subscribers[cmd.State.ID] = cmd.State
		f.registry.metrics.ActiveSubscribers = int64(len(f.registry.subscribers))
		fmt.Printf("FSM: Registered subscriber %s\n", cmd.State.ID)

	case OpUpdateConsumerGroup:
		if cmd.Group == nil {
			fmt.Printf("FSM: Consumer group is nil\n")
			return fmt.Errorf("consumer group is nil")
		}
		f.registry.consumerGroups[cmd.Group.ID] = cmd.Group
		f.registry.metrics.ConsumerGroups = int64(len(f.registry.consumerGroups))
		fmt.Printf("FSM: Updated consumer group %s with %d members\n", cmd.Group.ID, len(cmd.Group.Members))

	case OpUpdateOffset:
		if cmd.Offset == nil {
			fmt.Printf("FSM: Offset is nil\n")
			return fmt.Errorf("offset is nil")
		}
		f.registry.topicOffsets[cmd.Topic] = cmd.Offset
		fmt.Printf("FSM: Updated offset for topic %s\n", cmd.Topic)

	default:
		fmt.Printf("FSM: Unknown command operation: %s\n", cmd.Op)
		return fmt.Errorf("unknown command operation: %s", cmd.Op)
	}

	return nil
}

// Snapshot returns a snapshot of the registry state
func (f *RegistryFSM) Snapshot() (raft.FSMSnapshot, error) {
	f.registry.mu.RLock()
	defer f.registry.mu.RUnlock()

	state := registryState{
		Subscribers:    f.registry.subscribers,
		ConsumerGroups: f.registry.consumerGroups,
		TopicOffsets:   f.registry.topicOffsets,
	}

	return &registrySnapshot{state: state}, nil
}

// Restore restores the registry state from a snapshot
func (f *RegistryFSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	var state registryState
	if err := json.NewDecoder(rc).Decode(&state); err != nil {
		return fmt.Errorf("failed to decode state: %w", err)
	}

	f.registry.mu.Lock()
	defer f.registry.mu.Unlock()

	f.registry.subscribers = state.Subscribers
	f.registry.consumerGroups = state.ConsumerGroups
	f.registry.topicOffsets = state.TopicOffsets

	// Update metrics
	f.registry.metrics.ActiveSubscribers = int64(len(state.Subscribers))
	f.registry.metrics.ConsumerGroups = int64(len(state.ConsumerGroups))

	return nil
}

// registryState represents the complete state of the registry
type registryState struct {
	Subscribers    map[string]*SubscriberState `json:"subscribers"`
	ConsumerGroups map[string]*ConsumerGroup   `json:"consumer_groups"`
	TopicOffsets   map[string]*TopicOffset     `json:"topic_offsets"`
}

// registrySnapshot implements the FSMSnapshot interface
type registrySnapshot struct {
	state registryState
}

// Persist saves the snapshot to the given sink
func (s *registrySnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		data, err := json.Marshal(s.state)
		if err != nil {
			return fmt.Errorf("failed to marshal state: %w", err)
		}

		if _, err := sink.Write(data); err != nil {
			return fmt.Errorf("failed to write state: %w", err)
		}

		return sink.Close()
	}()

	if err != nil {
		sink.Cancel()
		return err
	}

	return nil
}

// Release is a no-op
func (s *registrySnapshot) Release() {}
