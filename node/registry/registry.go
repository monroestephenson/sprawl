package registry

import (
	"sync"
	"time"
)

// Registry manages distributed subscriber state
type Registry struct {
	mu             sync.RWMutex
	nodeID         string
	subscribers    map[string]*SubscriberState
	consumerGroups map[string]*ConsumerGroup
	topicOffsets   map[string]*TopicOffset
	metrics        *RegistryMetrics
}

// SubscriberState represents a subscriber's current state
type SubscriberState struct {
	ID       string    `json:"id"`
	Topics   []string  `json:"topics"`
	LastSeen time.Time `json:"last_seen"`
	NodeID   string    `json:"node_id"`
	IsActive bool      `json:"is_active"`
	GroupID  string    `json:"group_id,omitempty"`
	Metadata []byte    `json:"metadata,omitempty"`
}

// ConsumerGroup represents a group of subscribers
type ConsumerGroup struct {
	ID         string   `json:"id"`
	Topics     []string `json:"topics"`
	Members    []string `json:"members"`
	Generation int64    `json:"generation"`
	Leader     string   `json:"leader"`
	Metadata   []byte   `json:"metadata,omitempty"`
}

// TopicOffset tracks message offsets for topics
type TopicOffset struct {
	Topic          string    `json:"topic"`
	Partition      int       `json:"partition"`
	CurrentOffset  int64     `json:"current_offset"`
	CommitedOffset int64     `json:"committed_offset"`
	GroupID        string    `json:"group_id"`
	LastUpdate     time.Time `json:"last_update"`
}

// RegistryMetrics tracks registry statistics
type RegistryMetrics struct {
	ActiveSubscribers int64 `json:"active_subscribers"`
	ConsumerGroups    int64 `json:"consumer_groups"`
}

// NewRegistry creates a new registry instance
func NewRegistry(nodeID string) *Registry {
	return &Registry{
		nodeID:         nodeID,
		subscribers:    make(map[string]*SubscriberState),
		consumerGroups: make(map[string]*ConsumerGroup),
		topicOffsets:   make(map[string]*TopicOffset),
		metrics:        &RegistryMetrics{},
	}
}

// RegisterSubscriber adds or updates a subscriber
func (r *Registry) RegisterSubscriber(state *SubscriberState) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.subscribers[state.ID] = state
	r.metrics.ActiveSubscribers = int64(len(r.subscribers))
	return nil
}

// UpdateConsumerGroup updates a consumer group's state
func (r *Registry) UpdateConsumerGroup(group *ConsumerGroup) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.consumerGroups[group.ID] = group
	r.metrics.ConsumerGroups = int64(len(r.consumerGroups))
	return nil
}

// UpdateOffset updates the offset for a topic
func (r *Registry) UpdateOffset(offset *TopicOffset) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.topicOffsets[offset.Topic] = offset
	return nil
}

// GetSubscriber retrieves a subscriber's state
func (r *Registry) GetSubscriber(id string) (*SubscriberState, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	sub, ok := r.subscribers[id]
	return sub, ok
}

// GetConsumerGroup retrieves a consumer group's state
func (r *Registry) GetConsumerGroup(id string) (*ConsumerGroup, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	group, ok := r.consumerGroups[id]
	return group, ok
}

// GetOffset retrieves the offset for a topic
func (r *Registry) GetOffset(topic string) (*TopicOffset, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	offset, ok := r.topicOffsets[topic]
	return offset, ok
}

// GetMetrics returns the current registry metrics
func (r *Registry) GetMetrics() *RegistryMetrics {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return &RegistryMetrics{
		ActiveSubscribers: r.metrics.ActiveSubscribers,
		ConsumerGroups:    r.metrics.ConsumerGroups,
	}
}

// RemoveSubscriber removes a subscriber from the registry
func (r *Registry) RemoveSubscriber(id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.subscribers, id)
	r.metrics.ActiveSubscribers = int64(len(r.subscribers))
	return nil
}

// RemoveConsumerGroup removes a consumer group from the registry
func (r *Registry) RemoveConsumerGroup(id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.consumerGroups, id)
	r.metrics.ConsumerGroups = int64(len(r.consumerGroups))
	return nil
}

// GetSubscribersForTopic returns all subscribers for a given topic
func (r *Registry) GetSubscribersForTopic(topic string) []*SubscriberState {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var result []*SubscriberState
	for _, sub := range r.subscribers {
		for _, t := range sub.Topics {
			if t == topic {
				result = append(result, sub)
				break
			}
		}
	}
	return result
}

// GetConsumerGroupsForTopic returns all consumer groups for a given topic
func (r *Registry) GetConsumerGroupsForTopic(topic string) []*ConsumerGroup {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var result []*ConsumerGroup
	for _, group := range r.consumerGroups {
		for _, t := range group.Topics {
			if t == topic {
				result = append(result, group)
				break
			}
		}
	}
	return result
}
