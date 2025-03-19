package router

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"sprawl/node/consensus"
	"sprawl/node/dht"
	"sprawl/store"
)

type Router struct {
	dht         *dht.DHT
	store       *store.Store
	nodeID      string
	routeCache  sync.Map
	metrics     *RouterMetrics
	ackTracker  *AckTracker
	semaphore   chan struct{}
	replication *consensus.ReplicationManager
}

type RouterMetrics struct {
	messagesSent     atomic.Int64
	messagesRouted   atomic.Int64
	routeCacheHits   atomic.Int64
	latencySum       atomic.Int64
	latencyCount     atomic.Int64
	messagesStored   atomic.Int64
	messagesReceived atomic.Int64
}

type AckTracker struct {
	mu       sync.RWMutex
	pending  map[string]*MessageState
	maxRetry int
}

type MessageState struct {
	Message      Message
	Attempts     int
	LastAttempt  time.Time
	Destinations map[string]bool // nodeID -> acked
	Done         chan struct{}
}

func (m *RouterMetrics) RecordCacheHit() {
	m.routeCacheHits.Add(1)
}

func (m *RouterMetrics) RecordLatency(d time.Duration) {
	m.latencySum.Add(int64(d))
	m.latencyCount.Add(1)
}

func (m *RouterMetrics) GetAverageLatency() time.Duration {
	count := m.latencyCount.Load()
	if count == 0 {
		return 0
	}
	return time.Duration(m.latencySum.Load() / count)
}

type Message struct {
	ID      string
	Topic   string
	Payload []byte
	TTL     int
}

func NewRouter(nodeID string, dht *dht.DHT, store *store.Store, replication *consensus.ReplicationManager) *Router {
	return &Router{
		dht:         dht,
		store:       store,
		nodeID:      nodeID,
		metrics:     &RouterMetrics{},
		ackTracker:  &AckTracker{pending: make(map[string]*MessageState), maxRetry: 3},
		semaphore:   make(chan struct{}, 50),
		replication: replication,
	}
}

// RouteMessage routes a message to the appropriate nodes
func (r *Router) RouteMessage(ctx context.Context, msg Message) error {
	start := time.Now()

	// Check if we've already processed this message
	r.ackTracker.mu.RLock()
	if _, exists := r.ackTracker.pending[msg.ID]; exists {
		r.ackTracker.mu.RUnlock()
		log.Printf("[Router] Skipping duplicate message %s", truncateID(msg.ID))
		return nil
	}
	r.ackTracker.mu.RUnlock()

	// Create message state for tracking
	msgState := &MessageState{
		Message:      msg,
		Attempts:     1,
		LastAttempt:  time.Now(),
		Destinations: make(map[string]bool),
		Done:         make(chan struct{}),
	}

	r.ackTracker.mu.Lock()
	r.ackTracker.pending[msg.ID] = msgState
	r.ackTracker.mu.Unlock()

	// Ensure cleanup of message state
	defer func() {
		r.ackTracker.mu.Lock()
		delete(r.ackTracker.pending, msg.ID)
		r.ackTracker.mu.Unlock()
		close(msgState.Done)
	}()

	// Check route cache first
	var nodes []dht.NodeInfo
	if cachedNodes, ok := r.routeCache.Load(msg.Topic); ok {
		r.metrics.RecordCacheHit()
		nodes = cachedNodes.([]dht.NodeInfo)
	} else {
		nodes = r.dht.GetNodesForTopic(msg.Topic)
		if len(nodes) > 0 {
			r.routeCache.Store(msg.Topic, nodes)
		}
	}

	if len(nodes) == 0 {
		return fmt.Errorf("no nodes found for topic %s", msg.Topic)
	}

	// If we are one of the target nodes and we're the leader, handle replication
	isTargetNode := false
	for _, node := range nodes {
		if node.ID == r.nodeID {
			isTargetNode = true
			break
		}
	}

	if isTargetNode {
		// Try to replicate through the consensus system first
		err := r.replication.ProposeEntry(msg.Topic, msg.Payload)
		if err == nil {
			// Message will be stored locally via replication commit callback
			r.metrics.messagesRouted.Add(1)
			r.metrics.messagesStored.Add(1)
			r.metrics.RecordLatency(time.Since(start))
			return nil
		}

		// If replication fails (e.g., we're not the leader), fall back to direct storage
		if err.Error() == "not the leader" {
			// Find the leader node
			var leaderNode *dht.NodeInfo
			for _, node := range nodes {
				if node.ID == r.replication.GetLeader() {
					leaderNode = &node
					break
				}
			}

			if leaderNode != nil {
				// Forward to leader
				r.metrics.messagesRouted.Add(1)
				return r.forwardToNode(ctx, *leaderNode, msg)
			}
		}

		// If no leader found or other error, store locally as fallback
		log.Printf("[Router] Falling back to local storage for message %s", truncateID(msg.ID))

		// Create a store.Message from the router.Message
		storeMsg := store.Message{
			ID:        msg.ID,
			Topic:     msg.Topic,
			Payload:   msg.Payload,
			Timestamp: time.Now(),
			TTL:       msg.TTL,
		}
		r.store.Publish(storeMsg)

		msgState.Destinations[r.nodeID] = true
		r.metrics.messagesRouted.Add(1)
		r.metrics.messagesStored.Add(1)

		// If we're the only target node, consider the message delivered
		if len(nodes) == 1 {
			r.metrics.RecordLatency(time.Since(start))
			return nil
		}
	}

	// Send to other nodes
	var wg sync.WaitGroup
	errChan := make(chan error, len(nodes))
	successCount := atomic.Int32{}
	nodeErrorsMu := sync.Mutex{}
	nodeErrors := make(map[string]error)

	for _, node := range nodes {
		if node.ID == r.nodeID {
			continue
		}

		wg.Add(1)
		go func(targetNode dht.NodeInfo) {
			defer wg.Done()

			// Acquire semaphore
			r.semaphore <- struct{}{}
			defer func() { <-r.semaphore }()

			err := r.forwardToNode(ctx, targetNode, msg)
			if err != nil {
				nodeErrorsMu.Lock()
				nodeErrors[targetNode.ID] = err
				nodeErrorsMu.Unlock()
				select {
				case errChan <- fmt.Errorf("failed to send to %s: %v", targetNode.ID, err):
				default:
				}
				return
			}

			r.metrics.messagesReceived.Add(1)
			r.ackTracker.mu.Lock()
			msgState.Destinations[targetNode.ID] = true
			r.ackTracker.mu.Unlock()
			successCount.Add(1)
			r.metrics.messagesSent.Add(1)
		}(node)
	}

	// Wait for all goroutines to finish
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
		close(errChan)
	}()

	// Wait for either completion or context cancellation
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		// Check if we have enough successful deliveries
		if successCount.Load() > 0 {
			r.metrics.RecordLatency(time.Since(start))
			return nil
		}
		// Return first error if no successes
		if err, ok := <-errChan; ok {
			return err
		}
		return fmt.Errorf("no successful deliveries")
	}
}

// truncateID safely truncates a message ID for logging
func truncateID(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

func (r *Router) forwardToNode(ctx context.Context, targetNode dht.NodeInfo, msg Message) error {
	// Create message payload
	payload := map[string]interface{}{
		"topic":   msg.Topic,
		"payload": string(msg.Payload),
		"id":      msg.ID,
		"ttl":     msg.TTL - 1, // Decrement TTL before forwarding
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	// Create HTTP client with timeouts
	client := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   2 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 20,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	// Create context with timeout
	sendCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	url := fmt.Sprintf("http://%s:%d/publish", targetNode.Address, targetNode.HTTPPort)
	req, err := http.NewRequestWithContext(sendCtx, "POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send message: %w", err)
	}
	defer resp.Body.Close()

	// Read response body with timeout
	bodyBytes, err := io.ReadAll(io.LimitReader(resp.Body, 1024))
	if err != nil {
		return fmt.Errorf("failed to read response: %w", err)
	}

	var result struct {
		Status string `json:"status"`
		Error  string `json:"error"`
	}
	if err := json.Unmarshal(bodyBytes, &result); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}

	if result.Status == "published" {
		log.Printf("[Router] Node %s acknowledged message %s", targetNode.ID[:8], msg.ID[:8])
		return nil
	} else if result.Status == "dropped" {
		log.Printf("[Router] Message %s dropped by node %s (TTL: %d)", msg.ID[:8], targetNode.ID[:8], msg.TTL)
		return fmt.Errorf("message dropped by node %s", targetNode.ID[:8])
	} else {
		return fmt.Errorf("unexpected response status: %s", result.Status)
	}
}

// GetMetrics returns the current router metrics
func (r *Router) GetMetrics() map[string]interface{} {
	return map[string]interface{}{
		"messages_sent":    r.metrics.messagesSent.Load(),
		"messages_routed":  r.metrics.messagesRouted.Load(),
		"route_cache_hits": r.metrics.routeCacheHits.Load(),
		"avg_latency_ms":   r.metrics.GetAverageLatency().Milliseconds(),
	}
}
