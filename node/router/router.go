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

	"sprawl/node/dht"
	"sprawl/store"
)

type Router struct {
	dht        *dht.DHT
	store      *store.Store
	nodeID     string
	routeCache sync.Map
	metrics    *RouterMetrics
	ackTracker *AckTracker
	semaphore  chan struct{} // Limit concurrent goroutines
}

type RouterMetrics struct {
	messagesSent   atomic.Int64
	messagesRouted atomic.Int64
	routeCacheHits atomic.Int64
	latencySum     atomic.Int64
	latencyCount   atomic.Int64
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

func NewRouter(nodeID string, dht *dht.DHT, store *store.Store) *Router {
	return &Router{
		dht:     dht,
		store:   store,
		nodeID:  nodeID,
		metrics: &RouterMetrics{},
		ackTracker: &AckTracker{
			pending:  make(map[string]*MessageState),
			maxRetry: 3,
		},
		semaphore: make(chan struct{}, 50), // Limit to 50 concurrent goroutines
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

	// Store message locally if we are one of the target nodes
	shouldStoreLocally := false
	for _, node := range nodes {
		if node.ID == r.nodeID {
			shouldStoreLocally = true
			break
		}
	}

	if shouldStoreLocally {
		log.Printf("[Router] Storing message %s locally for topic %s", truncateID(msg.ID), msg.Topic)
		r.store.Publish(msg.Topic, msg.Payload)
		msgState.Destinations[r.nodeID] = true
		r.metrics.messagesRouted.Add(1)

		// If we're the only target node, consider the message delivered
		if len(nodes) == 1 {
			r.metrics.RecordLatency(time.Since(start))
			return nil
		}
	}

	// Send to other nodes with retry logic
	var lastErr error
	for attempt := 1; attempt <= r.ackTracker.maxRetry; attempt++ {
		if attempt > 1 {
			log.Printf("[Router] Retry attempt %d for message %s", attempt, truncateID(msg.ID))
			time.Sleep(time.Duration(attempt) * 200 * time.Millisecond)
		}

		err := r.sendToNodes(ctx, msg, nodes, msgState)
		if err == nil {
			// Check if we have enough successful deliveries
			r.ackTracker.mu.RLock()
			successCount := len(msgState.Destinations)
			r.ackTracker.mu.RUnlock()

			// Consider message delivered if at least one node received it
			if successCount > 0 {
				log.Printf("[Router] Message %s delivered to %d nodes", truncateID(msg.ID), successCount)
				r.metrics.messagesRouted.Add(1)
				r.metrics.RecordLatency(time.Since(start))
				return nil
			}
		}
		lastErr = err
	}

	if lastErr != nil {
		log.Printf("[Router] Max retries reached for message %s: %v", truncateID(msg.ID), lastErr)
		return fmt.Errorf("failed to deliver message after %d attempts: %v", r.ackTracker.maxRetry, lastErr)
	}

	return fmt.Errorf("failed to deliver message to enough nodes")
}

// truncateID safely truncates a message ID for logging
func truncateID(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

func (r *Router) sendToNodes(ctx context.Context, msg Message, nodes []dht.NodeInfo, msgState *MessageState) error {
	// Don't send to self or already acked nodes
	var sendNodes []dht.NodeInfo
	r.ackTracker.mu.RLock()
	for _, node := range nodes {
		if node.ID != r.nodeID && !msgState.Destinations[node.ID] {
			sendNodes = append(sendNodes, node)
		}
	}
	r.ackTracker.mu.RUnlock()

	if len(sendNodes) == 0 {
		return nil
	}

	log.Printf("[Router] Sending message %s to %d nodes for topic %s (TTL: %d)",
		msg.ID[:8], len(sendNodes), msg.Topic, msg.TTL)

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

	// Send to each node with limited concurrency
	var wg sync.WaitGroup
	nodeErrors := make(map[string]error)
	var nodeErrorsMu sync.Mutex
	successCount := atomic.Int64{}

	// Create error channel to collect errors
	errChan := make(chan error, len(sendNodes))

	for _, node := range sendNodes {
		// Try to acquire semaphore with context
		select {
		case <-ctx.Done():
			return ctx.Err()
		case r.semaphore <- struct{}{}:
		}

		wg.Add(1)
		go func(targetNode dht.NodeInfo) {
			defer wg.Done()
			defer func() { <-r.semaphore }()

			// Create context with timeout
			sendCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()

			url := fmt.Sprintf("http://%s:%d/publish", targetNode.Address, targetNode.HTTPPort)
			req, err := http.NewRequestWithContext(sendCtx, "POST", url, bytes.NewBuffer(jsonData))
			if err != nil {
				nodeErrorsMu.Lock()
				nodeErrors[targetNode.ID] = fmt.Errorf("failed to create request: %w", err)
				nodeErrorsMu.Unlock()
				select {
				case errChan <- err:
				default:
				}
				return
			}

			req.Header.Set("Content-Type", "application/json")

			resp, err := client.Do(req)
			if err != nil {
				nodeErrorsMu.Lock()
				nodeErrors[targetNode.ID] = fmt.Errorf("failed to send message: %w", err)
				nodeErrorsMu.Unlock()
				select {
				case errChan <- err:
				default:
				}
				return
			}
			defer resp.Body.Close()

			// Read response body with timeout
			bodyBytes, err := io.ReadAll(io.LimitReader(resp.Body, 1024))
			if err != nil {
				nodeErrorsMu.Lock()
				nodeErrors[targetNode.ID] = fmt.Errorf("failed to read response: %w", err)
				nodeErrorsMu.Unlock()
				select {
				case errChan <- err:
				default:
				}
				return
			}

			var result struct {
				Status string `json:"status"`
				Error  string `json:"error"`
			}
			if err := json.Unmarshal(bodyBytes, &result); err != nil {
				nodeErrorsMu.Lock()
				nodeErrors[targetNode.ID] = fmt.Errorf("failed to decode response: %w", err)
				nodeErrorsMu.Unlock()
				select {
				case errChan <- err:
				default:
				}
				return
			}

			if result.Status == "published" {
				r.ackTracker.mu.Lock()
				msgState.Destinations[targetNode.ID] = true
				r.ackTracker.mu.Unlock()
				successCount.Add(1)
				r.metrics.messagesSent.Add(1)
				log.Printf("[Router] Node %s acknowledged message %s", targetNode.ID[:8], msg.ID[:8])
			} else if result.Status == "dropped" {
				log.Printf("[Router] Message %s dropped by node %s (TTL: %d)", msg.ID[:8], targetNode.ID[:8], msg.TTL)
				nodeErrorsMu.Lock()
				nodeErrors[targetNode.ID] = fmt.Errorf("message dropped by node")
				nodeErrorsMu.Unlock()
				select {
				case errChan <- fmt.Errorf("message dropped by node %s", targetNode.ID[:8]):
				default:
				}
			} else {
				select {
				case errChan <- fmt.Errorf("unexpected response status: %s", result.Status):
				default:
				}
			}
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
			return nil
		}
		// Return first error if no successes
		if err, ok := <-errChan; ok {
			return err
		}
		return fmt.Errorf("no successful deliveries")
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
