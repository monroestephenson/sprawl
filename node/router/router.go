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
	"strconv"
	"strings"
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
		semaphore:   make(chan struct{}, 200),
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

	// Use a semaphore to limit concurrent message processing with retry logic
	var acquired bool
	backoffDuration := 50 * time.Millisecond
	maxRetries := 5

	log.Printf("[Router] RouteMessage for %s: semaphore check, current capacity: %d/%d",
		truncateID(msg.ID), len(r.semaphore), cap(r.semaphore))

	for retry := 0; retry < maxRetries; retry++ {
		select {
		case r.semaphore <- struct{}{}:
			// acquired semaphore, proceed with message processing
			log.Printf("[Router] RouteMessage for %s: semaphore acquired, new capacity: %d/%d",
				truncateID(msg.ID), len(r.semaphore), cap(r.semaphore))
			defer func() {
				<-r.semaphore
				log.Printf("[Router] RouteMessage for %s: semaphore released, new capacity: %d/%d",
					truncateID(msg.ID), len(r.semaphore), cap(r.semaphore))
			}()
			goto PROCESS_MESSAGE
		case <-ctx.Done():
			return fmt.Errorf("context deadline exceeded while waiting for router semaphore: %w", ctx.Err())
		case <-time.After(backoffDuration):
			// Exponential backoff for next iteration
			backoffDuration *= 2
			log.Printf("[Router] Failed to acquire semaphore for message %s, retry %d/%d with timeout %v, current capacity: %d/%d",
				truncateID(msg.ID), retry+1, maxRetries, backoffDuration, len(r.semaphore), cap(r.semaphore))
			continue
		}
	}

	if !acquired {
		return fmt.Errorf("failed to acquire routing semaphore after %d retries for message %s",
			maxRetries, truncateID(msg.ID))
	}

PROCESS_MESSAGE:
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
				log.Printf("[Router] Forwarding message %s to leader node %s",
					truncateID(msg.ID), truncateID(leaderNode.ID))
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
		if err := r.store.Publish(storeMsg); err != nil {
			log.Printf("[Router] Error publishing message %s to local store: %v", truncateID(msg.ID), err)
			return err
		}

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
	// Create a copy of the message with decremented TTL
	forwardMsg := msg
	forwardMsg.TTL--

	// Retry configuration
	maxRetries := 3
	initialBackoff := 100 * time.Millisecond
	backoff := initialBackoff

	// Construct the URL for the target node
	url := fmt.Sprintf("http://%s:%d/publish", targetNode.Address, targetNode.HTTPPort)

	log.Printf("[Router] Forwarding message %s to node %s at %s",
		truncateID(msg.ID), truncateID(targetNode.ID), url)

	// Prepare the payload
	payload := map[string]interface{}{
		"id":      forwardMsg.ID,
		"topic":   forwardMsg.Topic,
		"payload": string(forwardMsg.Payload),
		"ttl":     forwardMsg.TTL,
	}

	// Convert payload to JSON
	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("error marshaling message to JSON: %w", err)
	}

	var lastErr error

	// Retry loop with exponential backoff
	for attempt := 0; attempt <= maxRetries; attempt++ {
		// Check if our context is still valid
		if ctx.Err() != nil {
			return fmt.Errorf("context cancelled before forwarding to %s: %w",
				truncateID(targetNode.ID), ctx.Err())
		}

		// If this is a retry, log it
		if attempt > 0 {
			log.Printf("[Router] Retrying forward to node %s for message %s (attempt %d/%d, backoff %v)",
				truncateID(targetNode.ID), truncateID(msg.ID), attempt, maxRetries, backoff)
		}

		// Set up for this attempt
		shouldRetry := false

		// Enhanced error context for HTTP requests
		client := &http.Client{
			Timeout: 5 * time.Second,
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 20,
				IdleConnTimeout:     30 * time.Second,
				DisableCompression:  true,
			},
		}

		// Create HTTP request
		req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(jsonPayload))
		if err != nil {
			lastErr = fmt.Errorf("error creating forward request to %s: %w",
				truncateID(targetNode.ID), err)
			continue
		}

		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Forwarded-By", r.nodeID)

		// Send the request
		resp, err := client.Do(req)
		if err != nil {
			// Check if the error is due to context cancellation or timeout
			if ctx.Err() != nil {
				return fmt.Errorf("forward request to %s cancelled or timed out: %w",
					truncateID(targetNode.ID), ctx.Err())
			}

			// More descriptive network error handling
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				lastErr = fmt.Errorf("forward request to %s timed out", truncateID(targetNode.ID))
				shouldRetry = true
			} else if strings.Contains(err.Error(), "dial tcp") || strings.Contains(err.Error(), "lookup") {
				// Check if it's a DNS or connection error
				lastErr = fmt.Errorf("connection error to %s (%s:%d): %w",
					truncateID(targetNode.ID), targetNode.Address, targetNode.HTTPPort, err)
				shouldRetry = true
			} else {
				lastErr = fmt.Errorf("forward request to %s failed: %w", truncateID(targetNode.ID), err)
				shouldRetry = true
			}

			// Skip to next retry iteration
			if shouldRetry && attempt < maxRetries {
				// Sleep for backoff duration before retrying
				select {
				case <-ctx.Done():
					return fmt.Errorf("context cancelled during retry: %w", ctx.Err())
				case <-time.After(backoff):
					// Increase backoff for next round
					backoff = backoff * 2
				}
				continue
			} else if shouldRetry {
				return lastErr
			}
		}

		// We should only get here if we have a valid response
		if resp != nil {
			defer resp.Body.Close()

			// Read response body for better logging
			body, _ := io.ReadAll(resp.Body)
			log.Printf("[Router] Forward response from %s: status=%d, body=%s",
				truncateID(targetNode.ID), resp.StatusCode, string(body))

			// Special handling for 501 Not Implemented
			if resp.StatusCode == http.StatusNotImplemented {
				lastErr = fmt.Errorf("node %s returned 501 Not Implemented - cross-node messaging not supported",
					truncateID(targetNode.ID))
				// Don't retry 501 errors - these are implementation errors, not transient
				return lastErr
			}

			// Check for "server too busy" response (503) which we should retry
			if resp.StatusCode == http.StatusServiceUnavailable {
				// Create a new reader for the body since we've already read it
				bodyReader := bytes.NewReader(body)

				// Try to parse it as JSON
				var errorResp struct {
					Error      string `json:"error"`
					RetryAfter string `json:"retry_after"`
				}

				retrySeconds := 1 // Default backoff is 1 second
				if json.NewDecoder(bodyReader).Decode(&errorResp) == nil && errorResp.RetryAfter != "" {
					if s, err := strconv.Atoi(errorResp.RetryAfter); err == nil {
						retrySeconds = s
					}
				}

				// If this was our last retry, give up
				if attempt == maxRetries {
					return fmt.Errorf("node %s is too busy after %d retries",
						truncateID(targetNode.ID), maxRetries+1)
				}

				// Otherwise, do a backoff for the suggested time (or our exponential backoff)
				backoff = time.Duration(retrySeconds) * time.Second
				if backoff < initialBackoff*(1<<uint(attempt)) {
					backoff = initialBackoff * (1 << uint(attempt))
				}

				lastErr = fmt.Errorf("node %s is busy, will retry in %v",
					truncateID(targetNode.ID), backoff)
				shouldRetry = true
			} else if resp.StatusCode >= 400 {
				// Handle other error responses with detailed information
				var errResp struct {
					Error string `json:"error"`
				}

				// Create a new reader for the body since we've already read it
				bodyReader := bytes.NewReader(body)

				if err := json.NewDecoder(bodyReader).Decode(&errResp); err == nil && errResp.Error != "" {
					lastErr = fmt.Errorf("node %s returned error (status %d): %s",
						truncateID(targetNode.ID), resp.StatusCode, errResp.Error)
				} else {
					lastErr = fmt.Errorf("node %s returned error status: %d",
						truncateID(targetNode.ID), resp.StatusCode)
				}
				shouldRetry = true
			} else {
				// Decode the response body
				var result struct {
					Status string `json:"status"`
					ID     string `json:"id"`
				}

				// Create a new reader for the body since we've already read it
				bodyReader := bytes.NewReader(body)

				if err := json.NewDecoder(bodyReader).Decode(&result); err != nil {
					lastErr = fmt.Errorf("error decoding response from %s: %w",
						truncateID(targetNode.ID), err)
					shouldRetry = true
				} else if result.Status != "published" && result.Status != "dropped" {
					// Check if the message was published or dropped
					lastErr = fmt.Errorf("unexpected status from %s: %s",
						truncateID(targetNode.ID), result.Status)
					shouldRetry = true
				} else {
					// Success!
					log.Printf("[Router] Successfully forwarded message %s to node %s, status: %s",
						truncateID(msg.ID), truncateID(targetNode.ID), result.Status)
					return nil
				}
			}

			// Handle retry logic
			if shouldRetry && attempt < maxRetries {
				// Sleep for backoff duration before retrying
				select {
				case <-ctx.Done():
					return fmt.Errorf("context cancelled during retry: %w", ctx.Err())
				case <-time.After(backoff):
					// Increase backoff for next round
					backoff = backoff * 2
				}
				continue
			} else if shouldRetry {
				return lastErr
			}
		}
	}

	// We should never reach here, but just in case
	return lastErr
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
