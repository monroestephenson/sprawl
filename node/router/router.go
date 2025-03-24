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
	dht          *dht.DHT
	store        *store.Store
	nodeID       string
	routeCache   *RouteCache
	metrics      *RouterMetrics
	ackTracker   *AckTracker
	semaphore    chan struct{}
	replication  *consensus.ReplicationManager
	optimizer    *RouteOptimizer
	loadBalancer *LoadBalancer
	routeTimeout time.Duration
}

type RouterMetrics struct {
	messagesSent     atomic.Int64
	messagesRouted   atomic.Int64
	routeCacheHits   atomic.Int64
	latencySum       atomic.Int64
	latencyCount     atomic.Int64
	messagesStored   atomic.Int64
	messagesReceived atomic.Int64
	routeErrors      atomic.Int64
	p99LatencyMs     atomic.Int64
	maxLatencyMs     atomic.Int64
	messageSize      atomic.Int64
	messageCount     atomic.Int64
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
	// Initialize the cache with a capacity of 10,000 entries, 5-minute TTL, and 8 shards
	cache := NewRouteCache(10000, 5*time.Minute, 8)

	// Initialize the load balancer with default config
	loadBalancer := NewLoadBalancer(defaultLoadBalancerConfig())

	// Initialize the route optimizer with default config
	optimizer := NewRouteOptimizer(defaultOptimizerConfig())

	return &Router{
		dht:          dht,
		store:        store,
		nodeID:       nodeID,
		routeCache:   cache,
		metrics:      &RouterMetrics{},
		ackTracker:   &AckTracker{pending: make(map[string]*MessageState), maxRetry: 3},
		semaphore:    make(chan struct{}, 200),
		replication:  replication,
		optimizer:    optimizer,
		loadBalancer: loadBalancer,
		routeTimeout: 5 * time.Second,
	}
}

// RouteMessage routes a message to the appropriate nodes
func (r *Router) RouteMessage(ctx context.Context, msg Message) error {
	start := time.Now()
	defer func() {
		latency := time.Since(start)
		r.metrics.RecordLatency(latency)

		// Update P99 and max latency metrics
		latencyMs := latency.Milliseconds()

		// Simple approximation for P99 latency
		// In production, we'd use a more sophisticated algorithm like HDR Histogram
		currentP99 := r.metrics.p99LatencyMs.Load()
		if latencyMs > currentP99 {
			r.metrics.p99LatencyMs.Store(latencyMs)
		}

		// Update max latency
		currentMax := r.metrics.maxLatencyMs.Load()
		if latencyMs > currentMax {
			r.metrics.maxLatencyMs.Store(latencyMs)
		}

		// Track message size
		r.metrics.messageSize.Add(int64(len(msg.Payload)))
		r.metrics.messageCount.Add(1)
	}()

	// Check if we've already processed this message
	r.ackTracker.mu.RLock()
	if _, exists := r.ackTracker.pending[msg.ID]; exists {
		r.ackTracker.mu.RUnlock()
		log.Printf("[Router] Skipping duplicate message %s", truncateID(msg.ID))
		return nil
	}
	r.ackTracker.mu.RUnlock()

	// Use a semaphore to limit concurrent message processing with retry logic
	backoffDuration := 50 * time.Millisecond
	maxRetries := 5

	for retry := 0; retry < maxRetries; retry++ {
		select {
		case r.semaphore <- struct{}{}:
			// acquired semaphore, proceed with message processing
			log.Printf("[Router] RouteMessage for %s: semaphore acquired", truncateID(msg.ID))
			defer func() {
				<-r.semaphore
			}()
			goto PROCESS_MESSAGE
		case <-ctx.Done():
			r.metrics.routeErrors.Add(1)
			return fmt.Errorf("context deadline exceeded while waiting for router semaphore: %w", ctx.Err())
		case <-time.After(backoffDuration):
			// Exponential backoff for next iteration
			backoffDuration *= 2
			continue
		}
	}

	// If we reach here, we failed to acquire the semaphore after all retries
	r.metrics.routeErrors.Add(1)
	return fmt.Errorf("failed to acquire routing semaphore after %d retries for message %s",
		maxRetries, truncateID(msg.ID))

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
	cachedNodes, cacheHit := r.routeCache.Get(msg.Topic)

	if cacheHit {
		nodes = cachedNodes
		r.metrics.RecordCacheHit()
	} else {
		// Cache miss - get nodes from DHT
		// Create a timeout context for DHT operation
		dhtCtx, cancel := context.WithTimeout(ctx, r.routeTimeout)
		defer cancel()

		// Try with context-aware DHT lookup if available
		dhtNodes, err := r.dht.GetNodesForTopicWithContext(dhtCtx, msg.Topic)
		if err != nil {
			// Fall back to regular lookup on error
			dhtNodes = r.dht.GetNodesForTopic(msg.Topic)
		}

		// Update the cache with the results
		if len(dhtNodes) > 0 {
			nodes = dhtNodes
			r.routeCache.Set(msg.Topic, nodes)
		}
	}

	if len(nodes) == 0 {
		r.metrics.routeErrors.Add(1)
		return fmt.Errorf("no nodes found for topic %s", msg.Topic)
	}

	// Apply load balancing to select the best nodes based on health metrics
	selectedNodes := r.loadBalancer.SelectNodes(nodes, msg)

	// If load balancer filtered out all nodes, use original nodes as fallback
	if len(selectedNodes) == 0 {
		selectedNodes = nodes
	}

	// Apply route optimization to order the nodes optimally
	optimizedNodes := r.optimizer.OptimizeRoute(selectedNodes, msg)

	// Check if we're one of the target nodes
	isTargetNode := false
	for _, node := range optimizedNodes {
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
			return nil
		}

		// If replication fails (e.g., we're not the leader), fall back to direct storage
		if err.Error() == "not the leader" {
			// Find the leader node
			var leaderNode *dht.NodeInfo
			leaderID := r.replication.GetLeader()

			for _, node := range optimizedNodes {
				if node.ID == leaderID {
					leaderNode = &node
					break
				}
			}

			if leaderNode != nil {
				// Forward to leader
				log.Printf("[Router] Forwarding message %s to leader node %s",
					truncateID(msg.ID), truncateID(leaderNode.ID))
				r.metrics.messagesRouted.Add(1)

				// Create forwarding context with timeout
				forwardCtx, cancel := context.WithTimeout(ctx, r.routeTimeout)
				defer cancel()

				startForward := time.Now()
				err := r.forwardToNode(forwardCtx, *leaderNode, msg)

				// Record latency for this specific path
				r.optimizer.UpdateLatency(r.nodeID, leaderNode.ID, time.Since(startForward))

				// Update load balancer metrics based on success/failure
				if err != nil {
					r.loadBalancer.RecordNodeFailure(leaderNode.ID)
					r.metrics.routeErrors.Add(1)
				} else {
					r.loadBalancer.RecordNodeSuccess(leaderNode.ID)
				}

				return err
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
			r.metrics.routeErrors.Add(1)
			return err
		}

		msgState.Destinations[r.nodeID] = true
		r.metrics.messagesRouted.Add(1)
		r.metrics.messagesStored.Add(1)

		// If we're the only target node, consider the message delivered
		if len(optimizedNodes) == 1 {
			return nil
		}
	}

	// Send to other nodes
	var wg sync.WaitGroup
	errChan := make(chan error, len(optimizedNodes))
	successNodeCount := atomic.Int32{}

	// Create a context with timeout for the forwarding operations
	forwardCtx, cancel := context.WithTimeout(ctx, r.routeTimeout)
	defer cancel()

	for _, node := range optimizedNodes {
		// Skip ourselves, we already handled that case
		if node.ID == r.nodeID {
			continue
		}

		// Skip nodes with open circuit breakers
		if r.loadBalancer.IsCircuitOpen(node.ID) {
			log.Printf("[Router] Skipping node %s due to open circuit breaker", truncateID(node.ID))
			continue
		}

		wg.Add(1)
		go func(targetNode dht.NodeInfo) {
			defer wg.Done()

			startForward := time.Now()
			err := r.forwardToNode(forwardCtx, targetNode, msg)
			forwardLatency := time.Since(startForward)

			// Update routing metrics
			r.optimizer.UpdateLatency(r.nodeID, targetNode.ID, forwardLatency)
			r.loadBalancer.UpdateNodeMetric(targetNode.ID, "response_latency", float64(forwardLatency.Milliseconds()))

			if err != nil {
				log.Printf("[Router] Error forwarding message %s to node %s: %v",
					truncateID(msg.ID), truncateID(targetNode.ID), err)
				errChan <- err

				// Record failure in load balancer
				r.loadBalancer.RecordNodeFailure(targetNode.ID)
			} else {
				// Add to successful destinations
				r.ackTracker.mu.Lock()
				if state, exists := r.ackTracker.pending[msg.ID]; exists {
					state.Destinations[targetNode.ID] = true
				}
				r.ackTracker.mu.Unlock()

				// Update success metrics
				successNodeCount.Add(1)
				r.metrics.messagesSent.Add(1)

				// Record success in load balancer
				r.loadBalancer.RecordNodeSuccess(targetNode.ID)

				// Update throughput metric
				r.loadBalancer.UpdateNodeMetric(targetNode.ID, "message_throughput", 1.0)
			}
		}(node)
	}

	// Wait for all forwarding operations to complete
	wg.Wait()
	close(errChan)

	// If we successfully delivered to at least one node, consider it a success
	if successNodeCount.Load() > 0 {
		r.metrics.messagesRouted.Add(1)
		return nil
	}

	// Otherwise, return the first error
	for err := range errChan {
		r.metrics.routeErrors.Add(1)
		return fmt.Errorf("failed to route message: %w", err)
	}

	// If we get here, it means we didn't forward to any nodes
	if isTargetNode {
		// If we handled it locally, that's still a success
		return nil
	}

	r.metrics.routeErrors.Add(1)
	return fmt.Errorf("no suitable nodes found for topic %s", msg.Topic)
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

// GetMetrics returns router metrics for monitoring
func (r *Router) GetMetrics() map[string]interface{} {
	metrics := map[string]interface{}{
		"messages_sent":     r.metrics.messagesSent.Load(),
		"messages_routed":   r.metrics.messagesRouted.Load(),
		"route_cache_hits":  r.metrics.routeCacheHits.Load(),
		"avg_latency_ms":    r.metrics.GetAverageLatency() / time.Millisecond,
		"p99_latency_ms":    r.metrics.p99LatencyMs.Load(),
		"max_latency_ms":    r.metrics.maxLatencyMs.Load(),
		"messages_stored":   r.metrics.messagesStored.Load(),
		"messages_received": r.metrics.messagesReceived.Load(),
		"route_errors":      r.metrics.routeErrors.Load(),
		"avg_message_size":  int64(0),
	}

	// Calculate average message size
	if count := r.metrics.messageCount.Load(); count > 0 {
		metrics["avg_message_size"] = r.metrics.messageSize.Load() / count
	}

	// Add cache metrics
	for k, v := range r.routeCache.GetMetrics() {
		metrics["cache_"+k] = v
	}

	// Add load balancer metrics
	for k, v := range r.loadBalancer.GetMetrics() {
		metrics["load_balancer_"+k] = v
	}

	// Add optimizer metrics
	for k, v := range r.optimizer.GetMetrics() {
		metrics["optimizer_"+k] = v
	}

	return metrics
}

// UpdateNodeMetrics updates the load balancer with node metrics
// This should be called periodically with metrics from the monitoring system
func (r *Router) UpdateNodeMetrics(nodeID string, cpuUsage, memoryUsage float64) {
	r.loadBalancer.UpdateNodeMetric(nodeID, "cpu_usage", cpuUsage)
	r.loadBalancer.UpdateNodeMetric(nodeID, "memory_usage", memoryUsage)
}

// InvalidateRoutesForNode removes a node from the route cache
// This should be called when a node becomes unhealthy or unavailable
func (r *Router) InvalidateRoutesForNode(nodeID string) {
	r.routeCache.RemoveNodeFromAll(nodeID)
}

// InvalidateRoute removes a specific topic from the route cache
// This should be called when topic ownership changes
func (r *Router) InvalidateRoute(topic string) {
	r.routeCache.Remove(topic)
}
