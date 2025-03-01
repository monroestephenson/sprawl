package router

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
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
}

type RouterMetrics struct {
	messagesSent   atomic.Int64
	messagesRouted atomic.Int64
	routeCacheHits atomic.Int64
	latencySum     atomic.Int64
	latencyCount   atomic.Int64
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
	}
}

// RouteMessage routes a message to the appropriate nodes
func (r *Router) RouteMessage(ctx context.Context, msg Message) error {
	start := time.Now()

	// Record DHT lookup in metrics
	r.metrics.messagesRouted.Add(1)

	// Check route cache first
	var nodes []dht.NodeInfo
	if cachedNodes, ok := r.routeCache.Load(msg.Topic); ok {
		r.metrics.RecordCacheHit()
		log.Printf("[Router] Cache hit for topic %s", msg.Topic)
		nodes = cachedNodes.([]dht.NodeInfo)
	} else {
		// Get nodes from DHT and store in cache
		nodes = r.dht.GetNodesForTopic(msg.Topic)
		if len(nodes) > 0 {
			log.Printf("[Router] Found %d nodes for topic %s", len(nodes), msg.Topic)
			r.routeCache.Store(msg.Topic, nodes)
		}
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
		log.Printf("[Router] Storing message locally for topic %s", msg.Topic)
		r.store.Publish(msg.Topic, msg.Payload)
	} else {
		log.Printf("[Router] Not storing message locally for topic %s (not a target node)", msg.Topic)
	}

	// Send to other nodes if any exist
	if len(nodes) > 0 {
		err := r.sendToNodes(ctx, msg, nodes)
		r.metrics.RecordLatency(time.Since(start))
		return err
	}

	// If no nodes found and we didn't store locally, this is an error
	if !shouldStoreLocally {
		return fmt.Errorf("no nodes found for topic %s and not storing locally", msg.Topic)
	}

	r.metrics.RecordLatency(time.Since(start))
	return nil
}

func (r *Router) sendToNodes(ctx context.Context, msg Message, nodes []dht.NodeInfo) error {
	// Don't send to self
	var sendNodes []dht.NodeInfo
	for _, node := range nodes {
		if node.ID != r.nodeID {
			sendNodes = append(sendNodes, node)
		}
	}

	log.Printf("[Router] Sending message %s to %d nodes for topic %s", msg.ID[:8], len(sendNodes), msg.Topic)

	if len(sendNodes) > 0 {
		r.metrics.messagesSent.Add(int64(len(sendNodes)))

		// Create message payload that matches the expected format
		payload := map[string]interface{}{
			"topic":   msg.Topic,
			"payload": string(msg.Payload),
			"id":      msg.ID,
			"ttl":     msg.TTL,
		}

		jsonData, err := json.Marshal(payload)
		if err != nil {
			return fmt.Errorf("failed to marshal message: %w", err)
		}

		// Send to each node
		var wg sync.WaitGroup
		errors := make(chan error, len(sendNodes))
		successCount := atomic.Int32{}

		for _, node := range sendNodes {
			wg.Add(1)
			go func(targetNode dht.NodeInfo) {
				defer wg.Done()

				url := fmt.Sprintf("http://%s:%d/publish", targetNode.Address, targetNode.HTTPPort)
				log.Printf("[Router] Sending message %s to node %s at %s", msg.ID[:8], targetNode.ID[:8], url)

				req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(jsonData))
				if err != nil {
					errors <- fmt.Errorf("failed to create request for node %s: %w", targetNode.ID, err)
					return
				}

				req.Header.Set("Content-Type", "application/json")

				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					errors <- fmt.Errorf("failed to send message to node %s: %w", targetNode.ID, err)
					return
				}
				defer resp.Body.Close()

				if resp.StatusCode != http.StatusOK {
					errors <- fmt.Errorf("node %s responded with status %d", targetNode.ID, resp.StatusCode)
					return
				}

				// Parse response to check status
				var result struct {
					Status string `json:"status"`
					Error  string `json:"error"`
				}
				if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
					errors <- fmt.Errorf("failed to decode response from node %s: %w", targetNode.ID, err)
					return
				}

				if result.Status == "published" {
					successCount.Add(1)
					log.Printf("[Router] Successfully delivered message %s to node %s", msg.ID[:8], targetNode.ID[:8])
				} else if result.Status == "dropped" {
					log.Printf("[Router] Message %s dropped by node %s", msg.ID[:8], targetNode.ID[:8])
				} else {
					errors <- fmt.Errorf("node %s returned unexpected status: %s (%s)", targetNode.ID, result.Status, result.Error)
				}
			}(node)
		}

		// Wait for all goroutines to finish
		wg.Wait()
		close(errors)

		// Collect any errors
		var errs []error
		for err := range errors {
			errs = append(errs, err)
		}

		successes := successCount.Load()
		if successes == 0 && len(errs) > 0 {
			return fmt.Errorf("failed to deliver message to any nodes: %v", errs)
		}

		if len(errs) > 0 {
			log.Printf("[Router] Warning: Message %s delivered to %d/%d nodes with %d errors",
				msg.ID[:8], successes, len(sendNodes), len(errs))
		} else {
			log.Printf("[Router] Successfully delivered message %s to all %d nodes",
				msg.ID[:8], len(sendNodes))
		}
	} else {
		log.Printf("[Router] No remote nodes to send to for topic %s", msg.Topic)
	}

	return nil
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
