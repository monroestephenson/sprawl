package node

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"sprawl/node/balancer"
	"sprawl/node/consensus"
	"sprawl/node/dht"
	"sprawl/node/metrics"
	"sprawl/node/router"
	"sprawl/store"

	"github.com/google/uuid"
)

var startTime time.Time

func init() {
	startTime = time.Now()
}

type Node struct {
	ID          string
	Gossip      *GossipManager
	Store       *store.Store
	DHT         *dht.DHT
	Router      *router.Router
	Balancer    *balancer.LoadBalancer
	Metrics     *metrics.Metrics
	Consensus   *consensus.RaftNode
	Replication *consensus.ReplicationManager
	BindAddress string
	HTTPAddress string
	HTTPPort    int
	semaphore   chan struct{} // Limit concurrent requests
	server      interface{}   // Added to store the HTTP server
}

// NewNode initializes a Node with a gossip manager and an in-memory store
func NewNode(bindAddr string, bindPort int, httpAddr string, httpPort int) (*Node, error) {
	nodeID := uuid.New().String()

	dht := dht.NewDHT(nodeID)
	store := store.NewStore()

	gm, err := NewGossipManager(nodeID, bindAddr, bindPort, dht)
	if err != nil {
		return nil, err
	}

	// Initialize Raft consensus
	raftNode := consensus.NewRaftNode(nodeID, []string{})               // Peers will be added when joining cluster
	replication := consensus.NewReplicationManager(nodeID, raftNode, 3) // Default replication factor of 3

	// Create router with replication manager
	router := router.NewRouter(nodeID, dht, store, replication)
	balancer := balancer.NewLoadBalancer(dht)
	metrics := metrics.NewMetrics()

	node := &Node{
		ID:          nodeID,
		Gossip:      gm,
		Store:       store,
		DHT:         dht,
		Router:      router,
		Balancer:    balancer,
		Metrics:     metrics,
		Consensus:   raftNode,
		Replication: replication,
		BindAddress: bindAddr,
		HTTPAddress: httpAddr,
		HTTPPort:    httpPort,
		semaphore:   make(chan struct{}, 100), // Limit to 100 concurrent requests
	}

	// Set up replication commit callback
	replication.SetCommitCallback(func(entry consensus.ReplicationEntry) {
		// When an entry is committed, store it locally
		store.Publish(entry.Topic, entry.Payload)
	})

	return node, nil
}

// StartHTTP starts the HTTP server for publish/subscribe endpoints
func (n *Node) StartHTTP() {
	mux := http.NewServeMux()
	mux.HandleFunc("/publish", n.handlePublish)
	mux.HandleFunc("/subscribe", n.handleSubscribe)
	mux.HandleFunc("/metrics", n.handleMetrics)
	mux.HandleFunc("/status", n.handleStatus)

	server := &http.Server{
		Addr:    fmt.Sprintf("%s:%d", n.HTTPAddress, n.HTTPPort),
		Handler: mux,
		// Configure timeouts
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       120 * time.Second,
		ReadHeaderTimeout: 2 * time.Second,
		// Configure connection settings
		MaxHeaderBytes: 1 << 20, // 1MB
	}

	n.server = server

	log.Printf("Starting HTTP server on %s:%d", n.HTTPAddress, n.HTTPPort)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("HTTP server failed: %v", err)
	}
}

func (n *Node) handlePublish(w http.ResponseWriter, r *http.Request) {
	// Apply back pressure by using a semaphore
	select {
	case n.semaphore <- struct{}{}:
		defer func() { <-n.semaphore }()
	default:
		// If we can't acquire the semaphore immediately, return 503
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]string{
			"error": "Server is too busy",
		})
		return
	}

	// Create a context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	if r.Method != http.MethodPost {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMethodNotAllowed)
		json.NewEncoder(w).Encode(map[string]string{
			"error": "Method not allowed",
		})
		return
	}

	var msg struct {
		Topic   string `json:"topic"`
		Payload string `json:"payload"`
		ID      string `json:"id"`
		TTL     int    `json:"ttl"`
	}

	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{
			"error": "Invalid request body",
		})
		return
	}

	// Generate message ID if not provided (new message)
	if msg.ID == "" {
		msg.ID = uuid.New().String()
		msg.TTL = 3 // Default TTL for new messages
	}

	// Check TTL before processing
	if msg.TTL <= 0 {
		log.Printf("[Node %s] Dropping message %s due to expired TTL", n.ID[:8], msg.ID)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{
			"status": "dropped",
			"id":     msg.ID,
		})
		return
	}

	log.Printf("[Node %s] Publishing message %s to topic: %s (TTL: %d)",
		n.ID[:8], msg.ID, msg.Topic, msg.TTL)

	routerMsg := router.Message{
		ID:      msg.ID,
		Topic:   msg.Topic,
		Payload: []byte(msg.Payload),
		TTL:     msg.TTL,
	}

	// Use the context with timeout for routing
	if err := n.Router.RouteMessage(ctx, routerMsg); err != nil {
		log.Printf("[Node %s] Failed to route message: %v", n.ID[:8], err)

		// Check if the error was due to context timeout
		if ctx.Err() == context.DeadlineExceeded {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusGatewayTimeout)
			json.NewEncoder(w).Encode(map[string]string{
				"error": "Request timed out",
			})
			return
		}

		// Only return error if it's not a routing failure to other nodes
		if !strings.Contains(err.Error(), "failed to deliver message") {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]string{
				"error": err.Error(),
			})
			return
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"status": "published",
		"id":     msg.ID,
	})
}

func (n *Node) handleSubscribe(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var sub struct {
		Topic string `json:"topic"`
	}

	if err := json.NewDecoder(r.Body).Decode(&sub); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	subID := uuid.New().String()
	log.Printf("[Node %s] Processing subscription request for topic: %s", n.ID[:8], sub.Topic)

	// Register this node in the DHT for the topic with HTTP port
	hash := n.DHT.HashTopic(sub.Topic)
	n.DHT.RegisterNode(sub.Topic, n.ID, n.HTTPPort)
	log.Printf("[Node %s] Registered in DHT for topic: %s (hash: %s)", n.ID[:8], sub.Topic, hash[:8])

	// Create subscriber function with better logging
	subscriber := func(msg store.Message) {
		log.Printf("[Node %s] Received message on topic %s: %s",
			n.ID[:8], msg.Topic, string(msg.Payload))
		n.Metrics.RecordMessage(false) // Record received message
	}

	n.Store.Subscribe(sub.Topic, subscriber)
	log.Printf("[Node %s] Added subscriber function for topic: %s", n.ID[:8], sub.Topic)

	// Verify registration
	nodes := n.DHT.GetNodesForTopic(sub.Topic)
	found := false
	for _, node := range nodes {
		if node.ID == n.ID {
			found = true
			break
		}
	}

	if !found {
		log.Printf("[Node %s] Warning: Node not found in DHT after registration for topic: %s", n.ID[:8], sub.Topic)
	} else {
		log.Printf("[Node %s] Successfully verified DHT registration for topic: %s", n.ID[:8], sub.Topic)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"status": "subscribed",
		"id":     subID,
		"topic":  sub.Topic,
	})
}

func (n *Node) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	storeMetrics := n.Store.GetMetrics()
	routerMetrics := n.Router.GetMetrics()
	systemMetrics := n.Metrics.GetSnapshot()

	metrics := map[string]interface{}{
		"node_id": n.ID,
		"router":  routerMetrics,
		"system":  systemMetrics,
		"cluster": n.Gossip.GetMembers(),
		"storage": map[string]interface{}{
			"messages_stored": storeMetrics.MessagesStored,
			"bytes_stored":    storeMetrics.BytesStored,
			"topics":          storeMetrics.Topics,
			"last_write_time": storeMetrics.LastWriteTime,
			"storage_type":    storeMetrics.StorageType,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(metrics)
}

func (n *Node) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	status := map[string]interface{}{
		"node_id":         n.ID,
		"address":         n.BindAddress,
		"http_port":       n.HTTPPort,
		"cluster_members": n.Gossip.GetMembers(),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

// JoinCluster attempts to join an existing cluster
func (n *Node) JoinCluster(seeds []string) error {
	// First join the gossip cluster
	if err := n.Gossip.JoinCluster(seeds); err != nil {
		return err
	}

	// Wait for gossip to stabilize
	time.Sleep(2 * time.Second)

	// Get the current cluster members from gossip
	members := n.Gossip.GetMembers()
	var peers []string
	for _, member := range members {
		// Extract just the node ID from the member string
		// Format is "nodeID (address:port)"
		if id := strings.Split(member, " ")[0]; id != n.ID {
			peers = append(peers, id)
		}
	}

	// Update Raft peers
	n.Consensus.UpdatePeers(peers)

	// Wait for leader election to complete
	electionTimeout := time.NewTimer(10 * time.Second)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	defer electionTimeout.Stop()

	for {
		select {
		case <-electionTimeout.C:
			return fmt.Errorf("timeout waiting for leader election")
		case <-ticker.C:
			if leader := n.Consensus.GetLeader(); leader != "" {
				log.Printf("[Node %s] Cluster joined successfully, leader is %s", n.ID[:8], leader)
				return nil
			}
		}
	}
}

// Shutdown gracefully shuts down the node
func (n *Node) Shutdown() {
	// Stop the HTTP server
	if server, ok := n.server.(*http.Server); ok {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			log.Printf("Error shutting down HTTP server: %v", err)
		}
	}

	// Stop consensus and replication
	n.Consensus.Stop()
	n.Replication.Stop()

	// Stop metrics collection
	n.Metrics.Stop()

	// Stop gossip manager
	n.Gossip.Shutdown()

	// Close semaphore
	close(n.semaphore)

	log.Printf("[Node %s] Gracefully shut down", n.ID[:8])
}
