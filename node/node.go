package node

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"sprawl/node/balancer"
	"sprawl/node/consensus"
	"sprawl/node/dht"
	"sprawl/node/metrics"
	"sprawl/node/registry"
	"sprawl/node/router"
	"sprawl/store"

	"github.com/google/uuid"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
)

var startTime time.Time

func init() {
	startTime = time.Now()
}

// Node represents a node in the Sprawl cluster
type Node struct {
	ID            string
	Gossip        *GossipManager
	Store         *store.Store
	DHT           *dht.DHT
	Router        *router.Router
	Balancer      *balancer.LoadBalancer
	Metrics       *metrics.Metrics
	Consensus     *consensus.RaftNode
	Replication   *consensus.ReplicationManager
	BindAddress   string
	AdvertiseAddr string
	HTTPPort      int
	semaphore     chan struct{} // Limit concurrent requests
	server        interface{}   // Added to store the HTTP server
	raft          *raft.Raft
	registry      *registry.Registry
	fsm           *registry.RegistryFSM
	mu            sync.RWMutex
}

// NewNode creates a new node with the given bind address, advertise address, ports, and HTTP port
func NewNode(bindAddr string, advertiseAddr string, bindPort int, httpPort int) (*Node, error) {
	// Generate a unique node ID
	nodeIDBytes := uuid.New()
	nodeID := nodeIDBytes.String()

	// Validate HTTP port
	if httpPort <= 0 {
		return nil, fmt.Errorf("invalid HTTP port: %d", httpPort)
	}

	// Set up data store
	store := store.NewStore()

	// Initialize DHT with node ID
	dht := dht.NewDHT(nodeID)

	// Log node creation with HTTP port
	log.Printf("Creating new node with ID=%s, HTTPPort=%d", nodeID[:8], httpPort)

	// Initialize our own node in DHT with proper HTTP port
	dht.InitializeOwnNode(advertiseAddr, bindPort, httpPort)

	// Verify DHT has the correct HTTP port stored
	nodeInfo := dht.GetNodeInfo()
	log.Printf("After initialization, DHT has node info with HTTPPort=%d", nodeInfo.HTTPPort)

	// Double-check HTTP port in DHT matches expected
	if nodeInfo.HTTPPort != httpPort {
		log.Printf("WARNING: DHT node info HTTP port (%d) doesn't match expected (%d), fixing",
			nodeInfo.HTTPPort, httpPort)

		// Update the HTTP port in DHT
		nodeInfo.HTTPPort = httpPort
		dht.AddNode(nodeInfo)
	}

	// Initialize gossip for cluster membership
	gm, err := NewGossipManager(nodeID, bindAddr, bindPort, dht, httpPort)
	if err != nil {
		return nil, fmt.Errorf("failed to create gossip manager: %w", err)
	}

	// Initialize Raft for consensus
	raftBindPort := bindPort + 1
	raftBindAddr := fmt.Sprintf("%s:%d", bindAddr, raftBindPort)
	raftAdvertiseAddr := fmt.Sprintf("%s:%d", advertiseAddr, raftBindPort)

	// Set up Raft store
	logStore := raft.NewInmemStore()
	stableStore := raft.NewInmemStore()
	snapshotStore := raft.NewInmemSnapshotStore()

	// Set up registry
	reg := registry.NewRegistry(nodeID)
	fsm := registry.NewRegistryFSM(reg)

	// Configure Raft with appropriate timeouts for Kubernetes environment
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(nodeID)
	// More aggressive timeouts for Kubernetes:
	raftConfig.HeartbeatTimeout = 1000 * time.Millisecond
	raftConfig.ElectionTimeout = 1000 * time.Millisecond
	raftConfig.CommitTimeout = 50 * time.Millisecond
	raftConfig.LeaderLeaseTimeout = 500 * time.Millisecond
	// Logging
	logger := hclog.New(&hclog.LoggerOptions{
		Name:   "raft",
		Level:  hclog.LevelFromString("INFO"),
		Output: log.Writer(),
	})
	raftConfig.Logger = logger

	// Create Raft transport
	transport, err := raft.NewTCPTransport(raftBindAddr, nil, 3, 10*time.Second, log.Writer())
	if err != nil {
		return nil, fmt.Errorf("failed to create Raft transport: %w", err)
	}

	// Create Raft instance
	r, err := raft.NewRaft(raftConfig, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return nil, fmt.Errorf("failed to create Raft instance: %w", err)
	}

	// Bootstrap the cluster if this is the first node
	configuration := raft.Configuration{
		Servers: []raft.Server{
			{
				ID:      raft.ServerID(nodeID),
				Address: raft.ServerAddress(raftAdvertiseAddr),
			},
		},
	}
	r.BootstrapCluster(configuration)

	// Initialize Raft consensus for leader election
	raftNode := consensus.NewRaftNode(nodeID, []string{})               // Peers will be added when joining cluster
	replication := consensus.NewReplicationManager(nodeID, raftNode, 3) // Default replication factor of 3

	// Create router with replication manager
	router := router.NewRouter(nodeID, dht, store, replication)
	balancer := balancer.NewLoadBalancer(dht)
	metrics := metrics.NewMetrics()

	// Directly register node in DHT with the HTTP port to avoid type confusion
	log.Printf("Registering node in DHT with ID=%s, HTTPPort=%d",
		nodeID[:8], httpPort)

	// Register the node with an empty topic to make it discoverable
	dht.RegisterNode("", nodeID, httpPort)

	node := &Node{
		ID:            nodeID,
		Gossip:        gm,
		Store:         store,
		DHT:           dht,
		Router:        router,
		Balancer:      balancer,
		Metrics:       metrics,
		Consensus:     raftNode,
		Replication:   replication,
		BindAddress:   bindAddr,
		AdvertiseAddr: advertiseAddr,
		HTTPPort:      httpPort,
		semaphore:     make(chan struct{}, 100), // Limit to 100 concurrent requests
		raft:          r,
		registry:      reg,
		fsm:           fsm,
	}

	// Ensure node info is properly initialized in DHT before starting
	nodeInfo = dht.GetNodeInfo()
	if nodeInfo.HTTPPort != httpPort {
		log.Printf("Warning: DHT node info HTTP port mismatch. Expected: %d, Got: %d. Fixing...",
			httpPort, nodeInfo.HTTPPort)
		dht.InitializeOwnNode(advertiseAddr, bindPort, httpPort)
	}

	replication.SetCommitCallback(func(entry consensus.ReplicationEntry) {
		// When an entry is committed, store it locally
		// For now, just log the message - we'll implement proper storage later
		log.Printf("[Replication] Committed message to topic %s", entry.Topic)
	})

	return node, nil
}

// StartHTTP starts the HTTP server for publish/subscribe endpoints
func (n *Node) StartHTTP() {
	// Create the HTTP handler using our Handler function
	handler := n.Handler()

	// Log health check endpoints
	log.Printf("[Node %s] Registered health check endpoints at /health and /healthz", n.ID[:8])

	// Log additional endpoints
	log.Printf("[Node %s] Registered API endpoints for publish, subscribe, metrics, etc.", n.ID[:8])

	// Use 0.0.0.0 for the HTTP server to bind to all interfaces
	server := &http.Server{
		Addr:    fmt.Sprintf("0.0.0.0:%d", n.HTTPPort),
		Handler: handler,
		// Configure timeouts
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       120 * time.Second,
		ReadHeaderTimeout: 2 * time.Second,
		// Configure connection settings
		MaxHeaderBytes: 1 << 20, // 1MB
	}

	n.server = server

	log.Printf("[Node %s] Starting main HTTP server on 0.0.0.0:%d",
		n.ID[:8], n.HTTPPort)

	// Start the server and log errors
	if err := server.ListenAndServe(); err != nil {
		if err != http.ErrServerClosed {
			log.Printf("[Node %s] HTTP server error: %v", n.ID[:8], err)
		} else {
			log.Printf("[Node %s] HTTP server closed", n.ID[:8])
		}
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
		ID     string   `json:"id"`
		Topics []string `json:"topics"`
	}

	if err := json.NewDecoder(r.Body).Decode(&sub); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if len(sub.Topics) == 0 {
		http.Error(w, "At least one topic is required", http.StatusBadRequest)
		return
	}

	if sub.ID == "" {
		sub.ID = uuid.New().String()
	}

	// Create subscriber state
	state := &registry.SubscriberState{
		ID:       sub.ID,
		Topics:   sub.Topics,
		LastSeen: time.Now(),
		NodeID:   n.ID,
		IsActive: true,
	}

	// Register subscriber in the registry
	if err := n.registry.RegisterSubscriber(state); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Register this node in the DHT for each topic
	for _, topic := range sub.Topics {
		hash := n.DHT.HashTopic(topic)
		n.DHT.RegisterNode(topic, n.ID, n.HTTPPort)
		log.Printf("[Node %s] Registered in DHT for topic: %s (hash: %s)", n.ID[:8], topic, hash[:8])

		// Create subscriber function
		subscriber := func(msg store.Message) {
			log.Printf("[Node %s] Received message on topic %s: %s",
				n.ID[:8], msg.Topic, string(msg.Payload))
			n.Metrics.RecordMessage(false)
		}

		n.Store.Subscribe(topic, subscriber)
		log.Printf("[Node %s] Added subscriber function for topic: %s", n.ID[:8], topic)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  "subscribed",
		"id":      sub.ID,
		"topics":  sub.Topics,
		"node_id": n.ID,
	})
}

func (n *Node) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Get metrics from various components
	routerMetrics := n.Router.GetMetrics()
	systemMetrics := n.Metrics.GetSnapshot()

	// Create storage metrics directly
	storageMetrics := map[string]interface{}{
		"bytes_stored":    n.Store.GetMemoryUsage(),
		"messages_stored": n.Store.GetMessageCount(),
		"topics":          n.Store.GetTopics(),
		"storage_type":    "memory",
		"last_write_time": time.Now().Format(time.RFC3339Nano),
	}

	metrics := map[string]interface{}{
		"node_id": n.ID,
		"router":  routerMetrics,
		"system":  systemMetrics,
		"cluster": n.Gossip.GetMembers(),
		"storage": storageMetrics,
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

// handleHealth handles health check requests
func (n *Node) handleHealth(w http.ResponseWriter, r *http.Request) {
	// Log health check request for debugging
	log.Printf("[Node %s] Health check request from %s (path: %s)",
		n.ID[:8], r.RemoteAddr, r.URL.Path)

	// Simple health check that responds with 200
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	resp := map[string]string{
		"status": "ok",
		"nodeId": n.ID,
		"uptime": time.Since(startTime).String(),
	}

	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.Printf("[Node %s] Error encoding health response: %v", n.ID[:8], err)
	}

	log.Printf("[Node %s] Health check responded successfully", n.ID[:8])
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
func (n *Node) Shutdown() error {
	n.mu.Lock()
	defer n.mu.Unlock()

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

	if n.raft != nil {
		future := n.raft.Shutdown()
		if err := future.Error(); err != nil {
			return fmt.Errorf("error shutting down raft: %v", err)
		}
	}

	log.Printf("[Node %s] Gracefully shut down", n.ID[:8])
	return nil
}

// Handler returns the HTTP handler for the node
func (n *Node) Handler() http.Handler {
	log.Printf("[Node %s] Creating HTTP handler with all endpoints", n.ID[:8])
	mux := http.NewServeMux()

	// Health check endpoints
	log.Printf("[Node %s] Registering health endpoints at /health and /healthz", n.ID[:8])
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		// Log health check request for debugging
		log.Printf("[Node %s] Health check request from %s (path: %s)",
			n.ID[:8], r.RemoteAddr, r.URL.Path)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		resp := map[string]string{
			"status": "ok",
			"nodeId": n.ID,
			"uptime": time.Since(startTime).String(),
		}

		if err := json.NewEncoder(w).Encode(resp); err != nil {
			log.Printf("[Node %s] Error encoding health response: %v", n.ID[:8], err)
		}

		log.Printf("[Node %s] Health check responded successfully", n.ID[:8])
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		// Log health check request for debugging
		log.Printf("[Node %s] Health check request from %s (path: %s)",
			n.ID[:8], r.RemoteAddr, r.URL.Path)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		resp := map[string]string{
			"status": "ok",
			"nodeId": n.ID,
			"uptime": time.Since(startTime).String(),
		}

		if err := json.NewEncoder(w).Encode(resp); err != nil {
			log.Printf("[Node %s] Error encoding health response: %v", n.ID[:8], err)
		}

		log.Printf("[Node %s] Health check responded successfully", n.ID[:8])
	})

	// Regular API endpoints
	mux.HandleFunc("/publish", n.handlePublish)
	mux.HandleFunc("/subscribe", n.handleSubscribe)
	mux.HandleFunc("/metrics", n.handleMetrics)
	mux.HandleFunc("/status", n.handleStatus)

	// DHT and storage management endpoints
	mux.HandleFunc("/dht", n.handleDHT)
	mux.HandleFunc("/store", n.handleStore)
	mux.HandleFunc("/store/tiers", n.handleStorageTiers)
	mux.HandleFunc("/store/compact", n.handleStorageCompact)
	mux.HandleFunc("/topics", n.handleTopics)
	mux.HandleFunc("/topics/", n.handleTopic)

	// Consumer groups endpoints
	mux.HandleFunc("/consumer-groups", n.handleConsumerGroups)
	mux.HandleFunc("/consumer-groups/", n.handleConsumerGroup)

	// Offsets endpoints
	mux.HandleFunc("/offsets", n.handleOffsets)
	mux.HandleFunc("/offsets/", n.handleOffset)

	return mux
}

// handleConsumerGroups handles consumer group operations
func (n *Node) handleConsumerGroups(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		var req struct {
			GroupID string   `json:"group_id"`
			Topic   string   `json:"topic"`
			Members []string `json:"members"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			log.Printf("Error decoding request body: %v", err)
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		if req.GroupID == "" || req.Topic == "" || len(req.Members) == 0 {
			log.Printf("Missing required fields: group_id=%s, topic=%s, members=%v", req.GroupID, req.Topic, req.Members)
			http.Error(w, "Missing required fields", http.StatusBadRequest)
			return
		}

		group := &registry.ConsumerGroup{
			ID:         req.GroupID,
			Topics:     []string{req.Topic},
			Members:    req.Members,
			Generation: 1,
			Leader:     req.Members[0], // Set first member as leader
		}

		cmd := registry.Command{
			Op:    registry.OpUpdateConsumerGroup,
			Group: group,
		}

		data, err := json.Marshal(cmd)
		if err != nil {
			log.Printf("Error marshaling command: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// Apply through Raft FSM
		future := n.raft.Apply(data, 5*time.Second)
		if err := future.Error(); err != nil {
			log.Printf("Error applying Raft command: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// Wait for the FSM to apply the command
		resp := future.Response()
		if err, ok := resp.(error); ok && err != nil {
			log.Printf("FSM returned error: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// Verify the group was created
		createdGroup, ok := n.registry.GetConsumerGroup(req.GroupID)
		if !ok {
			log.Printf("Consumer group %s not found after creation", req.GroupID)
			http.Error(w, "Failed to create consumer group", http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusCreated)
		if err := json.NewEncoder(w).Encode(createdGroup); err != nil {
			log.Printf("Error encoding response: %v", err)
		}

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleConsumerGroup handles operations on a specific consumer group
func (n *Node) handleConsumerGroup(w http.ResponseWriter, r *http.Request) {
	groupID := r.URL.Path[len("/consumer-groups/"):]

	switch r.Method {
	case http.MethodGet:
		group, ok := n.registry.GetConsumerGroup(groupID)
		if !ok {
			http.Error(w, "Consumer group not found", http.StatusNotFound)
			return
		}
		json.NewEncoder(w).Encode(group)

	case http.MethodPut:
		var req struct {
			GroupID string   `json:"group_id"`
			Topic   string   `json:"topic"`
			Members []string `json:"members"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		if req.GroupID != groupID {
			http.Error(w, "Group ID mismatch", http.StatusBadRequest)
			return
		}

		// Get existing group to preserve generation
		existingGroup, ok := n.registry.GetConsumerGroup(groupID)
		if !ok {
			http.Error(w, "Consumer group not found", http.StatusNotFound)
			return
		}

		group := &registry.ConsumerGroup{
			ID:         req.GroupID,
			Topics:     []string{req.Topic},
			Members:    req.Members,
			Generation: existingGroup.Generation + 1,
			Leader:     req.Members[0], // Update leader to first member
		}

		cmd := registry.Command{
			Op:    registry.OpUpdateConsumerGroup,
			Group: group,
		}

		data, err := json.Marshal(cmd)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// Apply through Raft FSM
		future := n.raft.Apply(data, 5*time.Second)
		if err := future.Error(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		json.NewEncoder(w).Encode(group)

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleOffsets handles offset tracking operations
func (n *Node) handleOffsets(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		var req struct {
			Topic   string `json:"topic"`
			GroupID string `json:"group_id"`
			Offset  int64  `json:"offset"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}

		offset := &registry.TopicOffset{
			Topic:         req.Topic,
			GroupID:       req.GroupID,
			CurrentOffset: req.Offset,
		}

		if err := n.registry.UpdateOffset(offset); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(offset)

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleOffset handles operations on a specific offset
func (n *Node) handleOffset(w http.ResponseWriter, r *http.Request) {
	parts := splitPath(r.URL.Path[len("/offsets/"):])
	if len(parts) != 2 {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}

	topic, groupID := parts[0], parts[1]

	switch r.Method {
	case http.MethodGet:
		offset, ok := n.registry.GetOffset(topic)
		if !ok {
			http.Error(w, "Offset not found", http.StatusNotFound)
			return
		}

		if offset.GroupID != groupID {
			http.Error(w, "Offset not found for group", http.StatusNotFound)
			return
		}

		json.NewEncoder(w).Encode(offset)

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// Helper function to split path
func splitPath(path string) []string {
	var result []string
	current := ""
	for _, c := range path {
		if c == '/' {
			if current != "" {
				result = append(result, current)
				current = ""
			}
		} else {
			current += string(c)
		}
	}
	if current != "" {
		result = append(result, current)
	}
	return result
}

// SetRegistry sets the registry for the node
func (n *Node) SetRegistry(reg *registry.Registry) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.registry = reg
}

// SetFSM sets the FSM for the node
func (n *Node) SetFSM(fsm *registry.RegistryFSM) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.fsm = fsm
}

// handleDHT handles requests for DHT status
func (n *Node) handleDHT(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	// Get DHT information
	type topicEntry struct {
		Name  string   `json:"name"`
		Nodes []string `json:"nodes"`
	}

	type dhtResponse struct {
		NodeCount  int                 `json:"node_count"`
		TopicCount int                 `json:"topic_count"`
		Topics     map[string][]string `json:"topics"`
	}

	// Get the DHT topic map
	topicMap := n.DHT.GetTopicMap()

	response := dhtResponse{
		NodeCount:  len(n.Gossip.GetMembers()),
		TopicCount: len(topicMap),
		Topics:     topicMap,
	}

	// Set content type and encode response
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("[Node %s] Error encoding DHT response: %v", n.ID[:8], err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

// handleStore handles requests for storage statistics
func (n *Node) handleStore(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	// Get storage statistics from all tiers
	type tierStats struct {
		Enabled       bool      `json:"enabled"`
		CapacityBytes int64     `json:"capacity_bytes,omitempty"`
		UsedBytes     int64     `json:"used_bytes"`
		MessageCount  int       `json:"message_count"`
		Topics        []string  `json:"topics"`
		OldestMessage time.Time `json:"oldest_message,omitempty"`
		NewestMessage time.Time `json:"newest_message,omitempty"`
	}

	type storeResponse struct {
		Memory tierStats `json:"memory"`
		Disk   tierStats `json:"disk,omitempty"`
		Cloud  tierStats `json:"cloud,omitempty"`
	}

	// Create a basic response with memory tier info
	response := storeResponse{
		Memory: tierStats{
			Enabled:      true,
			UsedBytes:    n.Store.GetMemoryUsage(),
			MessageCount: n.Store.GetMessageCount(),
			Topics:       n.Store.GetTopics(),
		},
	}

	// Get information on disk and cloud tiers if available
	if diskStats := n.Store.GetDiskStats(); diskStats != nil {
		response.Disk = tierStats{
			Enabled:      true,
			UsedBytes:    diskStats.UsedBytes,
			MessageCount: diskStats.MessageCount,
			Topics:       diskStats.Topics,
		}
	}

	if cloudStats := n.Store.GetCloudStats(); cloudStats != nil {
		response.Cloud = tierStats{
			Enabled:      true,
			UsedBytes:    cloudStats.UsedBytes,
			MessageCount: cloudStats.MessageCount,
			Topics:       cloudStats.Topics,
		}
	}

	// Set content type and encode response
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("[Node %s] Error encoding storage response: %v", n.ID[:8], err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

// handleStorageTiers handles requests for storage tier configuration
func (n *Node) handleStorageTiers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	// Get storage tier configuration
	type tierPolicies struct {
		MemoryToDiskThresholdBytes int64 `json:"memory_to_disk_threshold_bytes"`
		MemoryToDiskAgeSeconds     int64 `json:"memory_to_disk_age_seconds"`
		DiskToCloudAgeSeconds      int64 `json:"disk_to_cloud_age_seconds"`
		DiskToCloudThresholdBytes  int64 `json:"disk_to_cloud_threshold_bytes"`
	}

	type tiersResponse struct {
		Tiers    []string     `json:"tiers"`
		Policies tierPolicies `json:"policies"`
	}

	// Get tier configuration from store
	config := n.Store.GetTierConfig()

	// Create a response with available tiers and policies
	response := tiersResponse{
		Tiers: []string{"memory"},
		Policies: tierPolicies{
			MemoryToDiskThresholdBytes: config.MemoryToDiskThresholdBytes,
			MemoryToDiskAgeSeconds:     config.MemoryToDiskAgeSeconds,
			DiskToCloudAgeSeconds:      config.DiskToCloudAgeSeconds,
			DiskToCloudThresholdBytes:  config.DiskToCloudThresholdBytes,
		},
	}

	// Add disk and cloud tiers if enabled
	if config.DiskEnabled {
		response.Tiers = append(response.Tiers, "disk")
	}

	if config.CloudEnabled {
		response.Tiers = append(response.Tiers, "cloud")
	}

	// Set content type and encode response
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("[Node %s] Error encoding tier config response: %v", n.ID[:8], err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

// handleStorageCompact handles requests to compact storage
func (n *Node) handleStorageCompact(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	// Start compaction in a goroutine
	go func() {
		if err := n.Store.Compact(); err != nil {
			log.Printf("[Node %s] Error compacting storage: %v", n.ID[:8], err)
		}
	}()

	// Create response with compaction status
	type compactResponse struct {
		Status              string    `json:"status"`
		Tiers               []string  `json:"tiers"`
		EstimatedCompletion time.Time `json:"estimated_completion"`
	}

	response := compactResponse{
		Status:              "compaction_started",
		Tiers:               []string{"disk"},
		EstimatedCompletion: time.Now().Add(5 * time.Minute),
	}

	// Set content type and encode response
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("[Node %s] Error encoding compact response: %v", n.ID[:8], err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

// handleTopics handles requests for topic listing
func (n *Node) handleTopics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	// Get all topics information
	type topicInfo struct {
		Name         string   `json:"name"`
		MessageCount int      `json:"message_count"`
		Subscribers  int      `json:"subscribers"`
		Nodes        []string `json:"nodes"`
	}

	type topicsResponse struct {
		Topics []topicInfo `json:"topics"`
	}

	// Get topics from DHT and store
	storeTopics := n.Store.GetTopics()
	dhtTopics := n.DHT.GetTopicMap()

	// Create a response with topics and their information
	response := topicsResponse{
		Topics: []topicInfo{},
	}

	// Add topics to response
	for _, topic := range storeTopics {
		info := topicInfo{
			Name:         topic,
			MessageCount: n.Store.GetMessageCountForTopic(topic),
			Subscribers:  n.Store.GetSubscriberCountForTopic(topic),
			Nodes:        []string{},
		}

		// Add nodes for this topic from DHT
		if nodes, exists := dhtTopics[topic]; exists {
			info.Nodes = nodes
		}

		response.Topics = append(response.Topics, info)
	}

	// Set content type and encode response
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("[Node %s] Error encoding topics response: %v", n.ID[:8], err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

// handleTopic handles requests for specific topic information
func (n *Node) handleTopic(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	// Extract topic name from URL path
	path := strings.TrimPrefix(r.URL.Path, "/topics/")
	if path == "" {
		http.Redirect(w, r, "/topics", http.StatusSeeOther)
		return
	}

	topic := path

	// Check if topic exists
	storeTopics := n.Store.GetTopics()
	topicExists := false

	for _, t := range storeTopics {
		if t == topic {
			topicExists = true
			break
		}
	}

	if !topicExists {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	// Get detailed information for this topic
	type topicResponse struct {
		Name          string    `json:"name"`
		MessageCount  int       `json:"message_count"`
		Subscribers   int       `json:"subscribers"`
		Nodes         []string  `json:"nodes"`
		OldestMessage time.Time `json:"oldest_message,omitempty"`
		NewestMessage time.Time `json:"newest_message,omitempty"`
		StorageTier   string    `json:"storage_tier"`
	}

	// Create response with topic information
	response := topicResponse{
		Name:         topic,
		MessageCount: n.Store.GetMessageCountForTopic(topic),
		Subscribers:  n.Store.GetSubscriberCountForTopic(topic),
		Nodes:        []string{},
		StorageTier:  n.Store.GetStorageTierForTopic(topic),
	}

	// Add nodes for this topic from DHT
	dhtTopics := n.DHT.GetTopicMap()
	if nodes, exists := dhtTopics[topic]; exists {
		response.Nodes = nodes
	}

	// Get message timestamps if available
	if timestamps := n.Store.GetTopicTimestamps(topic); timestamps != nil {
		response.OldestMessage = timestamps.Oldest
		response.NewestMessage = timestamps.Newest
	}

	// Set content type and encode response
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("[Node %s] Error encoding topic response: %v", n.ID[:8], err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}
