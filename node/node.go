package node

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"sprawl/ai" // Aliased import for AI metrics
	"sprawl/ai/prediction"
	"sprawl/node/balancer"
	"sprawl/node/cluster"
	"sprawl/node/consensus"
	"sprawl/node/dht"
	"sprawl/node/metrics"
	"sprawl/node/registry"
	"sprawl/node/router"
	"sprawl/store"

	"github.com/google/uuid"
)

var startTime time.Time

func init() {
	startTime = time.Now()
}

// AIEngine defines the interface for AI capabilities
type AIEngine interface {
	// Start begins collecting and analyzing metrics
	Start()

	// Stop halts all AI operations
	Stop()

	// GetStatus returns the current status of the AI engine
	GetStatus() map[string]interface{}

	// GetPrediction returns a prediction for the specified resource
	GetPrediction(resource string) (float64, float64)

	// GetSimpleAnomalies returns a simple list of detected anomalies
	GetSimpleAnomalies() []map[string]interface{}

	// RecordMetric records a single metric with the given attributes
	RecordMetric(metricKind ai.MetricKind, entityID string, value float64, labels map[string]string)

	// GetThresholds returns the current threshold configuration
	GetThresholds() ai.ThresholdConfig

	// SetThresholds updates the threshold configuration
	SetThresholds(config ai.ThresholdConfig) error

	// TriggerConfigReload manually triggers a configuration reload
	TriggerConfigReload()
}

// Metrics for the node
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
	AI            AIEngine
	HTTPPort      int
	BindAddress   string
	AdvertiseAddr string
	semaphore     chan struct{}
	server        interface{}
	stopCh        chan struct{}
	registry      *registry.Registry
	fsm           *registry.RegistryFSM
	options       *Options
	cluster       *cluster.ClusterManager
}

// NewNode creates a new node with the given options
func NewNode(opts *Options) (*Node, error) {
	// Generate a unique node ID
	nodeIDBytes := uuid.New()
	nodeID := nodeIDBytes.String()

	// Validate HTTP port for production use - allow 0 for tests
	if opts.HTTPPort < 0 {
		return nil, fmt.Errorf("invalid HTTP port: %d", opts.HTTPPort)
	}

	// Set up data store
	store := store.NewStore()

	// Initialize DHT with node ID
	dht := dht.NewDHT(nodeID)

	// Log node creation with HTTP port
	log.Printf("Creating new node with ID=%s, HTTPPort=%d", nodeID[:8], opts.HTTPPort)

	// Initialize our own node in DHT with proper HTTP port
	dht.InitializeOwnNode(opts.AdvertiseAddr, opts.BindPort, opts.HTTPPort)

	// Verify DHT has the correct HTTP port stored
	nodeInfo := dht.GetNodeInfo()
	log.Printf("After initialization, DHT has node info with HTTPPort=%d", nodeInfo.HTTPPort)

	// Double-check HTTP port in DHT matches expected
	if nodeInfo.HTTPPort != opts.HTTPPort {
		log.Printf("WARNING: DHT node info HTTP port (%d) doesn't match expected (%d), fixing",
			nodeInfo.HTTPPort, opts.HTTPPort)

		// Update the HTTP port in DHT
		nodeInfo.HTTPPort = opts.HTTPPort
		dht.AddNode(nodeInfo)
	}

	// Initialize gossip for cluster membership
	gm, err := NewGossipManager(nodeID, opts.BindAddress, opts.BindPort, dht, opts.HTTPPort)
	if err != nil {
		return nil, fmt.Errorf("failed to create gossip manager: %w", err)
	}

	// Initialize Raft consensus for leader election
	raftNode := consensus.NewRaftNode(nodeID, []string{})               // Peers will be added when joining cluster
	replication := consensus.NewReplicationManager(nodeID, raftNode, 3) // Default replication factor of 3

	// Create router with replication manager
	router := router.NewRouter(nodeID, dht, store, replication)
	balancer := balancer.NewLoadBalancer(dht)
	metrics := metrics.NewMetrics()

	// Create the AI Engine with default options and store
	aiOptions := ai.DefaultEngineOptions()

	// Check for config file path in environment
	aiConfigFile := os.Getenv("SPRAWL_AI_CONFIG_FILE")
	if aiConfigFile != "" {
		aiOptions.ConfigFile = aiConfigFile
		aiOptions.EnableAutoReload = true
		log.Printf("[Node %s] Using AI config file: %s with auto-reload enabled", nodeID[:8], aiConfigFile)
	} else {
		// Load thresholds from environment
		thresholds := ai.LoadThresholdsFromEnv()
		aiOptions.ThresholdConfig = &thresholds
		log.Printf("[Node %s] Using AI thresholds from environment variables", nodeID[:8])
	}

	// Configure the metrics collector
	metricsCollector := ai.NewDefaultMetricsCollector(store)

	// Set the metrics collector in AI options
	aiOptions.MetricsCollector = metricsCollector

	// Create the AI engine with the configured options
	aiEngine := ai.NewEngine(aiOptions, store)

	// Create registry for subscribers
	reg := registry.NewRegistry(nodeID)
	regFSM := registry.NewRegistryFSM(reg)

	// Create the cluster manager
	clusterMgr := cluster.NewClusterManager(nodeID, dht)

	// Create node with all components
	n := &Node{
		ID:            nodeID,
		Gossip:        gm,
		Store:         store,
		DHT:           dht,
		Router:        router,
		Balancer:      balancer,
		Metrics:       metrics,
		Consensus:     raftNode,
		Replication:   replication,
		AI:            aiEngine,
		BindAddress:   opts.BindAddress,
		AdvertiseAddr: opts.AdvertiseAddr,
		HTTPPort:      opts.HTTPPort,
		semaphore:     make(chan struct{}, opts.MaxConcurrentRequests),
		stopCh:        make(chan struct{}),
		registry:      reg,
		fsm:           regFSM,
		options:       opts,
		cluster:       clusterMgr,
	}

	// Debug logging for semaphore initialization
	log.Printf("[Node %s] Created new node with semaphore capacity %d", nodeID[:8], opts.MaxConcurrentRequests)
	log.Printf("[Node %s] Actual semaphore channel capacity: %d", nodeID[:8], cap(n.semaphore))

	return n, nil
}

// Start initializes the node and begins serving requests
func (n *Node) Start() error {
	// Create a cluster adapter and set it in the store
	clusterAdapter := cluster.NewClusterProviderAdapter(n.cluster)
	storeAdapter := &storeClusterAdapter{adapter: clusterAdapter}
	n.Store.SetClusterProvider(storeAdapter)

	// Start the AI Engine if not already started
	n.AI.Start()

	// Start feeding metrics to the AI engine in a background goroutine
	go n.feedMetricsToAI()

	// Start HTTP server
	n.StartHTTP()

	// Start gossip manager by joining the cluster
	if err := n.Gossip.JoinCluster([]string{}); err != nil {
		return fmt.Errorf("failed to join cluster: %w", err)
	}

	log.Printf("[Node %s] All components started successfully", n.ID[:8])
	return nil
}

// Stop gracefully shuts down the node
func (n *Node) Stop() error {
	// Send signal to stop all background goroutines
	close(n.stopCh)

	// Stop the AI Engine
	n.AI.Stop()

	// Stop HTTP server
	if server, ok := n.server.(*http.Server); ok {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return server.Shutdown(ctx)
	}

	return nil
}

// HandleAIStatus handles requests to get AI status
func (n *Node) handleAIStatus(w http.ResponseWriter, r *http.Request) {
	status := n.AI.GetStatus()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(status); err != nil {
		log.Printf("[Node %s] Error encoding AI status: %v", truncateID(n.ID), err)
	}
}

// HandleAIPredictions handles requests for AI predictions
func (n *Node) handleAIPredictions(w http.ResponseWriter, r *http.Request) {
	resource := r.URL.Query().Get("resource")
	if resource == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		if err := json.NewEncoder(w).Encode(map[string]interface{}{
			"success": false,
			"error":   "Missing resource parameter",
		}); err != nil {
			log.Printf("[Node %s] Error encoding error response: %v", truncateID(n.ID), err)
		}
		return
	}

	// Get prediction from AI Engine
	value, confidence := n.AI.GetPrediction(resource)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(map[string]interface{}{
		"success":    true,
		"resource":   resource,
		"prediction": value,
		"confidence": confidence,
	}); err != nil {
		log.Printf("[Node %s] Error encoding prediction response: %v", truncateID(n.ID), err)
	}
}

// HandleAIAnomalies handles requests to get AI detected anomalies
func (n *Node) handleAIAnomalies(w http.ResponseWriter, r *http.Request) {
	anomalies := n.AI.GetSimpleAnomalies()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(map[string]interface{}{
		"anomalies": anomalies,
	}); err != nil {
		log.Printf("[Node %s] Error encoding anomalies response: %v", truncateID(n.ID), err)
	}
}

// handleGetStore handles requests to get store information
func (n *Node) handleGetStore(w http.ResponseWriter, r *http.Request) {
	// Ensure request is GET
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Get memory store information
	memoryInfo := map[string]interface{}{
		"enabled":    true,
		"messages":   n.Store.GetMessageCount(),
		"bytes_used": n.Store.GetMemoryUsage(),
		"topics":     n.Store.GetTopics(),
	}

	// Get disk and cloud stats
	diskStats := n.Store.GetDiskStats()
	cloudStats := n.Store.GetCloudStats()

	// Create complete store info response
	storeInfo := map[string]interface{}{
		"memory":       memoryInfo,
		"disk":         diskStats,
		"cloud":        cloudStats,
		"storage_type": n.Store.GetStorageType(),
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(storeInfo); err != nil {
		log.Printf("[Node %s] Error encoding store info: %v", truncateID(n.ID), err)
	}
}

// StartHTTP starts the HTTP server for publish/subscribe endpoints
func (n *Node) StartHTTP() {
	// Create a new handler for the server
	mux := n.Handler()

	// Create the server with the handler
	server := &http.Server{
		Addr:    fmt.Sprintf("0.0.0.0:%d", n.HTTPPort),
		Handler: mux,
	}
	n.server = server

	// If auto port assignment is enabled, try finding an available port
	if n.options.AutoPortAssign {
		port := n.HTTPPort
		maxAttempts := n.options.PortRangeEnd - n.options.PortRangeStart

		for attempt := 0; attempt < maxAttempts; attempt++ {
			listener, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", port))
			if err == nil {
				// Port is available, close the listener and use it
				listener.Close()
				n.HTTPPort = port
				server.Addr = fmt.Sprintf("0.0.0.0:%d", port)
				log.Printf("[Node %s] Using HTTP port: %d", n.ID[:8], port)
				break
			}

			log.Printf("[Node %s] Port %d is in use, trying next port", n.ID[:8], port)
			port++

			// If we've reached the end of our range, wrap around
			if port > n.options.PortRangeEnd {
				port = n.options.PortRangeStart
			}

			// If we've tried all ports in range, give up
			if port == n.HTTPPort {
				log.Printf("[Node %s] Failed to find available port in range %d-%d",
					n.ID[:8], n.options.PortRangeStart, n.options.PortRangeEnd)
				break
			}
		}
	}

	// Update the DHT with our final HTTP port
	// This is critical for cross-node communication
	log.Printf("[Node %s] Updating DHT with HTTP port %d", n.ID[:8], n.HTTPPort)
	// Ensure DHT has the correct HTTP port
	n.DHT.InitializeOwnNode(n.options.AdvertiseAddr, n.options.BindPort, n.HTTPPort)

	// Create a channel to signal when the server is ready
	ready := make(chan struct{})
	go func() {
		log.Printf("[Node %s] Starting HTTP server on %s", n.ID[:8], server.Addr)
		close(ready) // Signal that we're about to start the server
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("[Node %s] Error starting HTTP server: %v", n.ID[:8], err)
		}
	}()

	// Wait for server to be ready
	<-ready
	log.Printf("[Node %s] HTTP server is ready on %s", n.ID[:8], server.Addr)
}

func (n *Node) handlePublish(w http.ResponseWriter, r *http.Request) {
	log.Printf("[Node %s] Publish request received from %s, method: %s, content-type: %s, content-length: %d",
		n.ID[:8], r.RemoteAddr, r.Method, r.Header.Get("Content-Type"), r.ContentLength)

	// Log all headers for debugging
	log.Printf("[Node %s] Publish request headers: %+v", n.ID[:8], r.Header)

	// Create a unique request ID for tracking this request through logs
	requestID := uuid.New().String()[:8]

	log.Printf("[Node %s] Publish request %s: processing", n.ID[:8], requestID)

	// Acquire semaphore to limit concurrent requests
	acquired := false
	timeout := 50 * time.Millisecond
	maxRetries := 5
	retryCount := 0

	for !acquired {
		select {
		case n.semaphore <- struct{}{}:
			acquired = true
			defer func() {
				<-n.semaphore
				log.Printf("[Node %s] Publish request %s: semaphore released, new capacity: %d/%d",
					n.ID[:8], requestID, len(n.semaphore), cap(n.semaphore))
			}()
			log.Printf("[Node %s] Publish request %s: semaphore acquired successfully, new capacity: %d/%d",
				n.ID[:8], requestID, len(n.semaphore), cap(n.semaphore))

		case <-time.After(timeout):
			if retryCount >= maxRetries {
				// If we've exhausted retries, return 503
				log.Printf("[Node %s] Publish request %s: failed to acquire semaphore after %d retries, returning 503",
					n.ID[:8], requestID, retryCount)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusServiceUnavailable)
				if err := json.NewEncoder(w).Encode(map[string]string{
					"error":       "Server is too busy, please retry later",
					"retry_after": "1",
				}); err != nil {
					log.Printf("[Node %s] Error encoding service unavailable response: %v", truncateID(n.ID), err)
				}
				return
			}
			// Increase timeout with exponential backoff
			timeout = timeout * 2
			retryCount++
			log.Printf("[Node %s] Publish request %s: failed to acquire semaphore, retry %d/%d with timeout %v, capacity: %d/%d",
				n.ID[:8], requestID, retryCount, maxRetries, timeout, len(n.semaphore), cap(n.semaphore))
			continue
		}
		if acquired {
			break
		}
	}

	// Create a context with timeout
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()

	if r.Method != http.MethodPost {
		log.Printf("[Node %s] Publish request %s: method not allowed: %s", n.ID[:8], requestID, r.Method)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMethodNotAllowed)
		if err := json.NewEncoder(w).Encode(map[string]string{
			"error": "Method not allowed",
		}); err != nil {
			log.Printf("[Node %s] Error encoding method not allowed response: %v", truncateID(n.ID), err)
		}
		return
	}

	var msg struct {
		Topic   string `json:"topic"`
		Payload string `json:"payload"`
		ID      string `json:"id"`
		TTL     int    `json:"ttl"`
	}

	// Read and log the request body
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("[Node %s] Publish request %s: error reading body: %v", n.ID[:8], requestID, err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		if err := json.NewEncoder(w).Encode(map[string]string{
			"error": "Error reading request body",
		}); err != nil {
			log.Printf("[Node %s] Error encoding bad request response: %v", truncateID(n.ID), err)
		}
		return
	}

	// Log the request body for debugging
	log.Printf("[Node %s] Publish request %s: body: %s", n.ID[:8], requestID, string(bodyBytes))

	// Create a new reader from the body bytes
	r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))

	if err := json.Unmarshal(bodyBytes, &msg); err != nil {
		log.Printf("[Node %s] Publish request %s: invalid JSON: %v", n.ID[:8], requestID, err)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		if err := json.NewEncoder(w).Encode(map[string]string{
			"error": "Invalid request body",
		}); err != nil {
			log.Printf("[Node %s] Error encoding invalid JSON response: %v", truncateID(n.ID), err)
		}
		return
	}

	// Check if required fields are present
	if msg.Topic == "" {
		log.Printf("[Node %s] Publish request %s: missing topic", n.ID[:8], requestID)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		if err := json.NewEncoder(w).Encode(map[string]string{
			"error": "Missing required field: topic",
		}); err != nil {
			log.Printf("[Node %s] Error encoding missing field response: %v", truncateID(n.ID), err)
		}
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
		if err := json.NewEncoder(w).Encode(map[string]string{
			"status": "dropped",
			"id":     msg.ID,
		}); err != nil {
			log.Printf("[Node %s] Error encoding dropped message response: %v", truncateID(n.ID), err)
		}
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

	// Register this node for the topic in the DHT
	// This ensures that the topic has at least one node assigned to it
	log.Printf("[Node %s] Registering this node for topic: %s", n.ID[:8], msg.Topic)
	n.DHT.RegisterNode(msg.Topic, n.ID, n.options.HTTPPort)

	// Check for forwarded message header
	forwardedBy := r.Header.Get("X-Forwarded-By")
	if forwardedBy != "" {
		log.Printf("[Node %s] Received message %s forwarded by node %s",
			n.ID[:8], msg.ID, forwardedBy)
	}

	// Use the context with timeout for routing
	if err := n.Router.RouteMessage(ctx, routerMsg); err != nil {
		log.Printf("[Node %s] Failed to route message: %v", n.ID[:8], err)

		// Check if the error was due to context timeout
		if ctx.Err() == context.DeadlineExceeded {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusGatewayTimeout)
			if err := json.NewEncoder(w).Encode(map[string]string{
				"error": "Request timed out",
			}); err != nil {
				log.Printf("[Node %s] Error encoding timeout response: %v", truncateID(n.ID), err)
			}
			return
		}

		// Only return error if it's not a routing failure to other nodes
		if !strings.Contains(err.Error(), "failed to deliver message") {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			if err := json.NewEncoder(w).Encode(map[string]string{
				"error": err.Error(),
			}); err != nil {
				log.Printf("[Node %s] Error encoding error response: %v", truncateID(n.ID), err)
			}
			return
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(map[string]string{
		"status": "published",
		"id":     msg.ID,
	}); err != nil {
		log.Printf("[Node %s] Error encoding published response: %v", truncateID(n.ID), err)
	}
}

func (n *Node) handleSubscribe(w http.ResponseWriter, r *http.Request) {
	log.Printf("[Node %s] Received subscribe request from %s", n.ID[:8], r.RemoteAddr)

	// Add panic recovery to prevent server crashes
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[Node %s] PANIC in handleSubscribe: %v", n.ID[:8], r)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
		}
	}()

	// Log headers
	log.Printf("[Node %s] Request headers: %+v", n.ID[:8], r.Header)

	// Log content length
	log.Printf("[Node %s] Content length: %d", n.ID[:8], r.ContentLength)

	if r.Method != http.MethodPost {
		log.Printf("[Node %s] Method not allowed: %s", n.ID[:8], r.Method)
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var sub struct {
		ID     string   `json:"id"`
		Topics []string `json:"topics"`
	}

	// Read request body into buffer for logging
	bodyBytes := make([]byte, r.ContentLength)
	_, err := r.Body.Read(bodyBytes)
	if err != nil {
		log.Printf("[Node %s] Error reading body: %v", n.ID[:8], err)
	}
	log.Printf("[Node %s] Request body: %s", n.ID[:8], string(bodyBytes))

	// Parse the request body
	if err := json.Unmarshal(bodyBytes, &sub); err != nil {
		log.Printf("[Node %s] Invalid request body: %v - Body: %s", n.ID[:8], err, string(bodyBytes))
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	log.Printf("[Node %s] Decoded subscribe request: ID=%s, Topics=%v", n.ID[:8], sub.ID, sub.Topics)

	// Check if registry is nil
	if n.registry == nil {
		log.Printf("[Node %s] CRITICAL ERROR: Registry is nil!", n.ID[:8])
		http.Error(w, "Server configuration error", http.StatusInternalServerError)
		return
	}

	if len(sub.Topics) == 0 {
		log.Printf("[Node %s] No topics provided", n.ID[:8])
		http.Error(w, "At least one topic is required", http.StatusBadRequest)
		return
	}

	if sub.ID == "" {
		sub.ID = uuid.New().String()
		log.Printf("[Node %s] Generated new subscriber ID: %s", n.ID[:8], sub.ID)
	}

	// Register this node for each topic in the DHT
	for _, topic := range sub.Topics {
		log.Printf("[Node %s] Registering this node for topic: %s (via subscribe)", n.ID[:8], topic)
		n.DHT.RegisterNode(topic, n.ID, n.options.HTTPPort)
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
		log.Printf("[Node %s] Failed to register subscriber: %v", n.ID[:8], err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Printf("[Node %s] Successfully registered subscriber: %s for topics %v", n.ID[:8], sub.ID, sub.Topics)

	// Register this node in the DHT for each topic
	for _, topic := range sub.Topics {
		hash := n.DHT.HashTopic(topic)
		n.DHT.RegisterNode(topic, n.ID, n.options.HTTPPort)
		log.Printf("[Node %s] Registered in DHT for topic: %s (hash: %s)", n.ID[:8], topic, hash[:8])

		// Create subscriber function
		subscriber := func(msg store.Message) {
			log.Printf("[Node %s] Received message on topic %s: %s",
				n.ID[:8], topic, string(msg.Payload))
		}

		// Subscribe to messages in the store
		n.Store.Subscribe(topic, subscriber)
	}

	// Send successful response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	response := map[string]string{
		"status": "subscribed",
		"id":     sub.ID,
	}

	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("[Node %s] Failed to encode subscribe response: %v", n.ID[:8], err)
		// At this point we've already set the status code, so we can't change it
		// but we can log the error
	} else {
		log.Printf("[Node %s] Successfully sent subscribe response to %s", n.ID[:8], r.RemoteAddr)
	}
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
		"storage_type":    n.Store.GetStorageType(),
		"last_write_time": time.Now().Format(time.RFC3339Nano),
	}

	metrics := map[string]interface{}{
		"node_id": n.ID,
		"router":  routerMetrics,
		"system":  systemMetrics,
		"cluster": []string{}, // Default empty cluster
		"storage": storageMetrics,
	}

	// Add cluster members if Gossip is available
	if n.Gossip != nil {
		metrics["cluster"] = n.Gossip.GetMembers()
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(metrics); err != nil {
		log.Printf("[Node %s] Error encoding metrics response: %v", truncateID(n.ID), err)
	}
}

func (n *Node) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Get leader information from replication manager if available
	var leader string
	if n.Replication != nil {
		leader = n.Replication.GetLeader()
	}

	// Get actual live members from the gossip manager (default to empty)
	members := []string{n.ID} // Always include self
	if n.Gossip != nil {
		members = n.Gossip.GetMembers()
	}

	status := map[string]interface{}{
		"node_id":         n.ID,
		"address":         n.BindAddress,
		"http_port":       n.HTTPPort,
		"cluster_members": members,
		"leader":          leader,
		"is_leader":       leader == n.ID,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(status); err != nil {
		log.Printf("[Node %s] Error encoding status response: %v", truncateID(n.ID), err)
	}
}

// handleHealth handles health check requests
func (n *Node) handleHealth(w http.ResponseWriter, r *http.Request) {
	// Log health check request for debugging
	log.Printf("[Node %s] Health check request from %s (path: %s)",
		n.ID[:8], r.RemoteAddr, r.URL.Path)

	// Simple health check that responds with 200
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	// Default cluster size for tests or when gossip is not initialized
	clusterSize := 1

	// Get actual cluster size if gossip is available
	if n.Gossip != nil {
		clusterSize = len(n.Gossip.GetMembers())
	}

	resp := map[string]interface{}{
		"status":       "ok",
		"nodeId":       n.ID,
		"uptime":       time.Since(startTime).String(),
		"version":      "1.0.0", // Add version info
		"cluster_size": clusterSize,
		"timestamp":    time.Now().Format(time.RFC3339),
	}

	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.Printf("[Node %s] Error encoding health response: %v", n.ID[:8], err)
	}
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
	var peerAddrs []string

	// Updated this section to extract member IDs and also populate node info
	for _, member := range members {
		if member == n.ID {
			continue
		}

		// Get node info from DHT
		nodeInfo := n.DHT.GetNodeInfoByID(member)
		if nodeInfo != nil {
			peers = append(peers, member)

			// Register the node's info with the consensus module
			n.Consensus.UpdateNodeInfo(
				member,
				nodeInfo.Address,
				nodeInfo.HTTPPort,
			)

			peerAddrs = append(peerAddrs,
				fmt.Sprintf("%s (addr: %s, httpPort: %d)",
					member[:8],
					nodeInfo.Address,
					nodeInfo.HTTPPort,
				),
			)
		}
	}

	if len(peers) > 0 {
		log.Printf("[Node %s] Discovered %d peers: %v", n.ID[:8], len(peers), peerAddrs)
	} else {
		log.Printf("[Node %s] No peers discovered, acting as standalone node", n.ID[:8])
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
			// Even if we time out waiting for a leader, we can still operate
			// This is not a fatal error as the cluster can converge later
			log.Printf("[Node %s] Timeout waiting for leader election, continuing as follower", n.ID[:8])
			return nil
		case <-ticker.C:
			// Check if there's a leader
			if leader := n.Consensus.GetLeader(); leader != "" {
				log.Printf("[Node %s] Cluster joined successfully, leader is %s", n.ID[:8], leader)
				return nil
			}

			// If we're the only node, we should become the leader
			if len(peers) == 0 {
				// The election should handle this automatically
				log.Printf("[Node %s] No other peers, should be operating as leader", n.ID[:8])
				return nil
			}
		}
	}
}

// Handler returns the HTTP handler for the node
func (n *Node) Handler() http.Handler {
	log.Printf("[Node %s] Creating HTTP handler with all endpoints", n.ID[:8])
	mux := http.NewServeMux()

	// Health check endpoints
	log.Printf("[Node %s] Registering health endpoints at /health and /healthz", n.ID[:8])
	mux.HandleFunc("/health", n.handleHealth)
	mux.HandleFunc("/healthz", n.handleHealth)

	// Regular API endpoints
	mux.HandleFunc("/publish", n.handlePublish)
	mux.HandleFunc("/subscribe", n.handleSubscribe)
	mux.HandleFunc("/metrics", n.handleMetrics)
	mux.HandleFunc("/status", n.handleStatus)

	// DHT and storage management endpoints
	mux.HandleFunc("/dht", n.handleDHT)
	mux.HandleFunc("/store", n.handleGetStore)
	mux.HandleFunc("/store/tiers", n.handleStorageTiers)
	mux.HandleFunc("/store/compact", n.handleStorageCompact)

	// Topic endpoints
	mux.HandleFunc("/topics", n.handleTopics)

	// AI endpoints
	mux.HandleFunc("/ai", n.handleAI)
	mux.HandleFunc("/ai/status", n.handleAIStatus)
	mux.HandleFunc("/ai/recommendations", n.handleAIRecommendations)
	mux.HandleFunc("/ai/anomalies", n.handleAIAnomalies)
	mux.HandleFunc("/ai/predictions", n.handleAIPredictions)
	mux.HandleFunc("/ai/train", n.handleAITrain)
	mux.HandleFunc("/ai/thresholds", n.handleAIThresholds)
	mux.HandleFunc("/ai/thresholds/reload", n.handleAIThresholdsReload)

	// Raft consensus endpoints
	log.Printf("[Node %s] Registering Raft consensus endpoints", n.ID[:8])
	mux.HandleFunc("/raft/vote", n.Consensus.HandleVote)
	mux.HandleFunc("/raft/append", n.Consensus.HandleAppendEntries)

	// Add other handlers as needed
	return mux // Using direct mux without middleware for simplicity
}

// truncateID safely truncates a node ID for logging
func truncateID(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

// feedMetricsToAI periodically sends metrics to the AI engine for analysis
func (n *Node) feedMetricsToAI() {
	log.Printf("[Node %s] Started metrics feed to AI engine", n.ID[:8])

	// Create ticker for periodic metrics collection
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-n.stopCh:
			log.Printf("[Node %s] Stopping metrics feed to AI engine", n.ID[:8])
			return
		case <-ticker.C:
			// Collect and feed metrics to AI engine
			n.collectAndSendMetrics()
		}
	}
}

// collectAndSendMetrics gathers node metrics and sends them to AI engine
func (n *Node) collectAndSendMetrics() {
	// Skip if AI engine is not available
	if n.AI == nil {
		return
	}

	// Get current timestamp
	now := time.Now()

	// Collect DHT metrics
	if n.DHT != nil {
		// Get node count from the topic map
		var nodeCount int
		if nodes := n.DHT.GetTopicMap(); nodes != nil {
			// Count unique nodes across all topics
			uniqueNodes := make(map[string]struct{})
			for _, nodeIDs := range nodes {
				for _, nodeID := range nodeIDs {
					uniqueNodes[nodeID] = struct{}{}
				}
			}
			nodeCount = len(uniqueNodes)
		}

		n.AI.RecordMetric(ai.MetricKindTopicActivity, n.ID, float64(nodeCount), map[string]string{
			"node_id": n.ID,
			"source":  "dht",
		})

		// Get topic count
		topicCount := 0
		if topicMap := n.DHT.GetTopicMap(); topicMap != nil {
			topicCount = len(topicMap)
		}

		n.AI.RecordMetric(ai.MetricKindQueueDepth, n.ID, float64(topicCount), map[string]string{
			"node_id": n.ID,
			"source":  "dht",
		})
	}

	// Collect gossip metrics
	if n.Gossip != nil {
		// Get member count (nodes in cluster)
		members := n.Gossip.GetMembers()
		memberCount := len(members)

		n.AI.RecordMetric(ai.MetricKindSubscriberCount, n.ID, float64(memberCount), map[string]string{
			"node_id": n.ID,
			"source":  "gossip",
		})
	}

	// Collect store metrics
	if n.Store != nil {
		// Get topic list
		topics := n.Store.GetTopics()

		n.AI.RecordMetric(ai.MetricKindTopicActivity, n.ID+"-store", float64(len(topics)), map[string]string{
			"node_id": n.ID,
			"source":  "store",
		})

		// Get message counts for each topic
		for _, topic := range topics {
			msgCount := n.Store.GetMessageCountForTopic(topic)
			n.AI.RecordMetric(ai.MetricKindMessageRate, topic, float64(msgCount), map[string]string{
				"node_id": n.ID,
				"topic":   topic,
				"source":  "store",
			})
		}
	}

	// Collect system metrics using the metrics package
	cpuUsage, memUsage, goroutineCount := metrics.GetSystemMetrics()

	n.AI.RecordMetric(ai.MetricKindCPUUsage, n.ID, cpuUsage, map[string]string{
		"node_id": n.ID,
		"source":  "system",
	})

	n.AI.RecordMetric(ai.MetricKindMemoryUsage, n.ID, memUsage, map[string]string{
		"node_id": n.ID,
		"source":  "system",
	})

	// Record goroutine count as a custom metric
	n.AI.RecordMetric(ai.MetricKindNetworkTraffic, n.ID+"-goroutines", float64(goroutineCount), map[string]string{
		"node_id": n.ID,
		"source":  "system",
		"type":    "goroutines",
	})

	// Include network traffic estimation based on message rate
	if n.Metrics != nil {
		messageRate := n.Metrics.MessageRate
		n.AI.RecordMetric(ai.MetricKindNetworkTraffic, n.ID, messageRate*1024, map[string]string{
			"node_id": n.ID,
			"source":  "metrics",
			"unit":    "bytes_per_second",
		})
	}

	log.Printf("[Node %s] Sent metrics to AI engine at %s", n.ID[:8], now.Format(time.RFC3339))
}

// handleDHT handles requests for DHT status
func (n *Node) handleDHT(w http.ResponseWriter, r *http.Request) {
	// Ensure request is GET
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Get DHT information
	dhtInfo := map[string]interface{}{
		"node_id":    n.ID,
		"topic_map":  n.DHT.GetTopicMap(),
		"node_count": len(n.DHT.GetTopicMap()),
	}

	// Get all node information if detailed=true
	if r.URL.Query().Get("detailed") == "true" {
		var nodes []interface{}
		for _, nodeInfo := range n.Gossip.GetMembers() {
			info := n.DHT.GetNodeInfoByID(nodeInfo)
			if info != nil {
				nodes = append(nodes, info)
			}
		}
		dhtInfo["nodes"] = nodes
	}

	// Return as JSON
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(dhtInfo); err != nil {
		log.Printf("[Node %s] Error encoding DHT status: %v", truncateID(n.ID), err)
	}
}

// handleStorageTiers handles requests for storage tier configuration
func (n *Node) handleStorageTiers(w http.ResponseWriter, r *http.Request) {
	// Handle GET requests for configuration
	if r.Method == http.MethodGet {
		// Get storage tier configuration
		tierConfig := n.Store.GetTierConfig()

		// Return as JSON
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(tierConfig); err != nil {
			log.Printf("[Node %s] Error encoding storage tier config: %v", truncateID(n.ID), err)
		}
		return
	}

	// Handle POST requests for topic tier assignment
	if r.Method == http.MethodPost {
		var request struct {
			Topic string `json:"topic"`
			Tier  string `json:"tier"` // "memory", "disk", or "cloud"
		}

		// Read and parse the request
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, "Invalid request", http.StatusBadRequest)
			return
		}

		// Validate topic and tier
		if request.Topic == "" {
			http.Error(w, "Topic is required", http.StatusBadRequest)
			return
		}

		if request.Tier == "" {
			http.Error(w, "Tier is required", http.StatusBadRequest)
			return
		}

		// Set the storage tier for the topic using our store implementation
		err := n.Store.SetStorageTierForTopic(request.Topic, request.Tier)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to set storage tier: %v", err), http.StatusBadRequest)
			return
		}

		// Return success response
		result := map[string]string{
			"status": "success",
			"topic":  request.Topic,
			"tier":   request.Tier,
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(result); err != nil {
			log.Printf("[Node %s] Error encoding storage tier response: %v", truncateID(n.ID), err)
		}
		return
	}

	// Method not allowed
	http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
}

// handleStorageCompact handles requests to compact storage
func (n *Node) handleStorageCompact(w http.ResponseWriter, r *http.Request) {
	// Ensure request is POST
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Try to compact storage
	err := n.Store.Compact()

	// Build response
	response := map[string]interface{}{
		"success": err == nil,
	}

	if err != nil {
		response["error"] = err.Error()
		w.WriteHeader(http.StatusInternalServerError)
	} else {
		response["message"] = "Storage compaction completed successfully"
		w.WriteHeader(http.StatusOK)
	}

	// Return as JSON
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("[Node %s] Error encoding compact response: %v", truncateID(n.ID), err)
	}
}

// handleTopics handles requests for topic listing
func (n *Node) handleTopics(w http.ResponseWriter, r *http.Request) {
	// Ensure request is GET
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Get topic information from the store
	topics := n.Store.GetTopics()

	// Build topic data with message counts
	topicData := make([]map[string]interface{}, 0, len(topics))
	for _, topic := range topics {
		messageCount := n.Store.GetMessageCountForTopic(topic)
		subscriberCount := n.Store.GetSubscriberCountForTopic(topic)

		// Get nodes hosting this topic
		nodes := n.DHT.GetNodesForTopic(topic)
		nodeIDs := make([]string, 0, len(nodes))
		for _, node := range nodes {
			nodeIDs = append(nodeIDs, node.ID)
		}

		// Get storage tier for this topic
		storageTier := n.Store.GetStorageTierForTopic(topic)

		// Add topic data
		topicData = append(topicData, map[string]interface{}{
			"name":             topic,
			"message_count":    messageCount,
			"subscriber_count": subscriberCount,
			"nodes":            nodeIDs,
			"storage_tier":     storageTier,
			"has_subscribers":  n.Store.HasSubscribers(topic),
		})
	}

	// Build complete response
	response := map[string]interface{}{
		"topics": topicData,
		"count":  len(topics),
	}

	// Return as JSON
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("[Node %s] Error encoding topics response: %v", truncateID(n.ID), err)
	}
}

// handleAI handles requests for AI insights and predictions
func (n *Node) handleAI(w http.ResponseWriter, r *http.Request) {
	// Ensure request is GET
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Get status from AI engine
	status := n.AI.GetStatus()

	// Add summary of available endpoints
	endpoints := []string{
		"/ai/status",
		"/ai/predictions",
		"/ai/anomalies",
		"/ai/recommendations",
		"/ai/train",
	}

	// Build response with all AI info
	response := map[string]interface{}{
		"status":    status,
		"endpoints": endpoints,
	}

	// Return as JSON
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("[Node %s] Error encoding AI response: %v", truncateID(n.ID), err)
	}
}

// handleAIRecommendations handles requests for AI-based scaling recommendations
func (n *Node) handleAIRecommendations(w http.ResponseWriter, r *http.Request) {
	// Ensure request is GET
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Get recommendations from the AI engine
	var recommendations []map[string]interface{}

	// Try to get recommendations from the production AI engine
	aiEngine, ok := n.AI.(*ai.Engine)
	if ok {
		// Get scaling recommendations from the AI engine
		scalingRecs := aiEngine.GetScalingRecommendations("local")

		// Convert to map format for JSON response
		recommendations = make([]map[string]interface{}, 0, len(scalingRecs))
		for _, rec := range scalingRecs {
			recommendations = append(recommendations, map[string]interface{}{
				"resource":        rec.Resource,
				"current_value":   rec.CurrentValue,
				"predicted_value": rec.PredictedValue,
				"recommendation":  rec.RecommendedAction,
				"confidence":      rec.Confidence,
				"timestamp":       rec.Timestamp.Format(time.RFC3339),
				"reason":          rec.Reason,
			})
		}
		log.Printf("[Node %s] Got %d AI recommendations from engine", n.ID[:8], len(recommendations))
	} else {
		// Try to use test engine if available
		testEngine, ok := n.AI.(*TestAIEngine)
		if ok {
			recommendations = testEngine.GetRecommendations()
			log.Printf("[Node %s] Got %d AI recommendations from test engine", n.ID[:8], len(recommendations))
		} else {
			// If we couldn't get recommendations from any AI engine, log a warning
			log.Printf("[Node %s] Warning: Could not get AI recommendations, AI engine unavailable", n.ID[:8])
			recommendations = []map[string]interface{}{}
		}
	}

	// Build response
	response := map[string]interface{}{
		"recommendations": recommendations,
		"timestamp":       time.Now().Format(time.RFC3339),
		"node_id":         n.ID,
	}

	// Return as JSON
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("[Node %s] Error encoding AI recommendations: %v", truncateID(n.ID), err)
	}
}

// handleAITrain handles requests to manually train the AI models
func (n *Node) handleAITrain(w http.ResponseWriter, r *http.Request) {
	// Only allow POST method for training endpoint
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse training parameters from request body
	var params struct {
		Resource string `json:"resource"` // cpu, memory, network, disk, message_rate
		LookBack int    `json:"lookback"` // lookback period in hours
		NodeID   string `json:"node_id"`  // optional node ID to train for
	}

	if err := json.NewDecoder(r.Body).Decode(&params); err != nil {
		http.Error(w, "Invalid request body: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Validate parameters
	if params.Resource == "" {
		http.Error(w, "Missing resource parameter", http.StatusBadRequest)
		return
	}

	if params.LookBack <= 0 {
		http.Error(w, "LookBack must be greater than 0", http.StatusBadRequest)
		return
	}

	// Use this node's ID if not specified
	if params.NodeID == "" {
		params.NodeID = n.ID
	}

	// Convert lookback hours to a duration
	lookbackDuration := time.Duration(params.LookBack) * time.Hour

	// Map resource string to AI resource type
	var resourceType prediction.ResourceType
	switch strings.ToLower(params.Resource) {
	case "cpu":
		resourceType = prediction.ResourceCPU
	case "memory":
		resourceType = prediction.ResourceMemory
	case "network":
		resourceType = prediction.ResourceNetwork
	case "disk":
		resourceType = prediction.ResourceDisk
	case "message_rate":
		resourceType = prediction.ResourceMessageRate
	default:
		http.Error(w, "Unsupported resource type: "+params.Resource, http.StatusBadRequest)
		return
	}

	// Prepare response
	response := struct {
		Success bool   `json:"success"`
		Message string `json:"message"`
		Error   string `json:"error,omitempty"`
	}{}

	// Check if AI engine is available
	if n.AI == nil {
		response.Success = false
		response.Message = "Failed to train model"
		response.Error = "AI engine not available"
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		if err := json.NewEncoder(w).Encode(response); err != nil {
			log.Printf("[Node %s] Error encoding response: %v", n.ID[:8], err)
		}
		return
	}

	// Try to use production engine
	enginePtr, ok := n.AI.(*ai.Engine)
	if ok {
		// Attempt to train the model with production engine
		err := enginePtr.TrainResourceModel(resourceType, n.ID, lookbackDuration)
		if err != nil {
			response.Success = false
			response.Message = "Failed to train model"
			response.Error = err.Error()
		} else {
			response.Success = true
			response.Message = fmt.Sprintf("Successfully trained model for %s with %s lookback", params.Resource, lookbackDuration)
		}
	} else {
		// Try to use test engine
		testEngine, ok := n.AI.(*TestAIEngine)
		if ok {
			// Attempt to train the model with test engine
			err := testEngine.TrainResourceModel(resourceType, n.ID, lookbackDuration)
			if err != nil {
				response.Success = false
				response.Message = "Failed to train model"
				response.Error = err.Error()
			} else {
				response.Success = true
				response.Message = fmt.Sprintf("Successfully trained model for %s with %s lookback", params.Resource, lookbackDuration)
			}
		} else {
			response.Success = false
			response.Message = "Failed to train model"
			response.Error = "AI engine implementation does not support training"
		}
	}

	// Return JSON response
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("[Node %s] Error encoding AI training response: %v", truncateID(n.ID), err)
	}
}

// handleStore handles store-related requests
func (n *Node) handleStore(w http.ResponseWriter, r *http.Request) {
	// Only accept POST requests
	if r.Method != http.MethodPost && r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Use semaphore to limit concurrent requests
	select {
	case n.semaphore <- struct{}{}:
		defer func() { <-n.semaphore }()
	default:
		http.Error(w, "Server busy, too many concurrent requests", http.StatusServiceUnavailable)
		return
	}

	// GET request to retrieve messages
	if r.Method == http.MethodGet {
		topic := r.URL.Query().Get("topic")
		if topic == "" {
			http.Error(w, "Topic parameter required", http.StatusBadRequest)
			return
		}

		// Get messages from the store
		messages := n.Store.GetMessages(topic)

		// Return messages as JSON
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(messages); err != nil {
			log.Printf("[Node %s] Error encoding messages: %v", n.ID[:8], err)
			http.Error(w, "Failed to encode messages", http.StatusInternalServerError)
			return
		}
		return
	}

	// POST request to store a message
	var msg store.Message
	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		http.Error(w, "Invalid message format", http.StatusBadRequest)
		return
	}

	// Validate message
	if msg.Topic == "" {
		http.Error(w, "Topic cannot be empty", http.StatusBadRequest)
		return
	}

	// Generate ID if not provided
	if msg.ID == "" {
		msg.ID = uuid.New().String()
	}

	// Set timestamp if not provided
	if msg.Timestamp.IsZero() {
		msg.Timestamp = time.Now()
	}

	// Store the message
	if err := n.Store.Publish(msg); err != nil {
		log.Printf("[Node %s] Error storing message: %v", n.ID[:8], err)
		http.Error(w, fmt.Sprintf("Failed to store message: %v", err), http.StatusInternalServerError)
		return
	}

	// Return success response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(map[string]string{
		"status":  "success",
		"message": "Message stored successfully",
		"id":      msg.ID,
	}); err != nil {
		log.Printf("[Node %s] Error encoding success response: %v", n.ID[:8], err)
	}
}

// storeClusterAdapter adapts cluster.ClusterProviderAdapter to store.ClusterProvider
type storeClusterAdapter struct {
	adapter *cluster.ClusterProviderAdapter
}

// GetAllNodeIDs implements the store.ClusterProvider interface
func (s *storeClusterAdapter) GetAllNodeIDs() []string {
	return s.adapter.GetAllNodeIDs()
}

// GetNodeClient implements the store.ClusterProvider interface
func (s *storeClusterAdapter) GetNodeClient(nodeID string) (store.NodeClient, error) {
	client, err := s.adapter.GetNodeClient(nodeID)
	if err != nil {
		return nil, err
	}
	return &storeNodeClientAdapter{client: client}, nil
}

// storeNodeClientAdapter adapts cluster.NodeClientAdapter to store.NodeClient
type storeNodeClientAdapter struct {
	client cluster.NodeClientAdapter
}

// GetMetrics implements the store.NodeClient interface
func (a *storeNodeClientAdapter) GetMetrics() (map[string]float64, error) {
	return a.client.GetMetrics()
}

// handleAIThresholds handles retrieving and updating AI threshold configuration
func (n *Node) handleAIThresholds(w http.ResponseWriter, r *http.Request) {
	// Handle GET requests - return current thresholds
	if r.Method == http.MethodGet {
		thresholds := n.AI.GetThresholds()

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(thresholds); err != nil {
			log.Printf("[Node %s] Error encoding thresholds: %v", truncateID(n.ID), err)
			http.Error(w, "Error encoding response", http.StatusInternalServerError)
		}
		return
	}

	// Handle PUT/POST requests - update thresholds
	if r.Method == http.MethodPut || r.Method == http.MethodPost {
		var newThresholds ai.ThresholdConfig

		// Parse request body
		if err := json.NewDecoder(r.Body).Decode(&newThresholds); err != nil {
			http.Error(w, fmt.Sprintf("Error parsing request: %v", err), http.StatusBadRequest)
			return
		}

		// Validate thresholds
		if err := newThresholds.Validate(); err != nil {
			http.Error(w, fmt.Sprintf("Invalid threshold values: %v", err), http.StatusBadRequest)
			return
		}

		// Update thresholds in AI engine
		if err := n.AI.SetThresholds(newThresholds); err != nil {
			http.Error(w, fmt.Sprintf("Failed to update thresholds: %v", err), http.StatusInternalServerError)
			return
		}

		// Return success response
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(w).Encode(map[string]string{
			"status":  "success",
			"message": "Thresholds updated successfully",
		}); err != nil {
			log.Printf("[Node %s] Error encoding response: %v", truncateID(n.ID), err)
		}
		return
	}

	// Method not allowed for other HTTP methods
	http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
}

// handleAIThresholdsReload handles triggering threshold configuration reload
func (n *Node) handleAIThresholdsReload(w http.ResponseWriter, r *http.Request) {
	// Only allow POST requests
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Check if the engine supports reload
	engineWithReload, ok := n.AI.(interface {
		TriggerConfigReload()
	})

	if !ok {
		http.Error(w, "AI engine does not support configuration reload", http.StatusNotImplemented)
		return
	}

	// Trigger reload
	engineWithReload.TriggerConfigReload()

	// Return success
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]string{
		"status":  "success",
		"message": "Threshold configuration reload triggered",
	}); err != nil {
		log.Printf("[Node %s] Error encoding response: %v", truncateID(n.ID), err)
	}
}
