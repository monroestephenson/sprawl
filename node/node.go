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
	BindAddress string
	HTTPAddress string
	HTTPPort    int
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

	router := router.NewRouter(nodeID, dht, store)
	balancer := balancer.NewLoadBalancer(dht)
	metrics := metrics.NewMetrics()

	return &Node{
		ID:          nodeID,
		Gossip:      gm,
		Store:       store,
		DHT:         dht,
		Router:      router,
		Balancer:    balancer,
		Metrics:     metrics,
		BindAddress: bindAddr,
		HTTPAddress: httpAddr,
		HTTPPort:    httpPort,
	}, nil
}

// StartHTTP starts the HTTP server for publish/subscribe endpoints
func (n *Node) StartHTTP() {
	http.HandleFunc("/publish", n.handlePublish)
	http.HandleFunc("/subscribe", n.handleSubscribe)
	http.HandleFunc("/metrics", n.handleMetrics)
	http.HandleFunc("/status", n.handleStatus)

	addr := fmt.Sprintf("%s:%d", n.HTTPAddress, n.HTTPPort)
	log.Printf("Starting HTTP server on %s", addr)
	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatalf("HTTP server failed: %v", err)
	}
}

func (n *Node) handlePublish(w http.ResponseWriter, r *http.Request) {
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

	ctx := context.Background()
	if err := n.Router.RouteMessage(ctx, routerMsg); err != nil {
		log.Printf("[Node %s] Failed to route message: %v", n.ID[:8], err)

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

	metrics := map[string]interface{}{
		"node_id": n.ID,
		"router":  n.Router.GetMetrics(),
		"system":  n.Metrics.GetSnapshot(),
		"cluster": n.Gossip.GetMembers(),
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
	return n.Gossip.JoinCluster(seeds)
}

// Shutdown leaves the gossip network
func (n *Node) Shutdown() {
	n.Gossip.Shutdown()
}
