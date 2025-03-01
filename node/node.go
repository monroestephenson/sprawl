package node

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"sprawl/store"

	"github.com/google/uuid"
)

type Node struct {
	ID          string
	Gossip      *GossipManager
	Store       *store.Store
	BindAddress string
	HTTPPort    int
}

// NewNode initializes a Node with a gossip manager and an in-memory store
func NewNode(bindAddr string, bindPort int, httpPort int) (*Node, error) {
	// Generate a random ID for this node
	nodeID := uuid.New().String()

	gm, err := NewGossipManager(nodeID, bindAddr, bindPort)
	if err != nil {
		return nil, err
	}

	return &Node{
		ID:          nodeID,
		Gossip:      gm,
		Store:       store.NewStore(),
		BindAddress: bindAddr,
		HTTPPort:    httpPort,
	}, nil
}

// StartHTTP starts the HTTP server for publish/subscribe endpoints
func (n *Node) StartHTTP() {
	mux := http.NewServeMux()

	// Publish endpoint
	mux.HandleFunc("/publish", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid method", http.StatusMethodNotAllowed)
			return
		}

		var req struct {
			Topic   string `json:"topic"`
			Message string `json:"message"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Bad request", http.StatusBadRequest)
			return
		}

		// Publish to local store
		n.Store.Publish(req.Topic, []byte(req.Message))

		// For MVP: We do NOT yet replicate to other nodes
		// (In a real system, you'd propagate this message to cluster members)

		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "Message published to topic '%s'\n", req.Topic)
	})

	// Subscribe endpoint (simple example returning 200, you’d want streaming or WebSockets)
	// We'll store the subscription callback in memory
	mux.HandleFunc("/subscribe", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid method", http.StatusMethodNotAllowed)
			return
		}

		var req struct {
			Topic string `json:"topic"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Bad request", http.StatusBadRequest)
			return
		}

		// Register a callback for this topic
		// For demonstration, we simply log the received messages
		n.Store.Subscribe(req.Topic, func(msg store.Message) {
			log.Printf("Node %s received message on topic '%s': %s\n",
				n.ID, msg.Topic, string(msg.Payload))
		})

		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "Subscribed to topic '%s'\n", req.Topic)
	})

	// Expose cluster info
	mux.HandleFunc("/members", func(w http.ResponseWriter, r *http.Request) {
		members := n.Gossip.GetMembers()
		enc := json.NewEncoder(w)
		_ = enc.Encode(members)
	})

	addr := fmt.Sprintf(":%d", n.HTTPPort)
	log.Printf("Node %s listening on HTTP %s\n", n.ID, addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatal("HTTP server error:", err)
	}
}

// JoinCluster attempts to join an existing cluster
func (n *Node) JoinCluster(seeds []string) error {
	return n.Gossip.JoinCluster(seeds)
}

// Shutdown leaves the gossip network
func (n *Node) Shutdown() {
	n.Gossip.Shutdown()
}
