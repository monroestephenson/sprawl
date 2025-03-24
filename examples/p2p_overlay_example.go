package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"sprawl/node"
	"sprawl/node/dht"
)

func main() {
	// Parse command-line flags
	var (
		nodeID   = flag.String("id", "", "Node ID (required)")
		bindAddr = flag.String("addr", "127.0.0.1", "Bind address")
		bindPort = flag.Int("port", 7946, "Bind port")
		httpPort = flag.Int("http", 8080, "HTTP port")
		join     = flag.String("join", "", "Comma-separated list of seed nodes to join")
	)
	flag.Parse()

	// Validate required flags
	if *nodeID == "" {
		log.Fatal("Node ID is required")
	}

	// Set up logging
	log.SetPrefix(fmt.Sprintf("[%s] ", *nodeID))
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	log.Printf("Starting P2P node with ID %s at %s:%d (HTTP port: %d)",
		*nodeID, *bindAddr, *bindPort, *httpPort)

	// Create DHT instance
	dhtInstance := dht.NewDHT(*nodeID)
	dhtInstance.InitializeOwnNode(*bindAddr, *bindPort, *httpPort)

	// Create gossip manager
	gossipManager, err := node.NewGossipManager(*nodeID, *bindAddr, *bindPort, dhtInstance, *httpPort)
	if err != nil {
		log.Fatalf("Failed to create gossip manager: %v", err)
	}
	defer gossipManager.Shutdown()

	// Join cluster if seed nodes are provided
	if *join != "" {
		seeds := []string{*join}
		log.Printf("Joining cluster with seed node: %s", *join)
		err := gossipManager.JoinCluster(seeds)
		if err != nil {
			log.Fatalf("Failed to join cluster: %v", err)
		}
	} else {
		log.Printf("Starting a new cluster")
	}

	// Set up HTTP server for health checks
	mux := http.NewServeMux()
	node.RegisterHealthEndpoint(mux, gossipManager, gossipManager.GetMetricsManager(), *nodeID, "1.0.0")

	// Add a node status endpoint
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "Node ID: %s\n", *nodeID)
		fmt.Fprintf(w, "Address: %s:%d\n", *bindAddr, *bindPort)
		fmt.Fprintf(w, "HTTP Port: %d\n", *httpPort)
		fmt.Fprintf(w, "Uptime: %s\n", time.Since(gossipManager.GetStartTime()))
		fmt.Fprintf(w, "Cluster Members: %d\n", len(gossipManager.GetMembers()))

		fmt.Fprintf(w, "\nCluster Members:\n")
		for _, member := range gossipManager.GetMembers() {
			info := gossipManager.GetMemberInfo(member)
			if info != nil {
				fmt.Fprintf(w, "  - %s (%s:%d) - State: %s\n",
					member, info.Address, info.Port, gossipManager.GetNodeState(member))
			} else {
				fmt.Fprintf(w, "  - %s (unknown)\n", member)
			}
		}

		fmt.Fprintf(w, "\nMetrics:\n")
		metrics := gossipManager.GetMetricsManager().GetCurrentMetrics()
		fmt.Fprintf(w, "  - CPU Usage: %.2f%%\n", metrics.CPUUsage)
		fmt.Fprintf(w, "  - Memory Usage: %.2f%%\n", metrics.MemoryUsage)
		fmt.Fprintf(w, "  - Message Count: %d\n", metrics.MessageCount)
		fmt.Fprintf(w, "  - Message Rate: %.2f/s\n", metrics.MessageRate)
	})

	// Simulate some message activity
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		for range ticker.C {
			gossipManager.AddMessageCount(1)
		}
	}()

	// Start HTTP server
	go func() {
		httpAddr := fmt.Sprintf(":%d", *httpPort)
		log.Printf("Starting HTTP server at %s", httpAddr)
		if err := http.ListenAndServe(httpAddr, mux); err != nil {
			log.Fatalf("HTTP server failed: %v", err)
		}
	}()

	// Wait for interrupt signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	log.Printf("Received signal %v, shutting down", sig)
}
