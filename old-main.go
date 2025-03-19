package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"sprawl/node"
)

func main() {
	// Get addresses from environment variables with fallbacks
	envBindAddr := os.Getenv("SPRAWL_BIND_ADDR")
	if envBindAddr == "" {
		envBindAddr = "0.0.0.0"
	}
	envAdvertiseAddr := os.Getenv("SPRAWL_ADVERTISE_ADDR")
	if envAdvertiseAddr == "" {
		envAdvertiseAddr = "127.0.0.1"
	}
	envSeeds := os.Getenv("SPRAWL_CLUSTER_SEEDS")

	// Command-line flags to configure gossip & HTTP (environment variables take precedence)
	bindAddr := flag.String("bindAddr", envBindAddr, "Bind address for all services")
	advertiseAddr := flag.String("advertiseAddr", envAdvertiseAddr, "Advertise address for Raft")
	bindPort := flag.Int("bindPort", 7946, "Gossip bind port")
	httpPort := flag.Int("httpPort", 8080, "HTTP server port")
	seedNodes := flag.String("seeds", envSeeds, "Comma-separated list of seed nodes (host:port)")

	flag.Parse()

	// Create node with both bind and advertise addresses
	n, err := node.NewNode(*bindAddr, *advertiseAddr, *bindPort, *httpPort)
	if err != nil {
		log.Fatalf("Failed to create node: %v", err)
	}

	// Start HTTP server in a goroutine
	go n.StartHTTP()

	// Wait for HTTP server to be ready
	ready := make(chan struct{})
	go func() {
		for {
			resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%d/health", *httpPort))
			if err == nil && resp.StatusCode == http.StatusOK {
				resp.Body.Close()
				close(ready)
				return
			}
			if resp != nil {
				resp.Body.Close()
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()

	// Wait for server to be ready or timeout
	select {
	case <-ready:
		log.Printf("HTTP server is ready on port %d", *httpPort)
	case <-time.After(5 * time.Second):
		log.Printf("Warning: HTTP server readiness check timed out")
	}

	// Join cluster if seeds provided
	var seedsArr []string
	if *seedNodes != "" {
		seedsArr = strings.Split(*seedNodes, ",")
		log.Printf("Attempting to join cluster with seeds: %v", seedsArr)
	}
	if err := n.JoinCluster(seedsArr); err != nil {
		log.Printf("Failed to join cluster: %v", err)
	}

	// Print out the node's ID and addresses
	log.Printf("Node started with ID: %s", n.ID)
	log.Printf("Bind address: %s, Advertise address: %s", *bindAddr, *advertiseAddr)

	// Set up signal handling for graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Wait for shutdown signal
	sig := <-sigCh
	log.Printf("Received signal %v, initiating graceful shutdown...", sig)

	// Perform graceful shutdown
	n.Shutdown()
}
