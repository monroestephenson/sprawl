package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"sprawl/node"
)

func main() {
	// Get bind addresses from environment variables with fallbacks
	envBindAddr := os.Getenv("BIND_ADDR")
	if envBindAddr == "" {
		envBindAddr = "0.0.0.0"
	}
	envHttpAddr := os.Getenv("HTTP_ADDR")
	if envHttpAddr == "" {
		envHttpAddr = "0.0.0.0"
	}
	envSeeds := os.Getenv("SEEDS")

	// Command-line flags to configure gossip & HTTP (environment variables take precedence)
	bindAddr := flag.String("bindAddr", envBindAddr, "Gossip bind address")
	bindPort := flag.Int("bindPort", 7946, "Gossip bind port")
	httpAddr := flag.String("httpAddr", envHttpAddr, "HTTP bind address")
	httpPort := flag.Int("httpPort", 8080, "HTTP server port")
	seedNodes := flag.String("seeds", envSeeds, "Comma-separated list of seed nodes (host:port)")

	flag.Parse()

	// Create node
	n, err := node.NewNode(*bindAddr, *bindPort, *httpAddr, *httpPort)
	if err != nil {
		log.Fatalf("Failed to create node: %v", err)
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

	// Print out the node's ID
	log.Printf("Node started with ID: %s\n", n.ID)

	// Set up signal handling for graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Start HTTP server in a goroutine
	go n.StartHTTP()

	// Wait for shutdown signal
	sig := <-sigCh
	log.Printf("Received signal %v, initiating graceful shutdown...", sig)

	// Perform graceful shutdown
	n.Shutdown()
}
