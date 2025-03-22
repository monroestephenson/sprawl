package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "sprawlctl",
	Short: "sprawlctl controls the Sprawl message system",
}

var (
	nodes      []string
	topic      string
	payload    string
	count      int
	parallel   int
	interval   time.Duration
	waitForAck bool
)

func init() {
	rootCmd.PersistentFlags().StringSliceVarP(&nodes, "nodes", "n", []string{"http://localhost:8080"}, "Comma-separated list of node URLs")

	publishCmd := &cobra.Command{
		Use:   "publish",
		Short: "Publish messages to a topic",
		Run:   runPublish,
	}
	publishCmd.Flags().StringVarP(&topic, "topic", "t", "test", "Topic to publish to")
	publishCmd.Flags().StringVarP(&payload, "payload", "p", "test message", "Message payload")
	publishCmd.Flags().IntVarP(&count, "count", "c", 1, "Number of messages to send")
	publishCmd.Flags().IntVarP(&parallel, "parallel", "P", 1, "Number of parallel publishers")
	publishCmd.Flags().DurationVarP(&interval, "interval", "i", 0, "Interval between messages")
	publishCmd.Flags().BoolVarP(&waitForAck, "wait", "w", true, "Wait for acknowledgment")

	subscribeCmd := &cobra.Command{
		Use:   "subscribe",
		Short: "Subscribe to a topic",
		Run:   runSubscribe,
	}
	subscribeCmd.Flags().StringVarP(&topic, "topic", "t", "test", "Topic to subscribe to")

	testCmd := &cobra.Command{
		Use:   "test",
		Short: "Run integration tests",
		Run:   runTests,
	}
	testCmd.Flags().IntVarP(&count, "count", "c", 100, "Number of test messages")
	testCmd.Flags().IntVarP(&parallel, "parallel", "P", 10, "Number of parallel publishers")

	diagnoseCmd := &cobra.Command{
		Use:   "diagnose",
		Short: "Run diagnostics on a node",
		Long:  `Diagnose runs a series of checks on a node to verify connectivity and API functionality.`,
		Run: func(cmd *cobra.Command, args []string) {
			if err := runDiagnose(nodes[0]); err != nil {
				fmt.Fprintf(os.Stderr, "Error running diagnostics: %v\n", err)
				os.Exit(1)
			}
		},
	}

	rootCmd.AddCommand(publishCmd, subscribeCmd, testCmd, diagnoseCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func runPublish(cmd *cobra.Command, args []string) {
	var wg sync.WaitGroup
	results := make(chan error, count*parallel)

	for p := 0; p < parallel; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < count; i++ {
				node := nodes[i%len(nodes)]
				msg := fmt.Sprintf("%s-%d", payload, i)
				if err := publish(node, topic, msg); err != nil {
					results <- fmt.Errorf("failed to publish to %s: %v", node, err)
				}
				if interval > 0 {
					time.Sleep(interval)
				}
			}
		}()
	}

	wg.Wait()
	close(results)

	failures := 0
	for err := range results {
		failures++
		fmt.Println("Error:", err)
	}

	fmt.Printf("Published %d messages (%d failures)\n", count*parallel-failures, failures)
}

// doRequest sends an HTTP request with retry capability
func doRequest(url string, data []byte, retries int) (*http.Response, error) {
	var resp *http.Response
	var err error

	for i := 0; i <= retries; i++ {
		resp, err = http.Post(url, "application/json", bytes.NewBuffer(data))
		if err == nil {
			return resp, nil
		}

		log.Printf("Request failed (attempt %d/%d): %v", i+1, retries+1, err)
		if i < retries {
			backoff := time.Duration(math.Pow(2, float64(i))) * 100 * time.Millisecond
			log.Printf("Retrying in %v...", backoff)
			time.Sleep(backoff)
		}
	}

	return nil, fmt.Errorf("request failed after %d attempts: %w", retries+1, err)
}

// Modify the subscribe function to use the doRequest function
func subscribe(nodeURL, topic string) error {
	data := map[string]interface{}{
		"topics": []string{topic},
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("error marshaling request: %w", err)
	}

	// Use the doRequest function with 3 retries
	resp, err := doRequest(nodeURL+"/subscribe", jsonData, 3)
	if err != nil {
		return fmt.Errorf("error subscribing to topic: %w", err)
	}
	defer resp.Body.Close()

	// Rest of the function remains the same
	var result struct {
		Status string `json:"status"`
		ID     string `json:"id"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("error decoding response: %w", err)
	}

	if result.Status != "subscribed" {
		return fmt.Errorf("unexpected status: %s", result.Status)
	}

	fmt.Printf("Successfully subscribed to topic: %s on node: %s (ID: %s)\n", topic, nodeURL, result.ID)
	return nil
}

// Modify the publish function to use the doRequest function
func publish(nodeURL, topic, payload string) error {
	data := map[string]interface{}{
		"topic":   topic,
		"payload": payload,
		"ttl":     3, // Default TTL of 3 seconds
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("error marshaling request: %w", err)
	}

	// Enhanced retry logic for publish with specific handling for 503 (Service Unavailable)
	var resp *http.Response
	maxRetries := 5
	baseBackoff := 100 * time.Millisecond
	maxBackoff := 10 * time.Second

	for i := 0; i <= maxRetries; i++ {
		resp, err = http.Post(nodeURL+"/publish", "application/json", bytes.NewBuffer(jsonData))

		// Network error handling
		if err != nil {
			if i < maxRetries {
				backoff := time.Duration(math.Pow(2, float64(i))) * baseBackoff
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
				fmt.Printf("Connection error, retrying in %v... (%d/%d): %v\n", backoff, i+1, maxRetries, err)
				time.Sleep(backoff)
				continue
			} else {
				return fmt.Errorf("request failed after %d attempts: %w", maxRetries+1, err)
			}
		}

		// HTTP status code handling
		if resp.StatusCode == http.StatusServiceUnavailable {
			// Handle "Server is too busy" with adaptive backoff
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()

			var errorResp struct {
				Error      string `json:"error"`
				RetryAfter string `json:"retry_after"`
			}

			// Default retry values
			retryAfter := time.Duration(math.Pow(2, float64(i))) * baseBackoff
			if retryAfter > maxBackoff {
				retryAfter = maxBackoff
			}

			// Try to parse response for more precise retry guidance
			if err := json.Unmarshal(body, &errorResp); err == nil && errorResp.RetryAfter != "" {
				if secs, err := time.ParseDuration(errorResp.RetryAfter + "s"); err == nil {
					retryAfter = secs
				}
			}

			if i < maxRetries {
				fmt.Printf("Server busy, retrying in %v... (%d/%d)\n", retryAfter, i+1, maxRetries)
				time.Sleep(retryAfter)
				continue
			}

			return fmt.Errorf("server is too busy after %d retries", maxRetries)
		} else if resp.StatusCode == http.StatusRequestTimeout || resp.StatusCode == http.StatusGatewayTimeout {
			// Handle timeout errors
			resp.Body.Close()

			if i < maxRetries {
				backoff := time.Duration(math.Pow(2, float64(i))) * baseBackoff * 2 // Double backoff for timeouts
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
				fmt.Printf("Request timed out, retrying with longer timeout in %v... (%d/%d)\n", backoff, i+1, maxRetries)
				time.Sleep(backoff)
				continue
			}

			return fmt.Errorf("request timed out after %d attempts", maxRetries+1)
		} else if resp.StatusCode != http.StatusOK {
			// Handle other HTTP errors
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			return fmt.Errorf("unexpected status: %d - %s", resp.StatusCode, string(body))
		}

		// Success case - parse the response
		defer resp.Body.Close()
		var result struct {
			Status string `json:"status"`
			ID     string `json:"id"`
		}

		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			return fmt.Errorf("error decoding response: %w", err)
		}

		if result.Status != "published" && result.Status != "dropped" {
			return fmt.Errorf("unexpected status: %s", result.Status)
		}

		// Successfully published
		return nil
	}

	// This should never happen but added for completeness
	return fmt.Errorf("failed to publish after %d retries", maxRetries)
}

// Add a new function to check node health before operations
func checkNodeHealth(nodeURL string) error {
	resp, err := http.Get(nodeURL + "/health")
	if err != nil {
		return fmt.Errorf("health check failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("node reported unhealthy status: %d", resp.StatusCode)
	}

	return nil
}

// Add a new diagnose command
func runDiagnose(nodeURL string) error {
	fmt.Printf("Running diagnostics on node: %s\n", nodeURL)

	// Check node health
	fmt.Print("Checking node health... ")
	if err := checkNodeHealth(nodeURL); err != nil {
		fmt.Println("FAILED")
		fmt.Printf("  Error: %v\n", err)
	} else {
		fmt.Println("OK")
	}

	// Check status endpoint
	fmt.Print("Checking node status... ")
	resp, err := http.Get(nodeURL + "/status")
	if err != nil {
		fmt.Println("FAILED")
		fmt.Printf("  Error: %v\n", err)
	} else {
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			fmt.Println("FAILED")
			fmt.Printf("  Unexpected status code: %d\n", resp.StatusCode)
		} else {
			fmt.Println("OK")
			var status map[string]interface{}
			if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
				fmt.Printf("  Error parsing status: %v\n", err)
			} else {
				fmt.Printf("  Node ID: %s\n", status["node_id"])
				fmt.Printf("  HTTP Port: %v\n", status["http_port"])
				fmt.Printf("  Address: %s\n", status["address"])
				fmt.Printf("  Cluster Members: %d\n", len(status["cluster_members"].([]interface{})))
			}
		}
	}

	// Check metrics endpoint
	fmt.Print("Checking metrics... ")
	resp, err = http.Get(nodeURL + "/metrics")
	if err != nil {
		fmt.Println("FAILED")
		fmt.Printf("  Error: %v\n", err)
	} else {
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			fmt.Println("FAILED")
			fmt.Printf("  Unexpected status code: %d\n", resp.StatusCode)
		} else {
			fmt.Println("OK")
		}
	}

	// Check new endpoints
	endpoints := []string{"/dht", "/store", "/topics"}
	for _, endpoint := range endpoints {
		fmt.Printf("Checking %s endpoint... ", endpoint)
		resp, err = http.Get(nodeURL + endpoint)
		if err != nil {
			fmt.Println("FAILED")
			fmt.Printf("  Error: %v\n", err)
		} else {
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				fmt.Println("FAILED")
				fmt.Printf("  Unexpected status code: %d\n", resp.StatusCode)
			} else {
				fmt.Println("OK")
			}
		}
	}

	return nil
}

func runSubscribe(cmd *cobra.Command, args []string) {
	for _, node := range nodes {
		data := map[string]string{
			"topic": topic,
		}

		jsonData, err := json.Marshal(data)
		if err != nil {
			log.Fatal(err)
		}

		resp, err := http.Post(node+"/subscribe", "application/json", bytes.NewBuffer(jsonData))
		if err != nil {
			log.Fatal(err)
		}
		defer resp.Body.Close()

		var result struct {
			Status string `json:"status"`
			ID     string `json:"id"`
		}

		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			log.Fatal(err)
		}

		fmt.Printf("Subscribed to %s on %s (ID: %s)\n", topic, node, result.ID)
	}
}

func runTests(cmd *cobra.Command, args []string) {
	fmt.Println("Running integration tests...")

	// Test 1: Basic pub/sub
	fmt.Println("\nTest 1: Basic pub/sub")
	if err := testBasicPubSub(); err != nil {
		fmt.Printf("❌ Failed: %v\n", err)
	} else {
		fmt.Println("✅ Passed")
	}

	// Test 2: Message delivery under load
	fmt.Println("\nTest 2: Message delivery under load")
	if err := testLoadDelivery(); err != nil {
		fmt.Printf("❌ Failed: %v\n", err)
	} else {
		fmt.Println("✅ Passed")
	}

	// Test 3: Node failure handling
	fmt.Println("\nTest 3: Node failure recovery")
	if err := testNodeFailure(); err != nil {
		fmt.Printf("❌ Failed: %v\n", err)
	} else {
		fmt.Println("✅ Passed")
	}
}

func testBasicPubSub() error {
	// Subscribe to test topic on all nodes
	for _, node := range nodes {
		data := map[string]string{
			"topic": topic,
		}

		jsonData, err := json.Marshal(data)
		if err != nil {
			return fmt.Errorf("failed to marshal subscribe request: %v", err)
		}

		resp, err := http.Post(node+"/subscribe", "application/json", bytes.NewBuffer(jsonData))
		if err != nil {
			return fmt.Errorf("subscribe failed on %s: %v", node, err)
		}
		defer resp.Body.Close()

		var result struct {
			Status string `json:"status"`
			ID     string `json:"id"`
		}

		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			return fmt.Errorf("failed to decode subscribe response: %v", err)
		}

		fmt.Printf("Subscribed to %s on %s (ID: %s)\n", topic, node, result.ID)
	}

	// Wait for subscriptions to propagate
	time.Sleep(2 * time.Second)

	// Publish test message
	testPayload := fmt.Sprintf("test-message-%d", time.Now().UnixNano())
	node := nodes[0] // Use first node for publishing

	if err := publish(node, topic, testPayload); err != nil {
		return fmt.Errorf("initial publish failed: %v", err)
	}

	// Wait for message to propagate
	time.Sleep(2 * time.Second)

	// Verify message delivery by checking metrics on all nodes
	for _, node := range nodes {
		resp, err := http.Get(node + "/metrics")
		if err != nil {
			return fmt.Errorf("failed to get metrics from %s: %v", node, err)
		}

		var metrics struct {
			Router struct {
				MessagesRouted int64 `json:"messages_routed"`
				MessagesSent   int64 `json:"messages_sent"`
			} `json:"router"`
			Store struct {
				MessagesStored   int64 `json:"messages_stored"`
				MessagesReceived int64 `json:"messages_received"`
			} `json:"store"`
		}

		if err := json.NewDecoder(resp.Body).Decode(&metrics); err != nil {
			return fmt.Errorf("failed to decode metrics from %s: %v", node, err)
		}

		fmt.Printf("Metrics for %s:\n", node)
		fmt.Printf("  Messages Routed: %d\n", metrics.Router.MessagesRouted)
		fmt.Printf("  Messages Sent: %d\n", metrics.Router.MessagesSent)
		fmt.Printf("  Messages Stored: %d\n", metrics.Store.MessagesStored)
		fmt.Printf("  Messages Received: %d\n", metrics.Store.MessagesReceived)
	}

	return nil
}

func testLoadDelivery() error {
	// Subscribe to test topic on all nodes
	for _, node := range nodes {
		data := map[string]string{
			"topic": topic,
		}

		jsonData, err := json.Marshal(data)
		if err != nil {
			return fmt.Errorf("failed to marshal subscribe request: %v", err)
		}

		resp, err := http.Post(node+"/subscribe", "application/json", bytes.NewBuffer(jsonData))
		if err != nil {
			return fmt.Errorf("subscribe failed on %s: %v", node, err)
		}
		defer resp.Body.Close()

		var result struct {
			Status string `json:"status"`
			ID     string `json:"id"`
		}

		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			return fmt.Errorf("failed to decode subscribe response: %v", err)
		}

		fmt.Printf("Subscribed to %s on %s (ID: %s)\n", topic, node, result.ID)
	}

	// Wait for subscriptions to propagate
	time.Sleep(2 * time.Second)

	// Publish test messages
	testPayload := fmt.Sprintf("test-message-%d", time.Now().UnixNano())
	for _, node := range nodes {
		if err := publish(node, topic, testPayload); err != nil {
			return fmt.Errorf("failed to publish to %s: %v", node, err)
		}
	}

	// Wait for messages to propagate
	time.Sleep(2 * time.Second)

	// Verify message delivery by checking metrics on all nodes
	for _, node := range nodes {
		resp, err := http.Get(node + "/metrics")
		if err != nil {
			return fmt.Errorf("failed to get metrics from %s: %v", node, err)
		}

		var metrics struct {
			Router struct {
				MessagesRouted int64 `json:"messages_routed"`
				MessagesSent   int64 `json:"messages_sent"`
			} `json:"router"`
			Store struct {
				MessagesStored   int64 `json:"messages_stored"`
				MessagesReceived int64 `json:"messages_received"`
			} `json:"store"`
		}

		if err := json.NewDecoder(resp.Body).Decode(&metrics); err != nil {
			return fmt.Errorf("failed to decode metrics from %s: %v", node, err)
		}

		fmt.Printf("Metrics for %s:\n", node)
		fmt.Printf("  Messages Routed: %d\n", metrics.Router.MessagesRouted)
		fmt.Printf("  Messages Sent: %d\n", metrics.Router.MessagesSent)
		fmt.Printf("  Messages Stored: %d\n", metrics.Store.MessagesStored)
		fmt.Printf("  Messages Received: %d\n", metrics.Store.MessagesReceived)
	}

	return nil
}

func testNodeFailure() error {
	// Subscribe to test topic on all nodes
	for _, node := range nodes {
		data := map[string]string{
			"topic": topic,
		}

		jsonData, err := json.Marshal(data)
		if err != nil {
			return fmt.Errorf("failed to marshal subscribe request: %v", err)
		}

		resp, err := http.Post(node+"/subscribe", "application/json", bytes.NewBuffer(jsonData))
		if err != nil {
			return fmt.Errorf("subscribe failed on %s: %v", node, err)
		}
		defer resp.Body.Close()

		var result struct {
			Status string `json:"status"`
			ID     string `json:"id"`
		}

		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			return fmt.Errorf("failed to decode subscribe response: %v", err)
		}

		fmt.Printf("Subscribed to %s on %s (ID: %s)\n", topic, node, result.ID)
	}

	// Wait for subscriptions to propagate
	time.Sleep(2 * time.Second)

	// Publish test message
	testPayload := fmt.Sprintf("test-message-%d", time.Now().UnixNano())
	node := nodes[0] // Use first node for publishing

	if err := publish(node, topic, testPayload); err != nil {
		return fmt.Errorf("initial publish failed: %v", err)
	}

	// Wait for message to propagate
	time.Sleep(2 * time.Second)

	// Simulate node failure
	fmt.Printf("Simulating node failure: %s\n", node)
	nodes = append(nodes[:0], nodes[1:]...)

	// Wait for message to be rerouted
	time.Sleep(2 * time.Second)

	// Verify message delivery by checking metrics on remaining nodes
	for _, node := range nodes {
		resp, err := http.Get(node + "/metrics")
		if err != nil {
			return fmt.Errorf("failed to get metrics from %s: %v", node, err)
		}

		var metrics struct {
			Router struct {
				MessagesRouted int64 `json:"messages_routed"`
				MessagesSent   int64 `json:"messages_sent"`
			} `json:"router"`
			Store struct {
				MessagesStored   int64 `json:"messages_stored"`
				MessagesReceived int64 `json:"messages_received"`
			} `json:"store"`
		}

		if err := json.NewDecoder(resp.Body).Decode(&metrics); err != nil {
			return fmt.Errorf("failed to decode metrics from %s: %v", node, err)
		}

		fmt.Printf("Metrics for %s:\n", node)
		fmt.Printf("  Messages Routed: %d\n", metrics.Router.MessagesRouted)
		fmt.Printf("  Messages Sent: %d\n", metrics.Router.MessagesSent)
		fmt.Printf("  Messages Stored: %d\n", metrics.Store.MessagesStored)
		fmt.Printf("  Messages Received: %d\n", metrics.Store.MessagesReceived)
	}

	return nil
}
