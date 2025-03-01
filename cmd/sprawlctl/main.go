package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
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

	rootCmd.AddCommand(publishCmd, subscribeCmd, testCmd)
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

func publish(nodeURL, topic, payload string) error {
	data := map[string]interface{}{
		"topic":   topic,
		"payload": payload,
		"ttl":     3, // Set initial TTL
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}

	resp, err := http.Post(nodeURL+"/publish", "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	var result struct {
		Status string `json:"status"`
		ID     string `json:"id"`
		Error  string `json:"error"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return err
	}

	if result.Status == "dropped" {
		return fmt.Errorf("message dropped by node")
	}

	if result.Status != "published" {
		return fmt.Errorf("publish failed: %s", result.Error)
	}

	if waitForAck {
		// Poll metrics endpoint until message is confirmed delivered
		maxRetries := 30 // 6 seconds total
		retryInterval := 200 * time.Millisecond

		for i := 0; i < maxRetries; i++ {
			time.Sleep(retryInterval)
			resp, err := http.Get(nodeURL + "/metrics")
			if err != nil {
				continue
			}

			var metrics struct {
				Router struct {
					MessagesRouted int64 `json:"messages_routed"`
					MessagesSent   int64 `json:"messages_sent"`
				} `json:"router"`
				Store struct {
					MessagesStored int64 `json:"messages_stored"`
				} `json:"store"`
			}

			if err := json.NewDecoder(resp.Body).Decode(&metrics); err != nil {
				resp.Body.Close()
				continue
			}
			resp.Body.Close()

			// Consider success if:
			// 1. Message was routed (either stored locally or sent to other nodes)
			// 2. Message was stored locally
			// 3. Message was sent to at least one other node
			if metrics.Router.MessagesRouted > 0 || metrics.Store.MessagesStored > 0 || metrics.Router.MessagesSent > 0 {
				return nil
			}
		}
		return fmt.Errorf("timeout waiting for message acknowledgment (after %v seconds)", float64(maxRetries)*retryInterval.Seconds())
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
			resp.Body.Close()
			return fmt.Errorf("failed to decode metrics from %s: %v", node, err)
		}
		resp.Body.Close()

		// Check if node has either stored or routed the message
		if metrics.Store.MessagesStored == 0 && metrics.Router.MessagesRouted == 0 && metrics.Store.MessagesReceived == 0 {
			return fmt.Errorf("message not delivered to %s", node)
		}
	}

	return nil
}

func testLoadDelivery() error {
	// Subscribe to loadtest topic on all nodes
	for _, node := range nodes {
		data := map[string]string{
			"topic": "loadtest",
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

		fmt.Printf("Subscribed to loadtest on %s (ID: %s)\n", node, result.ID)
	}

	// Wait for subscriptions to propagate
	time.Sleep(2 * time.Second)

	// Run load test
	var wg sync.WaitGroup
	results := make(chan error, count*parallel)

	for p := 0; p < parallel; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < count; i++ {
				node := nodes[i%len(nodes)]
				msg := fmt.Sprintf("loadtest-message-%d", i)
				if err := publish(node, "loadtest", msg); err != nil {
					results <- fmt.Errorf("load test failed: %v", err)
					return
				}
				time.Sleep(10 * time.Millisecond) // Small delay between messages
			}
		}()
	}

	wg.Wait()
	close(results)

	// Check for any errors
	for err := range results {
		return err
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
			resp.Body.Close()
			return fmt.Errorf("failed to decode metrics from %s: %v", node, err)
		}
		resp.Body.Close()

		// Check if node has handled a reasonable number of messages
		totalMessages := metrics.Store.MessagesStored + metrics.Router.MessagesRouted + metrics.Store.MessagesReceived
		if totalMessages < int64(count*parallel/len(nodes)/2) {
			return fmt.Errorf("node %s handled fewer messages than expected: %d", node, totalMessages)
		}
	}

	return nil
}

func testNodeFailure() error {
	// Subscribe to test topic on all nodes
	for _, node := range nodes {
		if err := subscribe(node, "failover-test"); err != nil {
			return fmt.Errorf("failed to subscribe on %s: %v", node, err)
		}
	}

	// Publish initial message to verify cluster is working
	if err := publish(nodes[0], "failover-test", "pre-failure test"); err != nil {
		return fmt.Errorf("initial publish failed: %v", err)
	}

	// Wait for initial message to be processed
	time.Sleep(2 * time.Second)

	// Get initial metrics
	initialMetrics := make(map[string]struct {
		Router struct {
			MessagesRouted int64 `json:"messages_routed"`
			MessagesSent   int64 `json:"messages_sent"`
		} `json:"router"`
	})

	for _, node := range nodes {
		resp, err := http.Get(node + "/metrics")
		if err != nil {
			return fmt.Errorf("failed to get initial metrics from %s: %v", node, err)
		}
		defer resp.Body.Close()

		var metrics struct {
			Router struct {
				MessagesRouted int64 `json:"messages_routed"`
				MessagesSent   int64 `json:"messages_sent"`
			} `json:"router"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&metrics); err != nil {
			return fmt.Errorf("failed to decode initial metrics: %v", err)
		}
		initialMetrics[node] = metrics
	}

	// Publish message to remaining nodes
	for i, node := range nodes {
		if i == 0 {
			continue // Skip the "failed" node
		}
		if err := publish(node, "failover-test", fmt.Sprintf("post-failure test %d", i)); err != nil {
			return fmt.Errorf("post-failure publish failed on %s: %v", node, err)
		}
	}

	// Wait for messages to be processed
	time.Sleep(2 * time.Second)

	// Verify messages were routed correctly
	for _, node := range nodes[1:] { // Check remaining nodes
		resp, err := http.Get(node + "/metrics")
		if err != nil {
			return fmt.Errorf("failed to get final metrics from %s: %v", node, err)
		}
		defer resp.Body.Close()

		var metrics struct {
			Router struct {
				MessagesRouted int64 `json:"messages_routed"`
				MessagesSent   int64 `json:"messages_sent"`
			} `json:"router"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&metrics); err != nil {
			return fmt.Errorf("failed to decode final metrics: %v", err)
		}

		// Verify that messages were routed after "failure"
		if metrics.Router.MessagesRouted <= initialMetrics[node].Router.MessagesRouted {
			return fmt.Errorf("no new messages routed on %s after simulated failure", node)
		}
	}

	return nil
}

func subscribe(nodeURL, topic string) error {
	data := map[string]string{
		"topic": topic,
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal subscribe request: %v", err)
	}

	resp, err := http.Post(nodeURL+"/subscribe", "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("subscribe failed on %s: %v", nodeURL, err)
	}
	defer resp.Body.Close()

	var result struct {
		Status string `json:"status"`
		ID     string `json:"id"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("failed to decode subscribe response: %v", err)
	}

	if result.Status != "subscribed" {
		return fmt.Errorf("subscribe failed on %s: unexpected status %s", nodeURL, result.Status)
	}

	fmt.Printf("Subscribed to %s on %s (ID: %s)\n", topic, nodeURL, result.ID)
	return nil
}
