// Package client provides network communication between Sprawl nodes
package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"
)

// NodeClient provides methods to communicate with other nodes in the cluster
type NodeClient struct {
	httpClient *http.Client
	baseURL    string
	timeout    time.Duration
	retries    int
}

// NodeClientOptions configures a new NodeClient
type NodeClientOptions struct {
	Timeout time.Duration
	Retries int
}

// DefaultNodeClientOptions returns the default options for a NodeClient
func DefaultNodeClientOptions() NodeClientOptions {
	return NodeClientOptions{
		Timeout: 5 * time.Second,
		Retries: 3,
	}
}

// NewNodeClient creates a new client for communicating with a node
func NewNodeClient(host string, port int, options NodeClientOptions) *NodeClient {
	// Create HTTP client with timeout
	httpClient := &http.Client{
		Timeout: options.Timeout,
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 20,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	// Construct base URL
	baseURL := fmt.Sprintf("http://%s:%d", host, port)

	return &NodeClient{
		httpClient: httpClient,
		baseURL:    baseURL,
		timeout:    options.Timeout,
		retries:    options.Retries,
	}
}

// GetMetrics retrieves metrics from a remote node
func (c *NodeClient) GetMetrics() (map[string]float64, error) {
	url := fmt.Sprintf("%s/api/metrics", c.baseURL)

	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	// Create request with context
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Set headers
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	// Execute request with retries
	var resp *http.Response
	var lastErr error

	for attempt := 0; attempt <= c.retries; attempt++ {
		if attempt > 0 {
			// Exponential backoff
			backoff := time.Duration(1<<uint(attempt-1)) * 100 * time.Millisecond
			log.Printf("Retrying metrics request in %v (attempt %d/%d)", backoff, attempt, c.retries)
			time.Sleep(backoff)
		}

		resp, err = c.httpClient.Do(req)
		if err == nil {
			break
		}

		lastErr = err
		log.Printf("Request failed: %v", err)
	}

	if resp == nil {
		return nil, fmt.Errorf("all requests failed after %d attempts: %w", c.retries, lastErr)
	}

	defer resp.Body.Close()

	// Check response status
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status code: %d, body: %s", resp.StatusCode, string(body))
	}

	// Parse response
	var metrics map[string]float64
	if err := json.NewDecoder(resp.Body).Decode(&metrics); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return metrics, nil
}

// GetAIRecommendations retrieves AI recommendations from a remote node
func (c *NodeClient) GetAIRecommendations() ([]map[string]interface{}, error) {
	url := fmt.Sprintf("%s/api/ai/recommendations", c.baseURL)

	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	// Create request with context
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Set headers
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	// Execute request with retries
	var resp *http.Response
	var lastErr error

	for attempt := 0; attempt <= c.retries; attempt++ {
		if attempt > 0 {
			// Exponential backoff
			backoff := time.Duration(1<<uint(attempt-1)) * 100 * time.Millisecond
			time.Sleep(backoff)
		}

		resp, err = c.httpClient.Do(req)
		if err == nil {
			break
		}

		lastErr = err
	}

	if resp == nil {
		return nil, fmt.Errorf("all requests failed after %d attempts: %w", c.retries, lastErr)
	}

	defer resp.Body.Close()

	// Check response status
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status code: %d, body: %s", resp.StatusCode, string(body))
	}

	// Parse response
	var recommendations []map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&recommendations); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return recommendations, nil
}

// GetNodeStatus retrieves the status of a remote node
func (c *NodeClient) GetNodeStatus() (map[string]interface{}, error) {
	url := fmt.Sprintf("%s/api/status", c.baseURL)

	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	// Create request with context
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Set headers
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	// Execute request
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	// Check response status
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status code: %d, body: %s", resp.StatusCode, string(body))
	}

	// Parse response
	var status map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return status, nil
}

// Ping checks if a node is reachable
func (c *NodeClient) Ping() error {
	url := fmt.Sprintf("%s/api/health", c.baseURL)

	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	// Create request with context
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	// Execute request
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("ping failed: %w", err)
	}
	defer resp.Body.Close()

	// Check response status
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("ping failed with status: %d", resp.StatusCode)
	}

	return nil
}
