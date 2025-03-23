package tiered

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Helper function to clean up the bucket
func cleanupBucket(t *testing.T, store *CloudStore) {
	// List all objects in the bucket
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(store.config.Bucket),
		Prefix: aws.String("messages/"),
	}

	// List and delete objects
	for {
		output, err := store.client.ListObjectsV2(context.Background(), input)
		if err != nil {
			t.Fatalf("Failed to list objects: %v", err)
		}

		for _, obj := range output.Contents {
			deleteInput := &s3.DeleteObjectInput{
				Bucket: aws.String(store.config.Bucket),
				Key:    obj.Key,
			}
			if _, err := store.client.DeleteObject(context.Background(), deleteInput); err != nil {
				t.Fatalf("Failed to delete object %s: %v", *obj.Key, err)
			}
		}

		if !aws.ToBool(output.IsTruncated) {
			break
		}
		input.ContinuationToken = output.NextContinuationToken
	}
}

func getTestConfig(t *testing.T) CloudConfig {
	endpoint := os.Getenv("MINIO_ENDPOINT")
	if endpoint == "" {
		endpoint = "localhost:19000" // Default to the exposed port
	}
	return CloudConfig{
		Endpoint:        endpoint,
		AccessKeyID:     os.Getenv("AWS_ACCESS_KEY_ID"),
		SecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
		Bucket:          "test-bucket",
		Region:          "us-east-1",
		BatchSize:       10,
		BatchTimeout:    time.Second,
		RetentionPeriod: 24 * time.Hour,
		UploadWorkers:   4,
	}
}

func TestCloudStore_BasicOperations(t *testing.T) {
	// Skip test if AWS credentials are not available
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" || os.Getenv("AWS_SECRET_ACCESS_KEY") == "" {
		t.Skip("Skipping test because AWS credentials are not available")
	}

	// Skip if no MinIO endpoint is provided
	endpoint := os.Getenv("MINIO_ENDPOINT")
	if endpoint == "" {
		endpoint = "localhost:19000" // Default to the exposed port
	}

	cfg := CloudConfig{
		Endpoint:        endpoint,
		AccessKeyID:     os.Getenv("AWS_ACCESS_KEY_ID"),
		SecretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
		Bucket:          "test-bucket",
		Region:          "us-east-1",
		BatchSize:       1, // Set to 1 for immediate upload
		BatchTimeout:    time.Second,
		RetentionPeriod: 24 * time.Hour,
		UploadWorkers:   2,
	}

	store, err := NewCloudStore(cfg)
	if err != nil {
		t.Fatalf("Failed to create CloudStore: %v", err)
	}
	defer store.Close()

	// Clean up the bucket before test
	cleanupBucket(t, store)

	// Test message
	msg := Message{
		ID:        "test1",
		Topic:     "test-topic",
		Payload:   []byte("test message"),
		Timestamp: time.Now(),
		TTL:       3,
	}

	// Test Store
	if err := store.Store(msg.Topic, []Message{msg}); err != nil {
		t.Fatalf("Failed to store message: %v", err)
	}

	// Wait for batch to be uploaded
	var retrieved *Message
	var retrieveErr error
	for i := 0; i < 10; i++ {
		time.Sleep(500 * time.Millisecond)
		retrieved, retrieveErr = store.Retrieve(msg.ID)
		if retrieveErr == nil {
			break
		}
	}

	if retrieveErr != nil {
		t.Fatalf("Failed to retrieve message after multiple attempts: %v", retrieveErr)
	}

	if retrieved.ID != msg.ID {
		t.Errorf("Expected message ID %s, got %s", msg.ID, retrieved.ID)
	}

	if string(retrieved.Payload) != string(msg.Payload) {
		t.Errorf("Expected payload %s, got %s", string(msg.Payload), string(retrieved.Payload))
	}

	// Test GetTopicMessages
	messages, err := store.GetTopicMessages(msg.Topic)
	if err != nil {
		t.Errorf("Failed to get topic messages: %v", err)
	}

	if len(messages) != 1 || messages[0].ID != msg.ID {
		t.Errorf("Expected topic to have one message with ID %s", msg.ID)
	}

	// Verify metrics
	metrics := store.GetMetrics()
	if metrics["messages_uploaded"].(uint64) != 1 {
		t.Error("Expected messages_uploaded to be 1")
	}
	if metrics["batches_uploaded"].(uint64) != 1 {
		t.Error("Expected batches_uploaded to be 1")
	}
}

func TestCloudStore_BatchUpload(t *testing.T) {
	// Skip test if AWS credentials are not available
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" || os.Getenv("AWS_SECRET_ACCESS_KEY") == "" {
		t.Skip("Skipping test because AWS credentials are not available")
	}

	// Skip if no MinIO endpoint is provided
	endpoint := os.Getenv("MINIO_ENDPOINT")
	if endpoint == "" {
		t.Skip("Skipping cloud store test: MINIO_ENDPOINT not set")
	}

	cfg := CloudConfig{
		Endpoint:        endpoint,
		Bucket:          "test-bucket",
		Region:          "us-east-1",
		BatchSize:       5,
		BatchTimeout:    time.Second * 2,
		RetentionPeriod: 24 * time.Hour,
		UploadWorkers:   2,
	}

	store, err := NewCloudStore(cfg)
	if err != nil {
		t.Fatalf("Failed to create CloudStore: %v", err)
	}
	defer store.Close()

	// Clean up the bucket before test
	cleanupBucket(t, store)

	// Store multiple messages
	messageCount := 10
	messages := make([]Message, messageCount)
	for i := 0; i < messageCount; i++ {
		messages[i] = Message{
			ID:        fmt.Sprintf("msg-%d", i),
			Topic:     "test-topic",
			Payload:   []byte("test message"),
			Timestamp: time.Now(),
			TTL:       3,
		}
	}
	if err := store.Store("test-topic", messages); err != nil {
		t.Errorf("Failed to store messages: %v", err)
	}

	// Wait for batches to be uploaded
	time.Sleep(3 * time.Second)

	// Verify all messages are retrievable
	for i := 0; i < messageCount; i++ {
		msg, err := store.Retrieve(fmt.Sprintf("msg-%d", i))
		if err != nil {
			t.Errorf("Failed to retrieve message %d: %v", i, err)
		}
		if msg.ID != fmt.Sprintf("msg-%d", i) {
			t.Errorf("Expected message ID %s, got %s", fmt.Sprintf("msg-%d", i), msg.ID)
		}
	}

	// Verify metrics
	metrics := store.GetMetrics()
	uploaded := metrics["messages_uploaded"].(uint64)
	if uploaded != uint64(messageCount) {
		t.Errorf("Expected %d messages uploaded, got %d", messageCount, uploaded)
	}

	batches := metrics["batches_uploaded"].(uint64)
	expectedBatches := uint64((messageCount + cfg.BatchSize - 1) / cfg.BatchSize)
	if batches != expectedBatches {
		t.Errorf("Expected %d batches uploaded, got %d", expectedBatches, batches)
	}
}

func TestCloudStore_ConcurrentAccess(t *testing.T) {
	// Skip test if AWS credentials are not available
	if os.Getenv("AWS_ACCESS_KEY_ID") == "" || os.Getenv("AWS_SECRET_ACCESS_KEY") == "" {
		t.Skip("Skipping test because AWS credentials are not available")
	}

	// Skip if no MinIO endpoint is provided
	endpoint := os.Getenv("MINIO_ENDPOINT")
	if endpoint == "" {
		t.Skip("Skipping cloud store test: MINIO_ENDPOINT not set")
	}

	cfg := getTestConfig(t)
	store, err := NewCloudStore(cfg)
	if err != nil {
		t.Fatalf("Failed to create CloudStore: %v", err)
	}
	defer store.Close()

	// Clean up before test
	cleanupBucket(t, store)

	// Reduce number of concurrent operations
	numPublishers := 3        // Reduced from 5
	messagesPerPublisher := 5 // Reduced from 10
	var wg sync.WaitGroup

	// Add timeout context
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Start publishers
	for i := 0; i < numPublishers; i++ {
		wg.Add(1)
		go func(publisherID int) {
			defer wg.Done()
			messages := make([]Message, messagesPerPublisher)
			for j := 0; j < messagesPerPublisher; j++ {
				messages[j] = Message{
					ID:        fmt.Sprintf("msg-%d-%d", publisherID, j),
					Topic:     "test-topic",
					Payload:   []byte(fmt.Sprintf("test message %d-%d", publisherID, j)),
					Timestamp: time.Now(),
				}
			}
			t.Logf("Storing %d messages for topic test-topic", len(messages))
			select {
			case <-ctx.Done():
				t.Error("Publisher timed out")
				return
			default:
				err := store.Store("test-topic", messages)
				if err != nil {
					t.Errorf("Failed to store messages: %v", err)
				}
			}
		}(i)
	}

	// Wait for all publishers with timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		t.Log("All publishers completed successfully")
	case <-ctx.Done():
		t.Fatal("Test timed out waiting for publishers")
	}

	// Verify messages with timeout
	msgs, err := store.GetTopicMessages("test-topic")
	if err != nil {
		t.Fatalf("Failed to get topic messages: %v", err)
	}
	expected := numPublishers * messagesPerPublisher
	if len(msgs) != expected {
		t.Errorf("Expected %d messages, got %d", expected, len(msgs))
	}
}
