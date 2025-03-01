package tiered

import (
	"context"
	"fmt"
	"os"
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

func TestCloudStore_BasicOperations(t *testing.T) {
	// Skip if no MinIO endpoint is provided
	endpoint := os.Getenv("MINIO_ENDPOINT")
	if endpoint == "" {
		t.Skip("Skipping cloud store test: MINIO_ENDPOINT not set")
	}

	cfg := CloudConfig{
		Endpoint:        endpoint,
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
	endpoint := os.Getenv("MINIO_ENDPOINT")
	if endpoint == "" {
		t.Skip("Skipping cloud store test: MINIO_ENDPOINT not set")
	}

	cfg := CloudConfig{
		Endpoint:        endpoint,
		Bucket:          "test-bucket",
		Region:          "us-east-1",
		BatchSize:       10,
		BatchTimeout:    time.Second,
		RetentionPeriod: 24 * time.Hour,
		UploadWorkers:   4,
	}

	store, err := NewCloudStore(cfg)
	if err != nil {
		t.Fatalf("Failed to create CloudStore: %v", err)
	}
	defer store.Close()

	// Clean up the bucket before test
	cleanupBucket(t, store)

	// Concurrent stores and retrieves
	done := make(chan bool)
	messageCount := 50
	errCh := make(chan error, 2)

	// Store messages
	go func() {
		defer func() { done <- true }()
		batchSize := 10
		for i := 0; i < messageCount; i += batchSize {
			end := i + batchSize
			if end > messageCount {
				end = messageCount
			}
			messages := make([]Message, end-i)
			for j := i; j < end; j++ {
				messages[j-i] = Message{
					ID:        fmt.Sprintf("msg-%d", j),
					Topic:     "test-topic",
					Payload:   []byte("test message"),
					Timestamp: time.Now(),
					TTL:       3,
				}
			}
			if err := store.Store("test-topic", messages); err != nil {
				errCh <- err
				return
			}
		}
	}()

	// Retrieve messages
	go func() {
		defer func() { done <- true }()
		time.Sleep(time.Second) // Give some time for messages to be uploaded
		for i := 0; i < messageCount; i++ {
			if _, err := store.GetTopicMessages("test-topic"); err != nil {
				errCh <- err
				return
			}
		}
	}()

	// Wait for both goroutines
	<-done
	<-done

	// Check for any errors
	select {
	case err := <-errCh:
		t.Error(err)
	default:
	}

	// Wait for final batches to be uploaded
	time.Sleep(2 * time.Second)

	// Verify final state
	messages, err := store.GetTopicMessages("test-topic")
	if err != nil {
		t.Errorf("Failed to get topic messages: %v", err)
	}

	if len(messages) != messageCount {
		t.Errorf("Expected %d messages, got %d", messageCount, len(messages))
	}

	// Verify all messages are unique
	seen := make(map[string]bool)
	for _, msg := range messages {
		if seen[msg.ID] {
			t.Errorf("Duplicate message ID found: %s", msg.ID)
		}
		seen[msg.ID] = true
	}

	// Check metrics
	metrics := store.GetMetrics()
	if metrics["messages_uploaded"].(uint64) != uint64(messageCount) {
		t.Errorf("Expected %d messages uploaded", messageCount)
	}
	if metrics["upload_errors"].(uint64) != 0 {
		t.Error("Expected no upload errors")
	}
}
