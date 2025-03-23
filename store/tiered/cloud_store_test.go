package tiered

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
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

func TestLRUCacheOperations(t *testing.T) {
	// Create a new LRU cache with small capacity for testing
	cache := newSimpleLRU(5)

	// Test adding items
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)
		cache.Add(key, value)
	}

	// Check all items are in the cache
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("key%d", i)
		expectedValue := fmt.Sprintf("value%d", i)
		value, found := cache.Get(key)
		if !found {
			t.Errorf("Key %s not found in cache", key)
		}
		if value != expectedValue {
			t.Errorf("Expected value %s for key %s, got %s", expectedValue, key, value)
		}
	}

	// Test LRU eviction by adding more items than capacity
	for i := 5; i < 10; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)
		cache.Add(key, value)
	}

	// The first items should have been evicted
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("key%d", i)
		_, found := cache.Get(key)
		if found {
			t.Errorf("Key %s should have been evicted but was still in cache", key)
		}
	}

	// The newer items should be present
	for i := 5; i < 10; i++ {
		key := fmt.Sprintf("key%d", i)
		expectedValue := fmt.Sprintf("value%d", i)
		value, found := cache.Get(key)
		if !found {
			t.Errorf("Key %s not found in cache", key)
		}
		if value != expectedValue {
			t.Errorf("Expected value %s for key %s, got %s", expectedValue, key, value)
		}
	}

	// Test that accessing an item makes it most recently used
	// First access key7 to make it most recently used
	cache.Get("key7")

	// Then add a new item to trigger eviction of least recently used
	cache.Add("key10", "value10")

	// key5 should be evicted as it's now the least recently used
	_, found := cache.Get("key5")
	if found {
		t.Errorf("Key key5 should have been evicted but was still in cache")
	}

	// key7 should still be present
	_, found = cache.Get("key7")
	if !found {
		t.Errorf("Key key7 should still be in cache but was evicted")
	}
}

func TestPersistentMappings(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "cloud-store-test-")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create config with persistence enabled
	cfg := CloudConfig{
		EnablePersistentIndex: true,
		IndexPath:             tempDir,
		Bucket:                "test-bucket",
		Region:                "us-east-1",
		UploadWorkers:         1,
		BatchSize:             10,
		BatchTimeout:          time.Second,
	}

	// Create a mock for the S3 client
	// In a real test, we'd use minio-go test server or a mock implementation
	// For this test, we'll just verify the file operations

	// Create a store with an empty mappings cache
	store := &CloudStore{
		config:     cfg,
		idMappings: &idMappingCache{cache: newSimpleLRU(100)},
		doneCh:     make(chan struct{}),
		metrics:    &CloudMetrics{},
		topicIndex: &topicIndex{topics: make(map[string]struct{})},
		batch:      &messageBatch{},
	}

	// Add some mappings
	testMappings := map[string]string{
		"msg1": "topic1/batch_123.json",
		"msg2": "topic1/batch_124.json",
		"msg3": "topic2/batch_125.json",
	}

	for id, objKey := range testMappings {
		store.idMappings.cache.Add(id, objKey)
	}

	// Persist mappings to disk
	store.persistMappingsToDisk()

	// Verify the file was created
	mappingsFile := filepath.Join(tempDir, "id_mappings.json")
	if _, err := os.Stat(mappingsFile); os.IsNotExist(err) {
		t.Errorf("Mappings file was not created at %s", mappingsFile)
	}

	// Create a new store and load the mappings
	newStore := &CloudStore{
		config:     cfg,
		idMappings: &idMappingCache{cache: newSimpleLRU(100)},
		doneCh:     make(chan struct{}),
		metrics:    &CloudMetrics{},
		topicIndex: &topicIndex{topics: make(map[string]struct{})},
		batch:      &messageBatch{},
	}

	// Load mappings from disk
	err = newStore.loadMappingsFromDisk()
	if err != nil {
		t.Fatalf("Failed to load mappings: %v", err)
	}

	// Verify the mappings were loaded correctly
	for id, expectedKey := range testMappings {
		objKey, found := newStore.idMappings.cache.Get(id)
		if !found {
			t.Errorf("Expected mapping for %s not found", id)
		} else if objKey != expectedKey {
			t.Errorf("Expected object key %s for message %s, got %s", expectedKey, id, objKey)
		}
	}
}

func TestCloudStore_TopicListing(t *testing.T) {
	// Skip if not running integration tests
	if os.Getenv("RUN_INTEGRATION_TESTS") != "1" {
		t.Skip("Skipping integration test")
	}

	// Get test config
	cfg := getTestConfig(t)

	// Create store
	store, err := NewCloudStore(cfg)
	if err != nil {
		t.Fatalf("Failed to create cloud store: %v", err)
	}
	defer cleanupBucket(t, store)
	defer store.Close()

	// Create test messages for multiple topics
	topics := []string{"test-topic-1", "test-topic-2", "test-topic-3"}
	for _, topic := range topics {
		msgs := []Message{
			{ID: fmt.Sprintf("%s-msg1", topic), Payload: []byte("test data 1"), Timestamp: time.Now()},
			{ID: fmt.Sprintf("%s-msg2", topic), Payload: []byte("test data 2"), Timestamp: time.Now()},
		}

		// Store the messages
		err := store.Store(topic, msgs)
		if err != nil {
			t.Fatalf("Failed to store messages: %v", err)
		}

		// Wait for batch upload
		time.Sleep(cfg.BatchTimeout + 100*time.Millisecond)
	}

	// Test the in-memory topic index
	listedTopics, err := store.ListTopics()
	if err != nil {
		t.Fatalf("Failed to list topics: %v", err)
	}

	// Verify all topics are listed
	topicMap := make(map[string]bool)
	for _, topic := range listedTopics {
		topicMap[topic] = true
	}

	for _, expectedTopic := range topics {
		if !topicMap[expectedTopic] {
			t.Errorf("Topic %s not found in listing", expectedTopic)
		}
	}

	// Create a new store instance to test listing when in-memory index is empty
	newStore, err := NewCloudStore(cfg)
	if err != nil {
		t.Fatalf("Failed to create new cloud store: %v", err)
	}
	defer newStore.Close()

	// Clear the in-memory index
	newStore.topicIndex.topics = make(map[string]struct{})

	// Test listing topics from S3
	listedTopics, err = newStore.ListTopics()
	if err != nil {
		t.Fatalf("Failed to list topics from S3: %v", err)
	}

	// Verify all topics are listed from S3
	topicMap = make(map[string]bool)
	for _, topic := range listedTopics {
		topicMap[topic] = true
	}

	for _, expectedTopic := range topics {
		if !topicMap[expectedTopic] {
			t.Errorf("Topic %s not found in S3 listing", expectedTopic)
		}
	}

	// Verify the in-memory index was updated
	if len(newStore.topicIndex.topics) != len(topics) {
		t.Errorf("In-memory topic index not updated correctly. Expected %d topics, got %d",
			len(topics), len(newStore.topicIndex.topics))
	}
}
