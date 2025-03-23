package tiered

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// CloudConfig holds configuration for cloud storage
type CloudConfig struct {
	Endpoint              string
	AccessKeyID           string
	SecretAccessKey       string
	Bucket                string
	Region                string
	UseSSL                bool
	BatchSize             int
	BatchTimeout          time.Duration
	RetentionPeriod       time.Duration
	UploadWorkers         int
	EnablePersistentIndex bool
	IndexPath             string // Path to store persistent index files
}

// lruNode represents an entry in the LRU cache
type lruNode struct {
	key   string
	value string
	prev  *lruNode
	next  *lruNode
}

// simpleLRU is a simple LRU cache implementation without external dependencies
type simpleLRU struct {
	capacity int
	size     int
	cache    map[string]*lruNode
	head     *lruNode // Most recently used
	tail     *lruNode // Least recently used
	mu       sync.RWMutex
}

// newSimpleLRU creates a new LRU cache with the given capacity
func newSimpleLRU(capacity int) *simpleLRU {
	return &simpleLRU{
		capacity: capacity,
		size:     0,
		cache:    make(map[string]*lruNode, capacity),
		head:     nil,
		tail:     nil,
	}
}

// Get retrieves a value from the cache
func (l *simpleLRU) Get(key string) (string, bool) {
	l.mu.RLock()
	node, found := l.cache[key]
	l.mu.RUnlock()

	if !found {
		return "", false
	}

	// Move to front (mark as most recently used)
	l.mu.Lock()
	l.moveToFront(node)
	l.mu.Unlock()

	return node.value, true
}

// Add adds a new key-value pair to the cache
func (l *simpleLRU) Add(key, value string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Check if key already exists
	if node, found := l.cache[key]; found {
		// Update value and move to front
		node.value = value
		l.moveToFront(node)
		return
	}

	// Create new node
	node := &lruNode{
		key:   key,
		value: value,
	}

	// Add to cache
	l.cache[key] = node

	// If this is the first entry
	if l.head == nil {
		l.head = node
		l.tail = node
		l.size = 1
		return
	}

	// Add to front of list
	node.next = l.head
	l.head.prev = node
	l.head = node
	l.size++

	// If over capacity, remove least recently used
	if l.size > l.capacity {
		// Remove tail
		delete(l.cache, l.tail.key)
		l.tail = l.tail.prev
		if l.tail != nil {
			l.tail.next = nil
		}
		l.size--
	}
}

// moveToFront moves a node to the front of the list (marks it as most recently used)
func (l *simpleLRU) moveToFront(node *lruNode) {
	// Already at front
	if node == l.head {
		return
	}

	// Remove from current position
	if node.prev != nil {
		node.prev.next = node.next
	}
	if node.next != nil {
		node.next.prev = node.prev
	}

	// If it was the tail, update tail
	if node == l.tail {
		l.tail = node.prev
	}

	// Move to front
	node.prev = nil
	node.next = l.head
	l.head.prev = node
	l.head = node
}

// idMappingCache uses LRU cache for ID to object mappings
type idMappingCache struct {
	cache *simpleLRU
}

// CloudStore implements cloud storage using S3/MinIO
type CloudStore struct {
	client     *s3.Client
	config     CloudConfig
	metrics    *CloudMetrics
	batch      *messageBatch
	uploadCh   chan []Message
	doneCh     chan struct{}
	idMappings *idMappingCache
	topicIndex *topicIndex
}

type CloudMetrics struct {
	messagesUploaded   uint64
	messagesFetched    uint64
	batchesUploaded    uint64
	uploadErrors       uint64
	currentBatchSize   uint64
	totalBytesUploaded uint64
}

type messageBatch struct {
	messages []Message
	size     int64
	mu       sync.Mutex
}

type topicIndex struct {
	topics map[string]struct{}
	mu     sync.RWMutex
}

// NewCloudStore creates a new S3/MinIO-backed store
func NewCloudStore(cfg CloudConfig) (*CloudStore, error) {
	// Validate config
	if cfg.Endpoint == "" {
		return nil, fmt.Errorf("endpoint must be set")
	}
	if cfg.AccessKeyID == "" || cfg.SecretAccessKey == "" {
		return nil, fmt.Errorf("AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set")
	}
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("bucket must be set")
	}

	// Create AWS config
	customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		return aws.Endpoint{
			URL:               "http://" + cfg.Endpoint,
			SigningRegion:     cfg.Region,
			HostnameImmutable: true,
		}, nil
	})

	awsCfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(cfg.Region),
		config.WithEndpointResolverWithOptions(customResolver),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, "")),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Create S3 client with path-style addressing
	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	// Ensure bucket exists
	_, err = client.HeadBucket(context.Background(), &s3.HeadBucketInput{
		Bucket: aws.String(cfg.Bucket),
	})
	if err != nil {
		// Create bucket if it doesn't exist
		_, err = client.CreateBucket(context.Background(), &s3.CreateBucketInput{
			Bucket: aws.String(cfg.Bucket),
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create bucket: %w", err)
		}
	}

	// Configure lifecycle policy
	if cfg.RetentionPeriod > 0 {
		_, err = client.PutBucketLifecycleConfiguration(context.Background(), &s3.PutBucketLifecycleConfigurationInput{
			Bucket: aws.String(cfg.Bucket),
			LifecycleConfiguration: &types.BucketLifecycleConfiguration{
				Rules: []types.LifecycleRule{
					{
						Status: types.ExpirationStatusEnabled,
						Expiration: &types.LifecycleExpiration{
							Days: aws.Int32(int32(cfg.RetentionPeriod.Hours() / 24)),
						},
						ID: aws.String("message-expiration"),
					},
				},
			},
		})
		if err != nil {
			return nil, fmt.Errorf("failed to configure lifecycle policy: %w", err)
		}
	}

	// Set a default index path if not provided
	if cfg.EnablePersistentIndex && cfg.IndexPath == "" {
		cfg.IndexPath = "./data/cloud_index"
	}

	// Initialize LRU cache for ID to object mappings
	cache := newSimpleLRU(100000) // 100K entries

	cs := &CloudStore{
		client:     client,
		config:     cfg,
		metrics:    &CloudMetrics{},
		batch:      &messageBatch{messages: make([]Message, 0, cfg.BatchSize)},
		uploadCh:   make(chan []Message, 100),
		doneCh:     make(chan struct{}),
		idMappings: &idMappingCache{cache: cache},
		topicIndex: &topicIndex{
			topics: make(map[string]struct{}),
		},
	}

	// Load existing mappings from disk if persistent index is enabled
	if cfg.EnablePersistentIndex {
		if err := cs.loadMappingsFromDisk(); err != nil {
			// Log but continue - this is non-fatal
			log.Printf("Warning: Failed to load ID mappings from disk: %v", err)
		}
	}

	// Start upload workers
	for i := 0; i < cfg.UploadWorkers; i++ {
		go cs.uploadWorker()
	}

	// Start batch uploader
	go cs.batchUploader()

	// Start periodic persistence if enabled
	if cfg.EnablePersistentIndex {
		cs.StartPeriodicPersistence(5 * time.Minute)
	}

	return cs, nil
}

// Store uploads messages to cloud storage
func (cs *CloudStore) Store(topic string, messages []Message) error {
	if len(messages) == 0 {
		return nil
	}

	// Get the current timestamp for batching
	now := time.Now()
	timePrefix := now.Format("2006-01-02/15-04-05")

	// Create key for the batch
	batchKey := fmt.Sprintf("topics/%s/%s/batch-%d.json", topic, timePrefix, now.UnixNano())

	// Serialize messages to JSON
	data, err := json.Marshal(messages)
	if err != nil {
		atomic.AddUint64(&cs.metrics.uploadErrors, 1)
		return fmt.Errorf("failed to serialize messages: %w", err)
	}

	// Create S3 upload input
	putObjectInput := &s3.PutObjectInput{
		Bucket:      aws.String(cs.config.Bucket),
		Key:         aws.String(batchKey),
		Body:        bytes.NewReader(data),
		ContentType: aws.String("application/json"),
	}

	// Set metadata for easier lookup
	metadata := map[string]string{
		"topic":      topic,
		"count":      fmt.Sprintf("%d", len(messages)),
		"timestamp":  now.Format(time.RFC3339),
		"first_id":   messages[0].ID,
		"last_id":    messages[len(messages)-1].ID,
		"batch_size": fmt.Sprintf("%d", len(data)),
	}
	putObjectInput.Metadata = metadata

	// Perform the S3 upload
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	_, err = cs.client.PutObject(ctx, putObjectInput)
	if err != nil {
		// Log the error and increment error counter
		atomic.AddUint64(&cs.metrics.uploadErrors, 1)
		return fmt.Errorf("failed to upload batch to S3: %w", err)
	}

	// Update metrics
	atomic.AddUint64(&cs.metrics.messagesUploaded, uint64(len(messages)))
	atomic.AddUint64(&cs.metrics.batchesUploaded, 1)
	atomic.AddUint64(&cs.metrics.totalBytesUploaded, uint64(len(data)))

	// Store topic in metadata for faster lookups
	cs.addTopicToIndex(topic)

	// Log success
	fmt.Printf("Successfully uploaded %d messages to cloud storage for topic %s\n",
		len(messages), topic)

	return nil
}

// Retrieve fetches a message from cloud storage by ID
func (cs *CloudStore) Retrieve(id string) (*Message, error) {
	// First check if we have a direct mapping for this ID
	objKey, err := cs.findObjectKeyForMessage(id)
	if err != nil {
		return nil, fmt.Errorf("message not found in cloud storage: %w", err)
	}

	// If we found a specific object key, retrieve the batch and extract the message
	if objKey != "" {
		return cs.retrieveMessageFromBatch(objKey, id)
	}

	// If we don't have a direct mapping, we'll need to search
	// This is expensive, so we want to avoid it if possible
	fmt.Printf("No direct mapping for message %s, searching all topic batches\n", id)

	// Start by listing all topic prefixes
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// List all objects under topics/
	listInput := &s3.ListObjectsV2Input{
		Bucket: aws.String(cs.config.Bucket),
		Prefix: aws.String("topics/"),
	}

	// We'll need to page through results
	paginator := s3.NewListObjectsV2Paginator(cs.client, listInput)

	// Search through each page of results
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list objects: %w", err)
		}

		// Check each object's metadata for our message ID
		for _, obj := range page.Contents {
			// Check if this object might contain our message
			// First, examine metadata (if available)
			headInput := &s3.HeadObjectInput{
				Bucket: aws.String(cs.config.Bucket),
				Key:    obj.Key,
			}

			headOutput, err := cs.client.HeadObject(ctx, headInput)
			if err != nil {
				// Skip this object if we can't get metadata
				continue
			}

			// Check if this batch contains our message based on metadata
			firstID, hasFirst := headOutput.Metadata["first_id"]
			lastID, hasLast := headOutput.Metadata["last_id"]

			if hasFirst && hasLast {
				// Simple lexicographical range check - may need revision for non-UUID IDs
				if id >= firstID && id <= lastID {
					// This batch might contain our message, retrieve it
					return cs.retrieveMessageFromBatch(*obj.Key, id)
				}
			} else {
				// No range info in metadata, we'll need to check the batch contents
				msg, err := cs.retrieveMessageFromBatch(*obj.Key, id)
				if err == nil && msg != nil {
					// Found it! Save the mapping for future lookups
					cs.saveIDToObjectMapping(id, *obj.Key)
					return msg, nil
				}
			}
		}
	}

	// If we got here, we couldn't find the message
	return nil, fmt.Errorf("message %s not found in cloud storage", id)
}

// findObjectKeyForMessage looks up the object key for a message ID
func (cs *CloudStore) findObjectKeyForMessage(id string) (string, error) {
	// First check our ID mapping cache
	objKey, found := cs.idMappings.cache.Get(id)

	if found {
		return objKey, nil
	}

	// If we have a metadata index, search it
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Look for objects with metadata containing this ID
	// We can search for objects where first_id <= id <= last_id
	listInput := &s3.ListObjectsV2Input{
		Bucket: aws.String(cs.config.Bucket),
		Prefix: aws.String("topics/"),
	}

	result, err := cs.client.ListObjectsV2(ctx, listInput)
	if err != nil {
		return "", fmt.Errorf("failed to list objects: %w", err)
	}

	// Check each object's metadata for ID range
	for _, obj := range result.Contents {
		// Get object metadata
		headInput := &s3.HeadObjectInput{
			Bucket: aws.String(cs.config.Bucket),
			Key:    obj.Key,
		}

		headOutput, err := cs.client.HeadObject(ctx, headInput)
		if err != nil {
			continue // Skip this object if we can't get its metadata
		}

		// Check if this object's metadata indicates it might contain our message
		firstID, hasFirst := headOutput.Metadata["first_id"]
		lastID, hasLast := headOutput.Metadata["last_id"]

		if hasFirst && hasLast {
			// Simple lexicographical range check
			if id >= firstID && id <= lastID {
				// Save this mapping for future lookups
				cs.saveIDToObjectMapping(id, *obj.Key)
				return *obj.Key, nil
			}
		}
	}

	// If we couldn't find it, return empty string
	return "", nil
}

// retrieveMessageFromBatch retrieves and parses a batch file, then extracts a specific message
func (cs *CloudStore) retrieveMessageFromBatch(objKey string, messageID string) (*Message, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Get the object from S3
	getObjectInput := &s3.GetObjectInput{
		Bucket: aws.String(cs.config.Bucket),
		Key:    aws.String(objKey),
	}

	getObjectOutput, err := cs.client.GetObject(ctx, getObjectInput)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve object %s: %w", objKey, err)
	}

	// Read the object data
	data, err := io.ReadAll(getObjectOutput.Body)
	defer getObjectOutput.Body.Close()

	if err != nil {
		return nil, fmt.Errorf("failed to read object data: %w", err)
	}

	// Parse the batch JSON
	var messages []Message
	if err := json.Unmarshal(data, &messages); err != nil {
		return nil, fmt.Errorf("failed to parse batch data: %w", err)
	}

	// Find the specific message in the batch
	for _, msg := range messages {
		if msg.ID == messageID {
			// Update metrics
			atomic.AddUint64(&cs.metrics.messagesFetched, 1)
			return &msg, nil
		}
	}

	return nil, fmt.Errorf("message %s not found in batch %s", messageID, objKey)
}

// saveIDToObjectMapping saves a mapping from message ID to object key for faster lookups
func (cs *CloudStore) saveIDToObjectMapping(id string, objKey string) {
	cs.idMappings.cache.Add(id, objKey)

	// In a production implementation, we would periodically persist this mapping to disk
	// if the persistent index option is enabled
	if cs.config.EnablePersistentIndex {
		go cs.persistMappingsToDisk()
	}
}

// persistMappingsToDisk persists the ID-to-object mappings to disk
func (cs *CloudStore) persistMappingsToDisk() {
	if !cs.config.EnablePersistentIndex {
		return
	}

	// Create the directory if it doesn't exist
	if err := os.MkdirAll(cs.config.IndexPath, 0755); err != nil {
		log.Printf("Error creating index directory: %v", err)
		return
	}

	// Prepare mappings file path
	mappingsPath := filepath.Join(cs.config.IndexPath, "id_mappings.json")
	tempPath := mappingsPath + ".tmp"

	// Get a snapshot of the current mappings
	cs.idMappings.cache.mu.RLock()
	mappings := make(map[string]string, len(cs.idMappings.cache.cache))
	for key, node := range cs.idMappings.cache.cache {
		mappings[key] = node.value
	}
	cs.idMappings.cache.mu.RUnlock()

	// Marshal to JSON
	data, err := json.MarshalIndent(mappings, "", "  ")
	if err != nil {
		log.Printf("Error marshaling ID mappings: %v", err)
		return
	}

	// Write to temp file first
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		log.Printf("Error writing ID mappings to temp file: %v", err)
		return
	}

	// Atomically rename to final file
	if err := os.Rename(tempPath, mappingsPath); err != nil {
		log.Printf("Error renaming temp mappings file: %v", err)
		// Try to clean up
		os.Remove(tempPath)
		return
	}

	log.Printf("Successfully persisted %d ID mappings to disk", len(mappings))
}

// loadMappingsFromDisk loads ID-to-object mappings from disk
func (cs *CloudStore) loadMappingsFromDisk() error {
	if !cs.config.EnablePersistentIndex {
		return nil
	}

	mappingsPath := filepath.Join(cs.config.IndexPath, "id_mappings.json")

	// Check if file exists
	if _, err := os.Stat(mappingsPath); os.IsNotExist(err) {
		// No file yet, not an error
		return nil
	}

	// Read the file
	data, err := os.ReadFile(mappingsPath)
	if err != nil {
		return fmt.Errorf("failed to read mappings file: %w", err)
	}

	// Unmarshal the data
	var mappings map[string]string
	if err := json.Unmarshal(data, &mappings); err != nil {
		return fmt.Errorf("failed to unmarshal mappings: %w", err)
	}

	// Load into cache
	for id, objKey := range mappings {
		cs.idMappings.cache.Add(id, objKey)
	}

	log.Printf("Loaded %d ID mappings from disk", len(mappings))
	return nil
}

// StartPeriodicPersistence starts a goroutine to periodically persist mappings to disk
func (cs *CloudStore) StartPeriodicPersistence(interval time.Duration) {
	if !cs.config.EnablePersistentIndex {
		return
	}

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				cs.persistMappingsToDisk()
			case <-cs.doneCh:
				// Final persistence before shutdown
				cs.persistMappingsToDisk()
				return
			}
		}
	}()
}

// addTopicToIndex records a topic in our internal index for faster topic listing
func (cs *CloudStore) addTopicToIndex(topic string) {
	cs.topicIndex.mu.Lock()
	defer cs.topicIndex.mu.Unlock()

	// Add to our in-memory topic set if not already present
	if _, exists := cs.topicIndex.topics[topic]; !exists {
		cs.topicIndex.topics[topic] = struct{}{}

		// If we're using a persistent index, write this to disk/database
		if cs.config.EnablePersistentIndex {
			// Add metadata object for this topic
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()

			// We create a small metadata object for each topic
			metaKey := fmt.Sprintf("topics/%s/_meta", topic)
			metaContent := []byte(fmt.Sprintf(`{"topic":"%s","created":%d}`,
				topic, time.Now().Unix()))

			_, err := cs.client.PutObject(ctx, &s3.PutObjectInput{
				Bucket:      aws.String(cs.config.Bucket),
				Key:         aws.String(metaKey),
				Body:        bytes.NewReader(metaContent),
				ContentType: aws.String("application/json"),
				Metadata: map[string]string{
					"content-type": "topic-meta",
					"topic":        topic,
				},
			})

			if err != nil {
				log.Printf("Warning: failed to write topic metadata for %s: %v", topic, err)
			}
		}
	}
}

// uploadBatch uploads the current batch to S3/MinIO
func (cs *CloudStore) uploadBatch() error {
	if len(cs.batch.messages) == 0 {
		return nil
	}

	// Prepare batch for upload
	messages := make([]Message, len(cs.batch.messages))
	copy(messages, cs.batch.messages)

	// Reset batch
	cs.batch.messages = nil
	cs.batch.size = 0

	// For small batches, upload directly
	if len(messages) <= 1 {
		return cs.uploadMessages(messages)
	}

	// Send to upload channel
	select {
	case cs.uploadCh <- messages:
		return nil
	default:
		// If channel is full, try direct upload
		return cs.uploadMessages(messages)
	}
}

// uploadWorker handles batch uploads
func (cs *CloudStore) uploadWorker() {
	for messages := range cs.uploadCh {
		if err := cs.uploadMessages(messages); err != nil {
			cs.metrics.uploadErrors++
			// Log error but continue processing
			fmt.Printf("Failed to upload batch: %v\n", err)
		}
	}
}

// uploadMessages uploads a batch of messages to S3/MinIO
func (cs *CloudStore) uploadMessages(messages []Message) error {
	if len(messages) == 0 {
		return nil
	}

	// Group messages by topic
	topicMessages := make(map[string][]Message)
	for _, msg := range messages {
		topicMessages[msg.Topic] = append(topicMessages[msg.Topic], msg)
	}

	// Upload each topic's messages
	for topic, msgs := range topicMessages {
		data, err := json.Marshal(msgs)
		if err != nil {
			return fmt.Errorf("failed to marshal messages: %w", err)
		}

		// Create URL-safe key
		safeTopic := strings.ReplaceAll(topic, "/", "-")
		key := fmt.Sprintf("messages/%s/%d.json", safeTopic, time.Now().UnixNano())

		fmt.Printf("Uploading %d messages to %s\n", len(msgs), key)

		// Upload with retries
		var uploadErr error
		for retries := 0; retries < 3; retries++ {
			_, err = cs.client.PutObject(context.Background(), &s3.PutObjectInput{
				Bucket: aws.String(cs.config.Bucket),
				Key:    aws.String(key),
				Body:   bytes.NewReader(data),
			})
			if err == nil {
				break
			}
			uploadErr = err
			time.Sleep(time.Duration(retries+1) * 100 * time.Millisecond)
		}
		if uploadErr != nil {
			return fmt.Errorf("failed to upload messages after retries: %w", uploadErr)
		}

		// Update metrics
		atomic.AddUint64(&cs.metrics.messagesUploaded, uint64(len(msgs)))
		atomic.AddUint64(&cs.metrics.totalBytesUploaded, uint64(len(data)))
	}

	atomic.AddUint64(&cs.metrics.batchesUploaded, 1)
	return nil
}

// batchUploader periodically uploads partial batches
func (cs *CloudStore) batchUploader() {
	ticker := time.NewTicker(cs.config.BatchTimeout)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			cs.batch.mu.Lock()
			if err := cs.uploadBatch(); err != nil {
				fmt.Printf("Failed to upload batch: %v\n", err)
			}
			cs.batch.mu.Unlock()
		case <-cs.doneCh:
			return
		}
	}
}

// GetTopicMessages gets all message IDs for a topic
func (cs *CloudStore) GetTopicMessages(topic string) ([]Message, error) {
	prefix := fmt.Sprintf("messages/%s/", url.PathEscape(topic))
	fmt.Printf("Looking for objects with prefix: %s\n", prefix)

	// Use a map to track unique messages by ID
	uniqueMessages := make(map[string]Message)

	var continuationToken *string
	for {
		input := &s3.ListObjectsV2Input{
			Bucket: aws.String(cs.config.Bucket),
			Prefix: aws.String(prefix),
		}
		if continuationToken != nil {
			input.ContinuationToken = continuationToken
		}

		output, err := cs.client.ListObjectsV2(context.Background(), input)
		if err != nil {
			return nil, fmt.Errorf("failed to list objects: %w", err)
		}

		fmt.Printf("Found %d objects with prefix %s\n", len(output.Contents), prefix)
		for _, obj := range output.Contents {
			fmt.Printf("Processing object: %s\n", *obj.Key)
			result, err := cs.client.GetObject(context.Background(), &s3.GetObjectInput{
				Bucket: aws.String(cs.config.Bucket),
				Key:    obj.Key,
			})
			if err != nil {
				fmt.Printf("Error getting object %s: %v\n", *obj.Key, err)
				continue
			}

			var messages []Message
			if err := json.NewDecoder(result.Body).Decode(&messages); err != nil {
				fmt.Printf("Error decoding messages from %s: %v\n", *obj.Key, err)
				result.Body.Close()
				continue
			}
			result.Body.Close()

			fmt.Printf("Found %d messages in object %s\n", len(messages), *obj.Key)
			// Add messages to the map, which automatically deduplicates by ID
			for _, msg := range messages {
				uniqueMessages[msg.ID] = msg
				atomic.AddUint64(&cs.metrics.messagesFetched, 1)
			}
		}

		if !aws.ToBool(output.IsTruncated) {
			break
		}
		continuationToken = output.NextContinuationToken
	}

	// Convert map to slice
	result := make([]Message, 0, len(uniqueMessages))
	for _, msg := range uniqueMessages {
		result = append(result, msg)
	}

	fmt.Printf("Found %d unique messages for topic %s\n", len(result), topic)
	return result, nil
}

// GetMetrics returns current metrics
func (cs *CloudStore) GetMetrics() map[string]interface{} {
	return map[string]interface{}{
		"messages_uploaded":    cs.metrics.messagesUploaded,
		"messages_fetched":     cs.metrics.messagesFetched,
		"batches_uploaded":     cs.metrics.batchesUploaded,
		"upload_errors":        cs.metrics.uploadErrors,
		"current_batch_size":   cs.metrics.currentBatchSize,
		"total_bytes_uploaded": cs.metrics.totalBytesUploaded,
	}
}

// Close stops the batch uploader and upload workers
func (cs *CloudStore) Close() error {
	// Upload any remaining messages
	cs.batch.mu.Lock()
	if err := cs.uploadBatch(); err != nil {
		cs.batch.mu.Unlock()
		return fmt.Errorf("failed to upload final batch: %w", err)
	}
	cs.batch.mu.Unlock()

	// Signal workers to stop
	close(cs.doneCh)
	close(cs.uploadCh)

	return nil
}

// ListTopics returns a list of all topics stored in the cloud
func (cs *CloudStore) ListTopics() ([]string, error) {
	// First, check the in-memory topic index
	cs.topicIndex.mu.RLock()
	if len(cs.topicIndex.topics) > 0 {
		topics := make([]string, 0, len(cs.topicIndex.topics))
		for topic := range cs.topicIndex.topics {
			topics = append(topics, topic)
		}
		cs.topicIndex.mu.RUnlock()
		return topics, nil
	}
	cs.topicIndex.mu.RUnlock()

	// If our in-memory index is empty, scan the bucket
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Use the S3 client to list the topics (which are prefixes in our storage scheme)
	// We use the delimiter-based approach to list "directories" (which are our topics)
	input := &s3.ListObjectsV2Input{
		Bucket:    &cs.config.Bucket,
		Delimiter: aws.String("/"),
	}

	// Keep track of unique topics
	topicsMap := make(map[string]struct{})

	// Paginate through all results
	paginator := s3.NewListObjectsV2Paginator(cs.client, input)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list objects in bucket: %w", err)
		}

		// Common prefixes are our "directories" which represent topics
		for _, prefix := range page.CommonPrefixes {
			if prefix.Prefix == nil {
				continue
			}

			// Extract topic name (remove the trailing slash)
			topic := *prefix.Prefix
			if len(topic) > 0 && topic[len(topic)-1] == '/' {
				topic = topic[:len(topic)-1]
			}

			topicsMap[topic] = struct{}{}
		}

		// Also check objects (for the case where we don't have directories)
		for _, obj := range page.Contents {
			if obj.Key == nil {
				continue
			}

			// Extract topic from object key (format: topic/batch_timestamp.json)
			parts := strings.SplitN(*obj.Key, "/", 2)
			if len(parts) > 0 {
				topicsMap[parts[0]] = struct{}{}
			}
		}
	}

	// Convert map to slice
	topics := make([]string, 0, len(topicsMap))
	for topic := range topicsMap {
		topics = append(topics, topic)

		// Also update our in-memory cache
		cs.topicIndex.mu.Lock()
		cs.topicIndex.topics[topic] = struct{}{}
		cs.topicIndex.mu.Unlock()
	}

	return topics, nil
}
