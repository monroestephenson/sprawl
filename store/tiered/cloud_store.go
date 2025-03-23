package tiered

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
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
	"github.com/aws/smithy-go"
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

	// Added new metrics for retries and errors
	retryAttempts    uint64
	retrySuccess     uint64
	networkErrors    uint64
	throttlingEvents uint64
	latencySum       uint64 // For calculating average latency
	latencyCount     uint64

	// Cloud provider specific metrics
	providerName string
	regionName   string
	cacheHits    uint64
	cacheMisses  uint64
}

// CloudProviderType identifies the detected cloud provider
type CloudProviderType string

const (
	ProviderAWS    CloudProviderType = "aws"
	ProviderGCP    CloudProviderType = "gcp"
	ProviderAzure  CloudProviderType = "azure"
	ProviderMinIO  CloudProviderType = "minio"
	ProviderCustom CloudProviderType = "custom"
)

// CloudProvider encapsulates provider-specific optimizations and detection
type CloudProvider struct {
	Type              CloudProviderType
	Name              string
	Region            string
	IsCommercial      bool
	IsLocalhost       bool
	SupportsMultipart bool
	MaxPartSize       int64
	EndpointFormat    string
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

	// Detect cloud provider for optimizations
	provider := detectCloudProvider(cfg.Endpoint, cfg.Region)
	log.Printf("[CloudStore] Detected cloud provider: %s in region %s", provider.Name, provider.Region)

	// Create AWS config with provider-specific optimizations
	var awsCfg aws.Config
	var err error

	if provider.IsLocalhost {
		// Local MinIO/Custom S3 setup
		customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			return aws.Endpoint{
				URL:               "http://" + cfg.Endpoint,
				SigningRegion:     cfg.Region,
				HostnameImmutable: true,
			}, nil
		})

		awsCfg, err = config.LoadDefaultConfig(context.Background(),
			config.WithRegion(cfg.Region),
			config.WithEndpointResolverWithOptions(customResolver),
			config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, "")),
		)
	} else {
		// Commercial cloud provider - use standard config loading with region
		awsCfg, err = config.LoadDefaultConfig(context.Background(),
			config.WithRegion(cfg.Region),
			config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, "")),
		)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Create S3 client with path-style addressing for MinIO, standard for AWS S3
	clientOptions := []func(*s3.Options){}

	// Apply provider-specific client options
	if provider.Type == ProviderMinIO || provider.Type == ProviderCustom {
		clientOptions = append(clientOptions, func(o *s3.Options) {
			o.UsePathStyle = true
		})
	}

	client := s3.NewFromConfig(awsCfg, clientOptions...)

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

	// Create cloud metrics with provider info
	metrics := &CloudMetrics{
		providerName: string(provider.Type),
		regionName:   provider.Region,
	}

	// Create a new LRU cache for ID to object mappings
	// Use a reasonable default size if not specified
	cacheSize := 10000
	cache := newSimpleLRU(cacheSize)

	// Create the CloudStore instance
	cs := &CloudStore{
		client:  client,
		config:  cfg,
		metrics: metrics,
		batch: &messageBatch{
			messages: make([]Message, 0, cfg.BatchSize),
		},
		uploadCh:   make(chan []Message, cfg.UploadWorkers),
		doneCh:     make(chan struct{}),
		idMappings: &idMappingCache{cache: cache},
		topicIndex: &topicIndex{
			topics: make(map[string]struct{}),
		},
	}

	// Load persisted mappings if enabled
	if cfg.EnablePersistentIndex {
		if err := cs.loadMappingsFromDisk(); err != nil {
			log.Printf("Warning: Failed to load persisted ID mappings: %v", err)
			// Continue anyway, this is not a fatal error
		}
	}

	// Start background workers
	for i := 0; i < cfg.UploadWorkers; i++ {
		go cs.uploadWorker()
	}
	go cs.batchUploader()

	// If persistent index is enabled, start a background task to periodically persist mappings
	if cfg.EnablePersistentIndex {
		go cs.StartPeriodicPersistence(5 * time.Minute)
	}

	return cs, nil
}

// detectCloudProvider attempts to determine which cloud provider is being used
func detectCloudProvider(endpoint, region string) CloudProvider {
	provider := CloudProvider{
		Type:              ProviderCustom,
		Name:              "Custom S3-compatible",
		Region:            region,
		IsCommercial:      false,
		IsLocalhost:       false,
		SupportsMultipart: true,
		MaxPartSize:       5 * 1024 * 1024 * 1024, // 5GB default
		EndpointFormat:    "%s.s3.%s.amazonaws.com",
	}

	// Check for localhost/custom endpoints
	if strings.Contains(endpoint, "localhost") ||
		strings.Contains(endpoint, "127.0.0.1") ||
		strings.Contains(endpoint, "0.0.0.0") {
		provider.Type = ProviderMinIO
		provider.Name = "MinIO (local)"
		provider.IsLocalhost = true
		provider.MaxPartSize = 5 * 1024 * 1024 * 1024 // 5GB
		return provider
	}

	// Check for standard AWS domains
	if strings.HasSuffix(endpoint, "amazonaws.com") {
		provider.Type = ProviderAWS
		provider.Name = "Amazon S3"
		provider.IsCommercial = true
		provider.SupportsMultipart = true
		provider.MaxPartSize = 5 * 1024 * 1024 * 1024 // 5GB
		return provider
	}

	// Check for GCP
	if strings.Contains(endpoint, "storage.googleapis.com") {
		provider.Type = ProviderGCP
		provider.Name = "Google Cloud Storage"
		provider.IsCommercial = true
		provider.SupportsMultipart = true
		provider.MaxPartSize = 5 * 1024 * 1024 * 1024 // 5GB
		provider.EndpointFormat = "storage.googleapis.com/%s"
		return provider
	}

	// Check for Azure
	if strings.Contains(endpoint, "blob.core.windows.net") {
		provider.Type = ProviderAzure
		provider.Name = "Azure Blob Storage"
		provider.IsCommercial = true
		provider.SupportsMultipart = true
		provider.MaxPartSize = 4 * 1024 * 1024 * 1024 // 4GB for Azure
		provider.EndpointFormat = "%s.blob.core.windows.net"
		return provider
	}

	// Check for MinIO in non-localhost mode
	if strings.Contains(endpoint, "minio") {
		provider.Type = ProviderMinIO
		provider.Name = "MinIO"
		provider.SupportsMultipart = true
		return provider
	}

	return provider
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
	// The simpleLRU implementation already handles LRU eviction when over capacity
	// so we can simply add the mapping and it will automatically evict the oldest
	// entry if needed
	cs.idMappings.cache.Add(id, objKey)

	// If persistent index is enabled, schedule a write to disk
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

	// Rename temp file to actual file (atomic operation)
	if err := os.Rename(tempPath, mappingsPath); err != nil {
		log.Printf("Error renaming ID mappings file: %v", err)
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

// GetMetrics returns detailed metrics about the cloud store
func (cs *CloudStore) GetMetrics() map[string]interface{} {
	metrics := make(map[string]interface{})

	// Basic operation metrics
	metrics["messages_uploaded"] = atomic.LoadUint64(&cs.metrics.messagesUploaded)
	metrics["messages_fetched"] = atomic.LoadUint64(&cs.metrics.messagesFetched)
	metrics["batches_uploaded"] = atomic.LoadUint64(&cs.metrics.batchesUploaded)
	metrics["upload_errors"] = atomic.LoadUint64(&cs.metrics.uploadErrors)
	metrics["total_bytes_uploaded"] = atomic.LoadUint64(&cs.metrics.totalBytesUploaded)

	// Reliability metrics
	metrics["retry_attempts"] = atomic.LoadUint64(&cs.metrics.retryAttempts)
	metrics["retry_success"] = atomic.LoadUint64(&cs.metrics.retrySuccess)
	metrics["network_errors"] = atomic.LoadUint64(&cs.metrics.networkErrors)
	metrics["throttling_events"] = atomic.LoadUint64(&cs.metrics.throttlingEvents)

	// Current state
	metrics["current_batch_size"] = atomic.LoadUint64(&cs.metrics.currentBatchSize)

	// Latency calculation
	latencySum := atomic.LoadUint64(&cs.metrics.latencySum)
	latencyCount := atomic.LoadUint64(&cs.metrics.latencyCount)
	var avgLatency uint64 = 0
	if latencyCount > 0 {
		avgLatency = latencySum / latencyCount
	}
	metrics["average_latency_ms"] = avgLatency

	// Cache metrics
	metrics["cache_hits"] = atomic.LoadUint64(&cs.metrics.cacheHits)
	metrics["cache_misses"] = atomic.LoadUint64(&cs.metrics.cacheMisses)

	// Provider info
	metrics["provider"] = cs.metrics.providerName
	metrics["region"] = cs.metrics.regionName

	return metrics
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

	// Persist mappings one last time if enabled
	if cs.config.EnablePersistentIndex {
		cs.persistMappingsToDisk()
	}

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
		Bucket:    aws.String(cs.config.Bucket),
		Delimiter: aws.String("/"),
	}

	// Keep track of unique topics
	topicsMap := make(map[string]struct{})

	// Paginate through all results
	paginator := s3.NewListObjectsV2Paginator(cs.client, input)
	for paginator.HasMorePages() {
		output, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("error listing objects: %w", err)
		}

		// Common prefixes are the "directories" (topics)
		for _, prefix := range output.CommonPrefixes {
			if prefix.Prefix == nil {
				continue
			}

			// Remove trailing slash and extract the topic name
			topicPath := strings.TrimSuffix(*prefix.Prefix, "/")
			if topicPath == "" {
				continue
			}

			// Add to the topics set
			topicsMap[topicPath] = struct{}{}
		}

		// Also check objects directly for topics
		for _, obj := range output.Contents {
			if obj.Key == nil {
				continue
			}

			// Extract topic from key using the first segment
			parts := strings.SplitN(*obj.Key, "/", 2)
			if len(parts) > 0 && parts[0] != "" {
				topicsMap[parts[0]] = struct{}{}
			}
		}
	}

	// Convert map to slice
	topics := make([]string, 0, len(topicsMap))
	for topic := range topicsMap {
		topics = append(topics, topic)
	}

	// Update our in-memory index with discovered topics
	// This prevents having to scan the bucket again next time
	cs.topicIndex.mu.Lock()
	for topic := range topicsMap {
		cs.topicIndex.topics[topic] = struct{}{}
	}
	cs.topicIndex.mu.Unlock()

	return topics, nil
}

// retry is a helper function that implements exponential backoff for S3 operations
func (cs *CloudStore) retry(operation string, f func() error) error {
	backoff := 100 * time.Millisecond
	maxBackoff := 10 * time.Second
	maxRetries := 5
	retries := 0

	var err error
	for retries < maxRetries {
		startTime := time.Now()
		err = f()
		duration := time.Since(startTime)

		// Track operation latency
		atomic.AddUint64(&cs.metrics.latencySum, uint64(duration.Milliseconds()))
		atomic.AddUint64(&cs.metrics.latencyCount, 1)

		if err == nil {
			return nil
		}

		// Check if the error is retryable
		if !isRetryableError(err) {
			log.Printf("[CloudStore] Non-retryable error in %s: %v", operation, err)
			return err
		}

		// Track retry metrics
		atomic.AddUint64(&cs.metrics.retryAttempts, 1)

		// Check for specific error types for metrics
		if strings.Contains(strings.ToLower(err.Error()), "timeout") ||
			strings.Contains(strings.ToLower(err.Error()), "connection") {
			atomic.AddUint64(&cs.metrics.networkErrors, 1)
		}

		if strings.Contains(strings.ToLower(err.Error()), "throttl") ||
			strings.Contains(strings.ToLower(err.Error()), "too many requests") ||
			strings.Contains(strings.ToLower(err.Error()), "rate exceeded") {
			atomic.AddUint64(&cs.metrics.throttlingEvents, 1)
		}

		// Log the retry
		retries++
		if retries < maxRetries {
			log.Printf("[CloudStore] Retrying %s after error (attempt %d/%d): %v",
				operation, retries, maxRetries, err)

			// Apply jitter to backoff to avoid thundering herd
			jitter := time.Duration(rand.Int63n(int64(backoff) / 2))
			sleepTime := backoff + jitter
			time.Sleep(sleepTime)

			// Increase backoff for next retry
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}

	log.Printf("[CloudStore] Failed %s after %d retries: %v", operation, maxRetries, err)
	return fmt.Errorf("operation %s failed after %d retries: %w", operation, maxRetries, err)
}

// isRetryableError determines if an error is retryable
func isRetryableError(err error) bool {
	if err == nil {
		return false
	}

	// Check for network errors that are typically temporary
	if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		return true
	}

	// Extract AWS S3 error code if possible
	var awsErr smithy.APIError
	if errors.As(err, &awsErr) {
		// Check if the error code indicates a retryable condition
		code := awsErr.ErrorCode()

		// Check if the error code indicates a retryable condition
		retryableCodes := []string{
			"RequestTimeout", "RequestTimeTooSkewed", "InternalError",
			"ServiceUnavailable", "ThrottlingException", "ProvisionedThroughputExceeded",
			"RequestLimitExceeded", "TooManyRequests", "SlowDown",
		}

		for _, retryableCode := range retryableCodes {
			if code == retryableCode {
				return true
			}
		}
	}

	// Check error message for known patterns
	errMsg := err.Error()
	retryablePatterns := []string{
		"timeout", "connection reset", "connection refused", "no such host",
		"network", "operation timed out", "deadline exceeded", "connection closed",
		"EOF", "broken pipe", "canceled", "503", "5xx", "429",
		"status code: 5", "status code: 429",
	}

	for _, pattern := range retryablePatterns {
		if strings.Contains(strings.ToLower(errMsg), pattern) {
			return true
		}
	}

	return false
}

// FetchObject fetches an object from S3/MinIO
func (cs *CloudStore) FetchObject(objKey string) ([]byte, error) {
	return cs.fetchObjectWithRetry(objKey)
}

// fetchObjectWithRetry fetches an object from S3/MinIO with retries
func (cs *CloudStore) fetchObjectWithRetry(objKey string) ([]byte, error) {
	var data []byte

	err := cs.retry("FetchObject", func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// Get object
		resp, err := cs.client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(cs.config.Bucket),
			Key:    aws.String(objKey),
		})
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		// Read response body
		data, err = io.ReadAll(resp.Body)
		return err
	})

	return data, err
}
