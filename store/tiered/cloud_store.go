package tiered

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
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
	Endpoint        string
	AccessKeyID     string
	SecretAccessKey string
	Bucket          string
	Region          string
	BatchSize       int
	BatchTimeout    time.Duration
	RetentionPeriod time.Duration
	UploadWorkers   int
}

// CloudStore implements cloud storage using S3/MinIO
type CloudStore struct {
	client   *s3.Client
	config   CloudConfig
	metrics  *CloudMetrics
	batch    *messageBatch
	uploadCh chan []Message
	doneCh   chan struct{}
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

	cs := &CloudStore{
		client:   client,
		config:   cfg,
		metrics:  &CloudMetrics{},
		batch:    &messageBatch{},
		uploadCh: make(chan []Message, cfg.UploadWorkers),
		doneCh:   make(chan struct{}),
	}

	// Start upload workers
	for i := 0; i < cfg.UploadWorkers; i++ {
		go cs.uploadWorker()
	}

	// Start batch uploader
	go cs.batchUploader()

	return cs, nil
}

// Store adds a message to the current batch
func (cs *CloudStore) Store(topic string, messages []Message) error {
	if len(messages) == 0 {
		return nil
	}

	fmt.Printf("Storing %d messages for topic %s\n", len(messages), topic)

	// Process messages in batches according to the configured batch size
	for i := 0; i < len(messages); i += cs.config.BatchSize {
		end := i + cs.config.BatchSize
		if end > len(messages) {
			end = len(messages)
		}
		batch := messages[i:end]

		// Create a unique batch ID for this upload
		batchID := fmt.Sprintf("%d", time.Now().UnixNano())
		key := fmt.Sprintf("messages/%s/%s.json", url.PathEscape(topic), batchID)

		// Marshal the batch
		data, err := json.Marshal(batch)
		if err != nil {
			return fmt.Errorf("failed to marshal messages: %w", err)
		}

		fmt.Printf("Uploading batch to %s\n", key)
		// Upload the batch
		_, err = cs.client.PutObject(context.Background(), &s3.PutObjectInput{
			Bucket: aws.String(cs.config.Bucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader(data),
		})
		if err != nil {
			atomic.AddUint64(&cs.metrics.uploadErrors, 1)
			fmt.Printf("Error uploading batch to %s: %v\n", key, err)
			return fmt.Errorf("failed to upload messages: %w", err)
		}

		fmt.Printf("Successfully uploaded batch to %s\n", key)
		// Update metrics
		atomic.AddUint64(&cs.metrics.messagesUploaded, uint64(len(batch)))
		atomic.AddUint64(&cs.metrics.batchesUploaded, 1)
		atomic.AddUint64(&cs.metrics.totalBytesUploaded, uint64(len(data)))
	}

	return nil
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

// Retrieve gets a message from cloud storage
func (cs *CloudStore) Retrieve(id string) (*Message, error) {
	// List objects with the message ID prefix
	output, err := cs.client.ListObjectsV2(context.Background(), &s3.ListObjectsV2Input{
		Bucket: aws.String(cs.config.Bucket),
		Prefix: aws.String("messages/"),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list objects: %w", err)
	}

	// Search through all objects for the message
	for _, obj := range output.Contents {
		if obj.Key == nil {
			continue
		}

		// Get the object
		result, err := cs.client.GetObject(context.Background(), &s3.GetObjectInput{
			Bucket: aws.String(cs.config.Bucket),
			Key:    obj.Key,
		})
		if err != nil {
			continue
		}

		// Read and decode messages
		data, err := io.ReadAll(result.Body)
		result.Body.Close()
		if err != nil {
			continue
		}

		var messages []Message
		if err := json.Unmarshal(data, &messages); err != nil {
			continue
		}

		// Find our message
		for _, msg := range messages {
			if msg.ID == id {
				atomic.AddUint64(&cs.metrics.messagesFetched, 1)
				return &msg, nil
			}
		}
	}

	return nil, fmt.Errorf("message not found: %s", id)
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
