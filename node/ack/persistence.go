package ack

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// Persistence handles durable storage of ACK states
type Persistence struct {
	db        *leveldb.DB
	batchSize int
	mu        sync.Mutex
	path      string
}

// PersistentAckState is a serializable version of AckState
type PersistentAckState struct {
	MessageID    string                    `json:"message_id"`
	Destinations map[string]DeliveryStatus `json:"destinations"`
	CreatedAt    time.Time                 `json:"created_at"`
	LastAttempt  time.Time                 `json:"last_attempt"`
	RetryCount   int                       `json:"retry_count"`
	// Note: Callbacks and CompleteChan are not persisted
}

// NewPersistence creates a new persistence layer with LevelDB
func NewPersistence(path string) (*Persistence, error) {
	// Create directory if it doesn't exist
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, fmt.Errorf("failed to create persistence directory: %w", err)
	}

	// Open LevelDB database
	dbPath := filepath.Join(path, "ack_states")
	db, err := leveldb.OpenFile(dbPath, &opt.Options{
		WriteBuffer:            64 * opt.MiB,
		CompactionTableSize:    32 * opt.MiB,
		CompactionTotalSize:    512 * opt.MiB,
		BlockCacheCapacity:     32 * opt.MiB,
		WriteL0SlowdownTrigger: 16,
		WriteL0PauseTrigger:    64,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open leveldb database: %w", err)
	}

	return &Persistence{
		db:        db,
		batchSize: 1000, // Maximum batch size
		path:      path,
	}, nil
}

// Store persists a single ACK state
func (p *Persistence) Store(state *AckState) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Convert to persistent state
	pState := p.toPersistentState(state)

	// Serialize to JSON
	data, err := json.Marshal(pState)
	if err != nil {
		return fmt.Errorf("failed to marshal ACK state: %w", err)
	}

	// Store in database
	key := []byte(state.MessageID)
	err = p.db.Put(key, data, nil)
	if err != nil {
		return fmt.Errorf("failed to store ACK state: %w", err)
	}

	return nil
}

// StoreAll persists multiple ACK states in a batch
func (p *Persistence) StoreAll(states []*AckState) error {
	if len(states) == 0 {
		return nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// Process in batches to avoid excessive memory usage
	for i := 0; i < len(states); i += p.batchSize {
		end := i + p.batchSize
		if end > len(states) {
			end = len(states)
		}

		batch := new(leveldb.Batch)
		for _, state := range states[i:end] {
			// Convert to persistent state
			pState := p.toPersistentState(state)

			// Serialize to JSON
			data, err := json.Marshal(pState)
			if err != nil {
				return fmt.Errorf("failed to marshal ACK state: %w", err)
			}

			// Add to batch
			key := []byte(state.MessageID)
			batch.Put(key, data)
		}

		// Write batch
		if err := p.db.Write(batch, nil); err != nil {
			return fmt.Errorf("failed to write batch: %w", err)
		}
	}

	return nil
}

// Load retrieves a single ACK state
func (p *Persistence) Load(msgID string) (*AckState, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Read from database
	data, err := p.db.Get([]byte(msgID), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil, nil // Not an error, just not found
		}
		return nil, fmt.Errorf("failed to load ACK state: %w", err)
	}

	// Deserialize from JSON
	var pState PersistentAckState
	if err := json.Unmarshal(data, &pState); err != nil {
		return nil, fmt.Errorf("failed to unmarshal ACK state: %w", err)
	}

	// Convert to AckState
	return p.fromPersistentState(&pState), nil
}

// LoadAll retrieves all persisted ACK states
func (p *Persistence) LoadAll() ([]*AckState, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	var states []*AckState

	// Iterate over all entries
	iter := p.db.NewIterator(nil, nil)
	defer iter.Release()

	for iter.Next() {
		// Deserialize from JSON
		var pState PersistentAckState
		if err := json.Unmarshal(iter.Value(), &pState); err != nil {
			log.Printf("[AckPersistence] Warning: Failed to unmarshal ACK state: %v", err)
			continue
		}

		// Convert to AckState and add to result
		state := p.fromPersistentState(&pState)
		states = append(states, state)
	}

	if err := iter.Error(); err != nil {
		return states, fmt.Errorf("error iterating over ACK states: %w", err)
	}

	return states, nil
}

// Delete removes a single ACK state
func (p *Persistence) Delete(msgID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Delete from database
	err := p.db.Delete([]byte(msgID), nil)
	if err != nil {
		return fmt.Errorf("failed to delete ACK state: %w", err)
	}

	return nil
}

// DeleteBatch removes multiple ACK states
func (p *Persistence) DeleteBatch(msgIDs []string) error {
	if len(msgIDs) == 0 {
		return nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// Process in batches
	for i := 0; i < len(msgIDs); i += p.batchSize {
		end := i + p.batchSize
		if end > len(msgIDs) {
			end = len(msgIDs)
		}

		batch := new(leveldb.Batch)
		for _, msgID := range msgIDs[i:end] {
			batch.Delete([]byte(msgID))
		}

		// Write batch
		if err := p.db.Write(batch, nil); err != nil {
			return fmt.Errorf("failed to write delete batch: %w", err)
		}
	}

	return nil
}

// DeleteOlderThan removes ACK states older than the given timestamp
func (p *Persistence) DeleteOlderThan(threshold time.Time) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	var toDelete []string
	var count int

	// Iterate over all entries
	iter := p.db.NewIterator(nil, nil)
	defer iter.Release()

	for iter.Next() {
		// Deserialize from JSON
		var pState PersistentAckState
		if err := json.Unmarshal(iter.Value(), &pState); err != nil {
			log.Printf("[AckPersistence] Warning: Failed to unmarshal ACK state: %v", err)
			continue
		}

		// Check if older than threshold
		if pState.CreatedAt.Before(threshold) {
			toDelete = append(toDelete, pState.MessageID)
			count++
		}

		// Process in batches to avoid excessive memory usage
		if len(toDelete) >= p.batchSize {
			batch := new(leveldb.Batch)
			for _, msgID := range toDelete {
				batch.Delete([]byte(msgID))
			}

			// Write batch
			if err := p.db.Write(batch, nil); err != nil {
				return count - len(toDelete), fmt.Errorf("failed to write delete batch: %w", err)
			}

			// Clear batch
			toDelete = toDelete[:0]
		}
	}

	// Process any remaining items
	if len(toDelete) > 0 {
		batch := new(leveldb.Batch)
		for _, msgID := range toDelete {
			batch.Delete([]byte(msgID))
		}

		// Write batch
		if err := p.db.Write(batch, nil); err != nil {
			return count - len(toDelete), fmt.Errorf("failed to write final delete batch: %w", err)
		}
	}

	if err := iter.Error(); err != nil {
		return count, fmt.Errorf("error iterating over ACK states: %w", err)
	}

	return count, nil
}

// Compact performs database compaction to reclaim space
func (p *Persistence) Compact() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Compact the entire key range
	err := p.db.CompactRange(util.Range{})
	if err != nil {
		return fmt.Errorf("failed to compact database: %w", err)
	}

	return nil
}

// Close closes the database
func (p *Persistence) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Close database
	err := p.db.Close()
	if err != nil {
		return fmt.Errorf("failed to close database: %w", err)
	}

	return nil
}

// toPersistentState converts an AckState to a PersistentAckState
func (p *Persistence) toPersistentState(state *AckState) *PersistentAckState {
	return &PersistentAckState{
		MessageID:    state.MessageID,
		Destinations: state.Destinations,
		CreatedAt:    state.CreatedAt,
		LastAttempt:  state.LastAttempt,
		RetryCount:   state.RetryCount,
	}
}

// fromPersistentState converts a PersistentAckState to an AckState
func (p *Persistence) fromPersistentState(pState *PersistentAckState) *AckState {
	return &AckState{
		MessageID:    pState.MessageID,
		Destinations: pState.Destinations,
		CreatedAt:    pState.CreatedAt,
		LastAttempt:  pState.LastAttempt,
		RetryCount:   pState.RetryCount,
		CompleteChan: make(chan struct{}),
		Callbacks:    []AckCallback{}, // Initialize empty callbacks
	}
}
