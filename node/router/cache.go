package router

import (
	"container/list"
	"sync"
	"time"

	"sprawl/node/dht"
)

// RouteCache implements a thread-safe LRU cache for topic to node mappings
type RouteCache struct {
	// Maximum number of items in the cache
	capacity int

	// Internal map for O(1) lookups: topic -> cacheEntry
	items map[string]*list.Element

	// List for LRU tracking
	evictList *list.List

	// Mutex for thread safety
	mu sync.RWMutex

	// Metric counters
	hits   int64
	misses int64

	// Default TTL for cache entries
	defaultTTL time.Duration

	// Number of shards for cache partitioning (reduce lock contention)
	shardCount int

	// Sharded caches - if sharding is enabled (shardCount > 1)
	shards []*shardedCache
}

// shardedCache represents a single shard of the cache when sharding is enabled
type shardedCache struct {
	items     map[string]*list.Element
	evictList *list.List
	mu        sync.RWMutex
	capacity  int
}

// cacheEntry represents a single entry in the cache
type cacheEntry struct {
	key       string
	value     []dht.NodeInfo
	expiresAt time.Time
}

// NewRouteCache creates a new LRU cache for topic to node mappings
func NewRouteCache(capacity int, ttl time.Duration, shardCount int) *RouteCache {
	if capacity <= 0 {
		capacity = 10000 // Default capacity
	}
	if ttl <= 0 {
		ttl = 5 * time.Minute // Default TTL
	}
	if shardCount <= 0 {
		shardCount = 1 // No sharding by default
	}

	cache := &RouteCache{
		capacity:   capacity,
		defaultTTL: ttl,
		shardCount: shardCount,
	}

	// Initialize sharded caches if needed
	if shardCount > 1 {
		cache.shards = make([]*shardedCache, shardCount)
		shardCapacity := capacity / shardCount

		// Ensure minimum capacity per shard
		if shardCapacity < 100 {
			shardCapacity = 100
		}

		// Store the per-shard capacity
		cache.capacity = shardCapacity * shardCount

		for i := 0; i < shardCount; i++ {
			cache.shards[i] = &shardedCache{
				items:     make(map[string]*list.Element),
				evictList: list.New(),
				capacity:  shardCapacity,
			}
		}
	} else {
		// Non-sharded cache
		cache.items = make(map[string]*list.Element)
		cache.evictList = list.New()
	}

	return cache
}

// getShard returns the appropriate shard for a key
func (c *RouteCache) getShard(key string) *shardedCache {
	if c.shardCount <= 1 {
		return nil // Not using sharding
	}

	// Simple hash function to distribute keys
	var hash uint32
	for i := 0; i < len(key); i++ {
		hash = 31*hash + uint32(key[i])
	}
	return c.shards[hash%uint32(c.shardCount)]
}

// Get retrieves nodes for a topic from the cache if present and not expired
func (c *RouteCache) Get(topic string) ([]dht.NodeInfo, bool) {
	// Check if we're using sharding
	if c.shardCount > 1 {
		shard := c.getShard(topic)
		return c.getFromShard(shard, topic)
	}

	// Non-sharded implementation
	c.mu.RLock()
	element, found := c.items[topic]
	if !found {
		c.mu.RUnlock()
		c.misses++
		return nil, false
	}

	entry := element.Value.(*cacheEntry)
	// Check if the entry has expired
	if time.Now().After(entry.expiresAt) {
		c.mu.RUnlock()
		// Remove expired entry (need write lock)
		c.mu.Lock()
		c.removeElement(element)
		c.mu.Unlock()
		c.misses++
		return nil, false
	}
	c.mu.RUnlock()

	// Move to front (need write lock)
	c.mu.Lock()
	c.evictList.MoveToFront(element)
	c.mu.Unlock()

	c.hits++
	return entry.value, true
}

// getFromShard retrieves an item from a specific shard
func (c *RouteCache) getFromShard(shard *shardedCache, topic string) ([]dht.NodeInfo, bool) {
	shard.mu.RLock()
	element, found := shard.items[topic]
	if !found {
		shard.mu.RUnlock()
		c.misses++
		return nil, false
	}

	entry := element.Value.(*cacheEntry)
	// Check if the entry has expired
	if time.Now().After(entry.expiresAt) {
		shard.mu.RUnlock()
		// Remove expired entry (need write lock)
		shard.mu.Lock()
		delete(shard.items, topic)
		shard.evictList.Remove(element)
		shard.mu.Unlock()
		c.misses++
		return nil, false
	}
	shard.mu.RUnlock()

	// Move to front (need write lock)
	shard.mu.Lock()
	shard.evictList.MoveToFront(element)
	shard.mu.Unlock()

	c.hits++
	return entry.value, true
}

// Set adds or updates a cache entry with nodes for a topic
func (c *RouteCache) Set(topic string, nodes []dht.NodeInfo) {
	// Check if we're using sharding
	if c.shardCount > 1 {
		shard := c.getShard(topic)
		c.setToShard(shard, topic, nodes)
		return
	}

	// Make a copy of the nodes slice to prevent external modifications
	nodesCopy := make([]dht.NodeInfo, len(nodes))
	copy(nodesCopy, nodes)

	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if the key already exists
	if element, found := c.items[topic]; found {
		// Update existing entry
		c.evictList.MoveToFront(element)
		entry := element.Value.(*cacheEntry)
		entry.value = nodesCopy
		entry.expiresAt = time.Now().Add(c.defaultTTL)
		return
	}

	// Add new entry
	entry := &cacheEntry{
		key:       topic,
		value:     nodesCopy,
		expiresAt: time.Now().Add(c.defaultTTL),
	}
	element := c.evictList.PushFront(entry)
	c.items[topic] = element

	// Check if we need to evict
	if c.evictList.Len() > c.capacity {
		c.removeOldest()
	}
}

// setToShard adds or updates an entry in a specific shard
func (c *RouteCache) setToShard(shard *shardedCache, topic string, nodes []dht.NodeInfo) {
	// Make a copy of the nodes slice to prevent external modifications
	nodesCopy := make([]dht.NodeInfo, len(nodes))
	copy(nodesCopy, nodes)

	shard.mu.Lock()
	defer shard.mu.Unlock()

	// Check if the key already exists
	if element, found := shard.items[topic]; found {
		// Update existing entry
		shard.evictList.MoveToFront(element)
		entry := element.Value.(*cacheEntry)
		entry.value = nodesCopy
		entry.expiresAt = time.Now().Add(c.defaultTTL)
		return
	}

	// Add new entry
	entry := &cacheEntry{
		key:       topic,
		value:     nodesCopy,
		expiresAt: time.Now().Add(c.defaultTTL),
	}
	element := shard.evictList.PushFront(entry)
	shard.items[topic] = element

	// Check if we need to evict
	if shard.evictList.Len() > c.capacity/c.shardCount {
		oldestElement := shard.evictList.Back()
		if oldestElement != nil {
			entry := oldestElement.Value.(*cacheEntry)
			delete(shard.items, entry.key)
			shard.evictList.Remove(oldestElement)
		}
	}
}

// removeOldest removes the oldest entry from the cache
func (c *RouteCache) removeOldest() {
	element := c.evictList.Back()
	if element != nil {
		c.removeElement(element)
	}
}

// removeElement removes a specific element from the cache
func (c *RouteCache) removeElement(e *list.Element) {
	entry := e.Value.(*cacheEntry)
	delete(c.items, entry.key)
	c.evictList.Remove(e)
}

// Remove explicitly removes a topic from the cache
func (c *RouteCache) Remove(topic string) {
	// Check if we're using sharding
	if c.shardCount > 1 {
		shard := c.getShard(topic)
		shard.mu.Lock()
		defer shard.mu.Unlock()

		if element, found := shard.items[topic]; found {
			delete(shard.items, topic)
			shard.evictList.Remove(element)
		}
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if element, found := c.items[topic]; found {
		c.removeElement(element)
	}
}

// RemoveNodeFromAll removes a node from all cache entries that contain it
// Used when a node becomes unavailable
func (c *RouteCache) RemoveNodeFromAll(nodeID string) {
	// Check if we're using sharding
	if c.shardCount > 1 {
		for _, shard := range c.shards {
			c.removeNodeFromShard(shard, nodeID)
		}
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Iterate through all entries
	for element := c.evictList.Front(); element != nil; element = element.Next() {
		entry := element.Value.(*cacheEntry)
		modified := false

		// Filter out the specified node
		filteredNodes := make([]dht.NodeInfo, 0, len(entry.value))
		for _, node := range entry.value {
			if node.ID != nodeID {
				filteredNodes = append(filteredNodes, node)
			} else {
				modified = true
			}
		}

		// If the node was found and removed, update the entry
		if modified {
			if len(filteredNodes) == 0 {
				// If no nodes left, remove the entry entirely
				c.removeElement(element)
			} else {
				// Update the entry with the filtered nodes
				entry.value = filteredNodes
			}
		}
	}
}

// removeNodeFromShard removes a node from all entries in a specific shard
func (c *RouteCache) removeNodeFromShard(shard *shardedCache, nodeID string) {
	shard.mu.Lock()
	defer shard.mu.Unlock()

	// Collect elements to be removed (if they end up with no nodes)
	var toRemove []*list.Element

	// Iterate through all entries
	for element := shard.evictList.Front(); element != nil; element = element.Next() {
		entry := element.Value.(*cacheEntry)
		modified := false

		// Filter out the specified node
		filteredNodes := make([]dht.NodeInfo, 0, len(entry.value))
		for _, node := range entry.value {
			if node.ID != nodeID {
				filteredNodes = append(filteredNodes, node)
			} else {
				modified = true
			}
		}

		// If the node was found and removed, update the entry
		if modified {
			if len(filteredNodes) == 0 {
				// If no nodes left, mark for removal
				toRemove = append(toRemove, element)
			} else {
				// Update the entry with the filtered nodes
				entry.value = filteredNodes
			}
		}
	}

	// Remove any entries that no longer have nodes
	for _, element := range toRemove {
		entry := element.Value.(*cacheEntry)
		delete(shard.items, entry.key)
		shard.evictList.Remove(element)
	}
}

// Clear empties the entire cache
func (c *RouteCache) Clear() {
	// Check if we're using sharding
	if c.shardCount > 1 {
		for _, shard := range c.shards {
			shard.mu.Lock()
			shard.items = make(map[string]*list.Element)
			shard.evictList = list.New()
			shard.mu.Unlock()
		}
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.items = make(map[string]*list.Element)
	c.evictList = list.New()
}

// Len returns the number of items in the cache
func (c *RouteCache) Len() int {
	// Check if we're using sharding
	if c.shardCount > 1 {
		total := 0
		for _, shard := range c.shards {
			shard.mu.RLock()
			total += shard.evictList.Len()
			shard.mu.RUnlock()
		}
		return total
	}

	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.evictList.Len()
}

// HitRate returns the cache hit rate as a percentage
func (c *RouteCache) HitRate() float64 {
	total := c.hits + c.misses
	if total == 0 {
		return 0.0
	}
	return float64(c.hits) / float64(total) * 100.0
}

// GetMetrics returns metrics about the cache for monitoring
func (c *RouteCache) GetMetrics() map[string]interface{} {
	metrics := map[string]interface{}{
		"capacity":    c.capacity,
		"size":        c.Len(),
		"hit_count":   c.hits,
		"miss_count":  c.misses,
		"hit_rate":    c.HitRate(),
		"shard_count": c.shardCount,
	}
	return metrics
}
