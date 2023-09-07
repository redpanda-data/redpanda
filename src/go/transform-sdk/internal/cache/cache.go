package cache

import "math"

type (
	entry[V any] struct {
		value           V
		insertionNumber int
	}
	// A cache that evicts in FIFO order based on number of entries
	Cache[K comparable, V any] struct {
		underlying                 map[K]entry[V]
		latestEntryInsertionNumber int
		maxEntries                 int
	}
)

// New returns a new cache with a limited set of entries.
func New[K comparable, V any](maxSize int) Cache[K, V] {
	return Cache[K, V]{
		underlying:                 make(map[K]entry[V]),
		latestEntryInsertionNumber: 0,
		maxEntries:                 maxSize,
	}
}

// Put adds an entry into the cache
func (c *Cache[K, V]) Put(k K, v V) {
	c.prune()
	c.underlying[k] = entry[V]{value: v, insertionNumber: c.latestEntryInsertionNumber}
	c.latestEntryInsertionNumber += 1
}

// Get extracts a value from the cache
func (c *Cache[K, V]) Get(k K) (v V, ok bool) {
	e, ok := c.underlying[k]
	if !ok {
		return v, false
	}
	return e.value, true
}

// Size returns the size of the cache
func (c *Cache[K, V]) Size() int {
	return len(c.underlying)
}

func (c *Cache[K, V]) prune() {
	for len(c.underlying) >= c.maxEntries {
		var key K
		min := math.MaxInt
		for k, e := range c.underlying {
			if e.insertionNumber < min {
				min = e.insertionNumber
				key = k
			}
		}
		delete(c.underlying, key)
	}
}
