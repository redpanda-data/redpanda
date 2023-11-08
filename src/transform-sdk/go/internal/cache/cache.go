package cache

import "container/list"

type (
	entry[K comparable, V any] struct {
		key   K
		value V
	}
	// A cache that evicts in LRU order based on number of entries
	Cache[K comparable, V any] struct {
		// list contains the entries themselves
		// in LRU order
		list       *list.List
		underlying map[K]*list.Element
		maxEntries int
	}
)

// New returns a new cache with a limited set of entries.
func New[K comparable, V any](maxSize int) Cache[K, V] {
	return Cache[K, V]{
		underlying: make(map[K]*list.Element),
		list:       list.New(),
		maxEntries: maxSize,
	}
}

// Put adds an entry into the cache
func (c *Cache[K, V]) Put(k K, v V) {
	if n, ok := c.underlying[k]; ok {
		// this key is already in the cache, update the value
		// then move it to the end of our list
		n.Value.(*entry[K, V]).value = v
		c.list.MoveToBack(n)
	} else {
		// otherwise prune the least recently used entry and
		// insert the new value
		c.prune()
		c.underlying[k] = c.list.PushBack(&entry[K, V]{k, v})
	}
}

// Get extracts a value from the cache
func (c *Cache[K, V]) Get(k K) (v V, ok bool) {
	n, ok := c.underlying[k]
	if !ok {
		return v, false
	}
	v = n.Value.(*entry[K, V]).value
	// move this node to the end of our list
	// to mark it as most recently used
	c.list.MoveToBack(n)
	return v, true
}

// Size returns the size of the cache
func (c *Cache[K, V]) Size() int {
	return len(c.underlying)
}

func (c *Cache[K, V]) prune() {
	for len(c.underlying) >= c.maxEntries {
		toRemove := c.list.Front()
		entry := toRemove.Value.(*entry[K, V])
		delete(c.underlying, entry.key)
		c.list.Remove(toRemove)
	}
}
