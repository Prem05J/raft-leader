package fsm

import "sync"

type DedupCache struct {
	mu    sync.Mutex
	keys  map[string]struct{}
	max   int
	order []string
}

func NewDedupCache(max int) *DedupCache {
	return &DedupCache{
		keys: make(map[string]struct{}),
		max:  max,
	}
}

func (c *DedupCache) Seen(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.keys[key]; exists {
		return true
	}

	c.keys[key] = struct{}{}
	c.order = append(c.order, key)
	if len(c.order) > c.max {
		oldest := c.order[0]
		c.order = c.order[1:]
		delete(c.keys, oldest)
	}
	return false
}

func (c *DedupCache) Add(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.keys[key]; exists {
		return
	}
	c.keys[key] = struct{}{}
	c.order = append(c.order, key)
	if len(c.order) > c.max {
		oldest := c.order[0]
		c.order = c.order[1:]
		delete(c.keys, oldest)
	}
}
