package dynamodb

import (
	"sync"
	"time"
)

// Cache is an in-memory store of documentID → updateTime stubs used for
// deduplication and the expanding lookback window calculation.
//
// All methods are safe for concurrent use.
type Cache struct {
	mu           sync.RWMutex
	items        map[string]time.Time // documentID → last-seen updateTime
	lastRelistAt *time.Time           // nil until first relist completes
}

func newCache() *Cache {
	return &Cache{items: make(map[string]time.Time)}
}

// FindChanged returns the documentIDs from stubs whose updateTime differs from
// the cached value (new items or items with a later updateTime). Items whose
// updateTime matches the cache are dropped (dedup — no doorbell needed).
func (c *Cache) FindChanged(stubs map[string]time.Time) []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var changed []string
	for docID, ut := range stubs {
		cached, exists := c.items[docID]
		if !exists || !cached.Equal(ut) {
			changed = append(changed, docID)
		}
	}
	return changed
}

// ApplyStubs updates the cache with the given stubs from a fast-poll result.
// Only updates entries that are already present or new — does not delete
// entries absent from stubs (deletions are only detected by ApplyRelist).
func (c *Cache) ApplyStubs(stubs map[string]time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for docID, ut := range stubs {
		c.items[docID] = ut
	}
}

// ApplyRelist diffs stubs (the full consistent scan result) against the cache.
// Returns the documentIDs that were added, modified, or deleted relative to
// the current cache, then replaces the cache with stubs and records the relist
// time.
func (c *Cache) ApplyRelist(stubs map[string]time.Time) (added, modified, deleted []string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for docID, ut := range stubs {
		cached, exists := c.items[docID]
		if !exists {
			added = append(added, docID)
		} else if !cached.Equal(ut) {
			modified = append(modified, docID)
		}
	}
	for docID := range c.items {
		if _, exists := stubs[docID]; !exists {
			deleted = append(deleted, docID)
		}
	}

	// Replace the entire cache with the relist result.
	c.items = make(map[string]time.Time, len(stubs))
	for k, v := range stubs {
		c.items[k] = v
	}

	now := time.Now().UTC()
	c.lastRelistAt = &now
	return
}

// EffectiveLookback returns the expanding lookback window duration for the
// next fast-poll query:
//
//   - Before the first relist: returns maxLookback (safe full-window default).
//   - After a relist: returns min(now - lastRelistAt, maxLookback).
//
// This mirrors the algorithm in box-tricks/watcher.py lines 663-671.
func (c *Cache) EffectiveLookback(maxLookback time.Duration) time.Duration {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.lastRelistAt == nil {
		return maxLookback
	}
	elapsed := time.Since(*c.lastRelistAt)
	if elapsed > maxLookback {
		return maxLookback
	}
	return elapsed
}
