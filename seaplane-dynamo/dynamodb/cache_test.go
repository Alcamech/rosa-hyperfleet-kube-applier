package dynamodb

import (
	"sort"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func sortedStrings(ss []string) []string {
	out := make([]string, len(ss))
	copy(out, ss)
	sort.Strings(out)
	return out
}

func mustContainExactly(t *testing.T, label string, got []string, want ...string) {
	t.Helper()
	g := sortedStrings(got)
	w := sortedStrings(want)
	if len(g) != len(w) {
		t.Fatalf("%s: got %v (len=%d), want %v (len=%d)", label, g, len(g), w, len(w))
	}
	for i := range g {
		if g[i] != w[i] {
			t.Fatalf("%s: got %v, want %v", label, g, w)
		}
	}
}

func mustBeEmpty(t *testing.T, label string, got []string) {
	t.Helper()
	if len(got) != 0 {
		t.Fatalf("%s: expected empty, got %v", label, got)
	}
}

// ---------------------------------------------------------------------------
// FindChanged
// ---------------------------------------------------------------------------

func TestCache_FindChanged_NewItem(t *testing.T) {
	c := newCache()
	ut := time.Now()
	changed := c.FindChanged(map[string]time.Time{"doc-1": ut})
	mustContainExactly(t, "changed", changed, "doc-1")
}

func TestCache_FindChanged_MultipleNewItems(t *testing.T) {
	c := newCache()
	ut := time.Now()
	changed := c.FindChanged(map[string]time.Time{
		"doc-a": ut,
		"doc-b": ut.Add(time.Second),
		"doc-c": ut.Add(2 * time.Second),
	})
	mustContainExactly(t, "changed", changed, "doc-a", "doc-b", "doc-c")
}

func TestCache_FindChanged_ChangedUpdateTime(t *testing.T) {
	c := newCache()
	old := time.Now().Add(-time.Minute)
	newer := time.Now()
	c.items["doc-1"] = old

	changed := c.FindChanged(map[string]time.Time{"doc-1": newer})
	mustContainExactly(t, "changed", changed, "doc-1")
}

func TestCache_FindChanged_UnchangedUpdateTime_NoCallback(t *testing.T) {
	// Dedup: same updateTime → not changed.
	c := newCache()
	ut := time.Now().Truncate(time.Second) // RFC3339 precision
	c.items["doc-1"] = ut

	changed := c.FindChanged(map[string]time.Time{"doc-1": ut})
	mustBeEmpty(t, "changed", changed)
}

func TestCache_FindChanged_MixedChangedAndUnchanged(t *testing.T) {
	c := newCache()
	ut := time.Now().Truncate(time.Second)
	c.items["unchanged"] = ut
	c.items["changed"] = ut.Add(-time.Minute)

	changed := c.FindChanged(map[string]time.Time{
		"unchanged": ut,
		"changed":   ut, // newer than cached
		"new-item":  ut,
	})
	mustContainExactly(t, "changed", changed, "changed", "new-item")
}

func TestCache_FindChanged_NilStubs(t *testing.T) {
	c := newCache()
	c.items["doc-1"] = time.Now()
	changed := c.FindChanged(nil)
	mustBeEmpty(t, "changed with nil stubs", changed)
}

func TestCache_FindChanged_EmptyStubs(t *testing.T) {
	c := newCache()
	c.items["doc-1"] = time.Now()
	changed := c.FindChanged(map[string]time.Time{})
	mustBeEmpty(t, "changed with empty stubs", changed)
}

// ---------------------------------------------------------------------------
// ApplyStubs
// ---------------------------------------------------------------------------

func TestCache_ApplyStubs_AddsNewItems(t *testing.T) {
	c := newCache()
	ut := time.Now()
	c.ApplyStubs(map[string]time.Time{"doc-1": ut})

	if got, ok := c.items["doc-1"]; !ok || !got.Equal(ut) {
		t.Errorf("ApplyStubs should add new item; got %v", got)
	}
}

func TestCache_ApplyStubs_UpdatesExistingItems(t *testing.T) {
	c := newCache()
	old := time.Now().Add(-time.Minute)
	newer := time.Now()
	c.items["doc-1"] = old

	c.ApplyStubs(map[string]time.Time{"doc-1": newer})

	if got := c.items["doc-1"]; !got.Equal(newer) {
		t.Errorf("ApplyStubs should update existing item; got %v, want %v", got, newer)
	}
}

func TestCache_ApplyStubs_DoesNotDeleteAbsentItems(t *testing.T) {
	// Unlike ApplyRelist, ApplyStubs must NOT remove items absent from stubs.
	c := newCache()
	ut := time.Now()
	c.items["existing"] = ut

	c.ApplyStubs(map[string]time.Time{"new": ut})

	if _, ok := c.items["existing"]; !ok {
		t.Error("ApplyStubs must not delete items absent from stubs; only relist does that")
	}
	if _, ok := c.items["new"]; !ok {
		t.Error("ApplyStubs must add the new item")
	}
}

// ---------------------------------------------------------------------------
// ApplyRelist
// ---------------------------------------------------------------------------

func TestCache_ApplyRelist_DetectsAdded(t *testing.T) {
	c := newCache()
	ut := time.Now()
	added, modified, deleted := c.ApplyRelist(map[string]time.Time{"new-doc": ut})

	mustContainExactly(t, "added", added, "new-doc")
	mustBeEmpty(t, "modified", modified)
	mustBeEmpty(t, "deleted", deleted)
}

func TestCache_ApplyRelist_DetectsModified(t *testing.T) {
	c := newCache()
	old := time.Now().Add(-time.Minute).Truncate(time.Second)
	newer := time.Now().Truncate(time.Second)
	c.items["doc-1"] = old

	added, modified, deleted := c.ApplyRelist(map[string]time.Time{"doc-1": newer})

	mustBeEmpty(t, "added", added)
	mustContainExactly(t, "modified", modified, "doc-1")
	mustBeEmpty(t, "deleted", deleted)
}

func TestCache_ApplyRelist_DetectsDeleted(t *testing.T) {
	c := newCache()
	c.items["old-doc"] = time.Now()

	added, modified, deleted := c.ApplyRelist(map[string]time.Time{
		"new-doc": time.Now(),
	})

	mustContainExactly(t, "added", added, "new-doc")
	mustBeEmpty(t, "modified", modified)
	mustContainExactly(t, "deleted", deleted, "old-doc")
}

func TestCache_ApplyRelist_UnchangedItems_NotReported(t *testing.T) {
	c := newCache()
	ut := time.Now().Truncate(time.Second)
	c.items["doc-1"] = ut

	added, modified, deleted := c.ApplyRelist(map[string]time.Time{"doc-1": ut})

	mustBeEmpty(t, "added", added)
	mustBeEmpty(t, "modified", modified)
	mustBeEmpty(t, "deleted", deleted)
}

func TestCache_ApplyRelist_AllThreeCategories(t *testing.T) {
	c := newCache()
	ut := time.Now().Truncate(time.Second)

	// Pre-populate: "kept" (unchanged), "updated" (will be modified), "gone" (will be deleted)
	c.items["kept"] = ut
	c.items["updated"] = ut.Add(-time.Minute)
	c.items["gone"] = ut

	added, modified, deleted := c.ApplyRelist(map[string]time.Time{
		"kept":    ut,
		"updated": ut,    // newer
		"arrived": ut,    // new
		// "gone" is absent → deleted
	})

	mustContainExactly(t, "added", added, "arrived")
	mustContainExactly(t, "modified", modified, "updated")
	mustContainExactly(t, "deleted", deleted, "gone")
}

func TestCache_ApplyRelist_ReplacesEntireCache(t *testing.T) {
	c := newCache()
	c.items["old-a"] = time.Now()
	c.items["old-b"] = time.Now()

	ut := time.Now()
	c.ApplyRelist(map[string]time.Time{"new-x": ut})

	if _, ok := c.items["old-a"]; ok {
		t.Error("old-a should be gone after relist replaces cache")
	}
	if _, ok := c.items["old-b"]; ok {
		t.Error("old-b should be gone after relist replaces cache")
	}
	if got, ok := c.items["new-x"]; !ok || !got.Equal(ut) {
		t.Errorf("new-x should be in cache with correct updateTime; got %v", got)
	}
}

func TestCache_ApplyRelist_EmptyTableClearsCache(t *testing.T) {
	c := newCache()
	c.items["doc-1"] = time.Now()
	c.items["doc-2"] = time.Now()

	added, modified, deleted := c.ApplyRelist(map[string]time.Time{})

	mustBeEmpty(t, "added", added)
	mustBeEmpty(t, "modified", modified)
	mustContainExactly(t, "deleted", deleted, "doc-1", "doc-2")
	if len(c.items) != 0 {
		t.Errorf("cache should be empty after relist with empty table; got %v", c.items)
	}
}

func TestCache_ApplyRelist_SetsLastRelistAt(t *testing.T) {
	c := newCache()
	if c.lastRelistAt != nil {
		t.Fatal("lastRelistAt should be nil before first relist")
	}

	before := time.Now()
	c.ApplyRelist(map[string]time.Time{})
	after := time.Now()

	if c.lastRelistAt == nil {
		t.Fatal("lastRelistAt should be set after relist")
	}
	if c.lastRelistAt.Before(before) || c.lastRelistAt.After(after) {
		t.Errorf("lastRelistAt %v should be between %v and %v", *c.lastRelistAt, before, after)
	}
}

// ---------------------------------------------------------------------------
// EffectiveLookback
// ---------------------------------------------------------------------------

func TestCache_EffectiveLookback_BeforeFirstRelist_ReturnsMax(t *testing.T) {
	c := newCache()
	max := 5 * time.Minute
	got := c.EffectiveLookback(max)
	if got != max {
		t.Errorf("before first relist: got %v, want %v", got, max)
	}
}

func TestCache_EffectiveLookback_ImmediatelyAfterRelist_NearZero(t *testing.T) {
	c := newCache()
	c.ApplyRelist(map[string]time.Time{})
	max := 5 * time.Minute
	got := c.EffectiveLookback(max)
	if got > 200*time.Millisecond {
		t.Errorf("immediately after relist: lookback = %v, want < 200ms", got)
	}
}

func TestCache_EffectiveLookback_GrowsWithElapsedTime(t *testing.T) {
	c := newCache()
	max := 5 * time.Minute

	// Simulate relist that happened 30s ago.
	past := time.Now().Add(-30 * time.Second)
	c.lastRelistAt = &past

	got := c.EffectiveLookback(max)
	if got < 28*time.Second || got > 32*time.Second {
		t.Errorf("30s after relist: lookback = %v, want ~30s", got)
	}
}

func TestCache_EffectiveLookback_CappedAtMax(t *testing.T) {
	c := newCache()
	max := 5 * time.Minute

	// Simulate relist long ago (elapsed >> max).
	past := time.Now().Add(-10 * time.Minute)
	c.lastRelistAt = &past

	got := c.EffectiveLookback(max)
	if got != max {
		t.Errorf("elapsed >> max: got %v, want capped at %v", got, max)
	}
}

func TestCache_EffectiveLookback_AtExactlyMax_ReturnedAsMax(t *testing.T) {
	c := newCache()
	max := 2 * time.Minute

	// Exactly at max boundary.
	past := time.Now().Add(-max)
	c.lastRelistAt = &past

	got := c.EffectiveLookback(max)
	// elapsed ≈ max: result should be max (within a small tolerance for execution time).
	if got > max+200*time.Millisecond {
		t.Errorf("at max boundary: got %v, want ≤ %v", got, max)
	}
}

func TestCache_EffectiveLookback_MultipleRelistsResetWindow(t *testing.T) {
	c := newCache()
	max := 5 * time.Minute

	// First relist long ago.
	past := time.Now().Add(-10 * time.Minute)
	c.lastRelistAt = &past

	got1 := c.EffectiveLookback(max)
	if got1 != max {
		t.Errorf("first check: got %v, want %v", got1, max)
	}

	// Second relist just happened → window resets near zero.
	c.ApplyRelist(map[string]time.Time{})
	got2 := c.EffectiveLookback(max)
	if got2 > 200*time.Millisecond {
		t.Errorf("after second relist: lookback = %v, want < 200ms", got2)
	}
}
