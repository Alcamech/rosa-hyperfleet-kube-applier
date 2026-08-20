package dynamodb

// Ironclad unit tests for the two-speed Watcher engine.
//
// All tests are pure unit tests — no LocalStack, no real AWS. The fakeDynamo
// HTTP transport (fake_dynamo_test.go) intercepts DynamoDB Scan and Query
// calls and returns configured responses, letting us drive the full
// ScanAll → runRelist and QuerySince → runFastPoll code paths end-to-end.
//
// Test coverage matrix:
//
//	┌──────────────────────────────────────────────────────────┬──────────┐
//	│ Scenario                                                  │ Category │
//	├──────────────────────────────────────────────────────────┼──────────┤
//	│ ScanAll returns items; runRelist fires onChange for each  │ relist   │
//	│ Relist detects added items                                │ relist   │
//	│ Relist detects modified items                             │ relist   │
//	│ Relist detects deleted items (hard delete)                │ relist   │
//	│ Relist replaces cache (old items purged)                  │ relist   │
//	│ Done() closes after first relist tick                     │ relist   │
//	│ Relist scan error → onChange NOT called                   │ relist   │
//	│ QuerySince returns items; runFastPoll fires onChange       │ poll     │
//	│ Poll dedupes unchanged items (same updateTime)            │ poll     │
//	│ Poll updates cache after firing onChange                  │ poll     │
//	│ Poll does NOT detect hard deletes                         │ poll     │
//	│ Expanding lookback: near zero after relist                │ poll     │
//	│ Expanding lookback: capped at relistInterval              │ poll     │
//	│ Poll error → onChange NOT called                          │ poll     │
//	│ Stop() / context cancel terminates both loops             │ engine   │
//	│ Startup order: relist goroutine starts before poll        │ engine   │
//	└──────────────────────────────────────────────────────────┴──────────┘

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// collectOnChange returns an OnChange callback that records all received
// documentIDs, plus a function to retrieve the current snapshot.
func collectOnChange() (OnChange, func() []string) {
	var mu sync.Mutex
	var received []string
	cb := func(docID string) {
		mu.Lock()
		defer mu.Unlock()
		received = append(received, docID)
	}
	get := func() []string {
		mu.Lock()
		defer mu.Unlock()
		out := make([]string, len(received))
		copy(out, received)
		return out
	}
	return cb, get
}

// waitForIDs waits up to timeout for the get() snapshot to contain exactly
// the wanted IDs (order-independent). Returns an error if it times out.
func waitForIDs(t *testing.T, label string, get func() []string, timeout time.Duration, want ...string) {
	t.Helper()
	wantSorted := append([]string(nil), want...)
	sort.Strings(wantSorted)

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		got := get()
		sorted := append([]string(nil), got...)
		sort.Strings(sorted)
		if len(sorted) == len(wantSorted) {
			match := true
			for i := range sorted {
				if sorted[i] != wantSorted[i] {
					match = false
					break
				}
			}
			if match {
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("%s: timed out waiting for IDs %v; last snapshot: %v", label, want, get())
}

// waitForAtLeast waits until get() has at least n entries.
func waitForAtLeast(t *testing.T, label string, get func() []string, n int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if len(get()) >= n {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("%s: timed out waiting for at least %d IDs; last snapshot: %v", label, n, get())
}

// waitForClosed waits until ch is closed.
func waitForClosed(t *testing.T, label string, ch <-chan struct{}, timeout time.Duration) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(timeout):
		t.Fatalf("%s: timed out waiting for channel to close", label)
	}
}

// newTestWatcher creates a Watcher backed by the given fakeDynamo.
func newTestWatcher(fd *fakeDynamo, onChange OnChange, pollInterval, relistInterval time.Duration) *Watcher {
	return New(
		fd.newClient(),
		"test-table",
		onChange,
		Options{
			PollInterval:   pollInterval,
			RelistInterval: relistInterval,
			ShardCount:     GSIShardCount,
		},
	)
}

// t1, t2, t3 are fixed RFC3339-aligned timestamps used across tests.
var (
	t1 = time.Date(2026, 1, 1, 0, 0, 1, 0, time.UTC)
	t2 = time.Date(2026, 1, 1, 0, 0, 2, 0, time.UTC)
	t3 = time.Date(2026, 1, 1, 0, 0, 3, 0, time.UTC)
)

// setQueryAllShards sets empty responses on shards 1–3 and the given items on
// shard 0. Shard 0 is set last so it is not overwritten by the loop.
func setQueryAllShards(fd *fakeDynamo, items []map[string]any) {
	for s := 1; s < GSIShardCount; s++ {
		fd.setQueryItems(fmt.Sprintf("%d", s), nil)
	}
	fd.setQueryItems("0", items)
}

// ---------------------------------------------------------------------------
// ScanAll unit tests (tests the scan.go path in isolation)
// ---------------------------------------------------------------------------

func TestScanAll_ReturnsAllItems(t *testing.T) {
	fd := newFakeDynamo()
	fd.setScanItems(stubItems(map[string]time.Time{
		"doc-a": t1,
		"doc-b": t2,
	}))

	ctx := context.Background()
	result, err := ScanAll(ctx, fd.newClient(), "test-table")
	if err != nil {
		t.Fatalf("ScanAll error: %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("ScanAll: got %d items, want 2", len(result))
	}
	if !result["doc-a"].Equal(t1) {
		t.Errorf("doc-a updateTime: got %v, want %v", result["doc-a"], t1)
	}
	if !result["doc-b"].Equal(t2) {
		t.Errorf("doc-b updateTime: got %v, want %v", result["doc-b"], t2)
	}
}

func TestScanAll_EmptyTable(t *testing.T) {
	fd := newFakeDynamo()
	fd.setScanItems(nil)

	ctx := context.Background()
	result, err := ScanAll(ctx, fd.newClient(), "test-table")
	if err != nil {
		t.Fatalf("ScanAll error: %v", err)
	}
	if len(result) != 0 {
		t.Fatalf("ScanAll: expected empty result, got %v", result)
	}
}

// ---------------------------------------------------------------------------
// QuerySince unit tests (tests the gsi.go path in isolation)
// ---------------------------------------------------------------------------

func TestQuerySince_ReturnsItemsFromAllShards(t *testing.T) {
	fd := newFakeDynamo()
	// One item per shard.
	for s := 0; s < GSIShardCount; s++ {
		shard := fmt.Sprintf("%d", s)
		docID := fmt.Sprintf("doc-shard-%d", s)
		fd.setQueryItems(shard, stubItems(map[string]time.Time{docID: t1}))
	}

	ctx := context.Background()
	result, err := QuerySince(ctx, fd.newClient(), "test-table", t1.Add(-time.Minute), GSIShardCount)
	if err != nil {
		t.Fatalf("QuerySince error: %v", err)
	}
	if len(result) != GSIShardCount {
		t.Fatalf("QuerySince: got %d items, want %d (one per shard)", len(result), GSIShardCount)
	}
	for s := 0; s < GSIShardCount; s++ {
		docID := fmt.Sprintf("doc-shard-%d", s)
		if _, ok := result[docID]; !ok {
			t.Errorf("missing item from shard %d: %q", s, docID)
		}
	}
}

func TestQuerySince_EmptyShards(t *testing.T) {
	fd := newFakeDynamo()
	for s := 0; s < GSIShardCount; s++ {
		fd.setQueryItems(fmt.Sprintf("%d", s), nil)
	}

	ctx := context.Background()
	result, err := QuerySince(ctx, fd.newClient(), "test-table", t1, GSIShardCount)
	if err != nil {
		t.Fatalf("QuerySince error: %v", err)
	}
	if len(result) != 0 {
		t.Fatalf("QuerySince: expected empty, got %v", result)
	}
}

// ---------------------------------------------------------------------------
// Relist end-to-end tests
//
// Key parameter choices:
//   pollInterval:   time.Hour     — poll goroutine starts at 500ms but ticker
//                                   fires after 1h, so poll effectively never runs
//   relistInterval: 50ms          — first relist fires at t=50ms, well within timeouts
// ---------------------------------------------------------------------------

func TestWatcher_Relist_FirstRelist_FiresOnChangeForAllItems(t *testing.T) {
	// On the very first relist the cache is empty, so all scanned items are
	// new → onChange must fire for each one.
	fd := newFakeDynamo()
	fd.setScanItems(stubItems(map[string]time.Time{
		"doc-a": t1,
		"doc-b": t2,
		"doc-c": t3,
	}))

	onChange, get := collectOnChange()
	w := newTestWatcher(fd, onChange, time.Hour, 50*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go w.Run(ctx)
	defer w.Stop()

	waitForIDs(t, "first relist", get, 3*time.Second, "doc-a", "doc-b", "doc-c")
}

func TestWatcher_Relist_Done_ClosesAfterFirstRelist(t *testing.T) {
	// Done() must close after the first relist ticker fires.
	fd := newFakeDynamo()
	fd.setScanItems(nil) // empty table is fine

	onChange, _ := collectOnChange()
	w := newTestWatcher(fd, onChange, time.Hour, 50*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go w.Run(ctx)
	defer w.Stop()

	waitForClosed(t, "Done()", w.Done(), 3*time.Second)
}

func TestWatcher_Relist_DetectsAdded(t *testing.T) {
	// Second relist finds a new item not in the first scan.
	fd := newFakeDynamo()
	// First relist: one item.
	fd.setScanItems(stubItems(map[string]time.Time{"existing": t1}))
	// Second relist: original + new item.
	fd.setScanItems(stubItems(map[string]time.Time{
		"existing": t1,
		"new-doc":  t2,
	}))

	onChange, get := collectOnChange()
	w := newTestWatcher(fd, onChange, time.Hour, 30*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go w.Run(ctx)
	defer w.Stop()

	// Wait for Done() (first relist complete) then for second relist.
	waitForClosed(t, "Done()", w.Done(), 3*time.Second)
	waitForAtLeast(t, "second relist onChange", get, 2, 3*time.Second)

	received := get()
	found := map[string]bool{}
	for _, id := range received {
		found[id] = true
	}
	if !found["existing"] {
		t.Error("'existing' should have fired from first relist")
	}
	if !found["new-doc"] {
		t.Error("'new-doc' should have fired from second relist as added")
	}
}

func TestWatcher_Relist_DetectsModified(t *testing.T) {
	// Second relist sees a newer updateTime for an existing item.
	fd := newFakeDynamo()
	fd.setScanItems(stubItems(map[string]time.Time{"doc-1": t1}))
	fd.setScanItems(stubItems(map[string]time.Time{"doc-1": t2})) // updated

	onChange, get := collectOnChange()
	w := newTestWatcher(fd, onChange, time.Hour, 30*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go w.Run(ctx)
	defer w.Stop()

	waitForClosed(t, "Done()", w.Done(), 3*time.Second)
	// First relist fires doc-1 (new). Second relist fires doc-1 again (modified).
	waitForAtLeast(t, "two onChange calls for doc-1", get, 2, 3*time.Second)

	for _, id := range get() {
		if id != "doc-1" {
			t.Errorf("unexpected onChange call for %q", id)
		}
	}
}

func TestWatcher_Relist_DetectsDeleted(t *testing.T) {
	// Second relist no longer contains a doc that was in the first scan.
	fd := newFakeDynamo()
	fd.setScanItems(stubItems(map[string]time.Time{"gone-doc": t1}))
	fd.setScanItems(nil) // gone-doc is deleted

	onChange, get := collectOnChange()
	w := newTestWatcher(fd, onChange, time.Hour, 30*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go w.Run(ctx)
	defer w.Stop()

	waitForClosed(t, "Done()", w.Done(), 3*time.Second)
	// Two calls: first relist (added), second relist (deleted).
	waitForAtLeast(t, "two onChange calls", get, 2, 3*time.Second)

	for _, id := range get() {
		if id != "gone-doc" {
			t.Errorf("unexpected onChange call for %q; expected only gone-doc", id)
		}
	}
}

func TestWatcher_Relist_UnchangedItem_NoSecondCallback(t *testing.T) {
	// If updateTime is the same in both relists → no second onChange.
	fd := newFakeDynamo()
	fd.setScanItems(stubItems(map[string]time.Time{"doc-stable": t1}))
	// Same updateTime → second relist should produce no onChange.
	fd.setScanItems(stubItems(map[string]time.Time{"doc-stable": t1}))

	onChange, get := collectOnChange()
	w := newTestWatcher(fd, onChange, time.Hour, 30*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go w.Run(ctx)
	defer w.Stop()

	waitForClosed(t, "Done()", w.Done(), 3*time.Second)
	// Let second relist tick fire.
	time.Sleep(60 * time.Millisecond)

	received := get()
	if len(received) != 1 {
		t.Errorf("expected exactly 1 onChange call (first relist, item new), got %d: %v", len(received), received)
	}
}

// ---------------------------------------------------------------------------
// Fast poll end-to-end tests
//
// Key parameter choices:
//   relistInterval: 50ms   — relist fires at t=50ms, before poll starts at t=500ms
//   pollInterval:   20ms   — first poll fires at t=520ms
//
// Startup sequence: relist goroutine starts immediately, poll goroutine starts
// after startupRelistDelay (500ms). With relistInterval=50ms, the first relist
// completes at t=50ms and seeds the cache well before the poll fires at t=520ms.
// ---------------------------------------------------------------------------

func TestWatcher_FastPoll_DetectsNewItem(t *testing.T) {
	// Relist finds nothing (empty table). Fast poll finds a new item in the GSI.
	fd := newFakeDynamo()
	fd.setScanItems(nil) // empty table at relist time

	// GSI returns a new item on the first poll (shard 0).
	setQueryAllShards(fd, stubItems(map[string]time.Time{"poll-doc": t1}))

	onChange, get := collectOnChange()
	w := newTestWatcher(fd, onChange, 20*time.Millisecond, 50*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go w.Run(ctx)
	defer w.Stop()

	waitForIDs(t, "poll detects new item", get, 3*time.Second, "poll-doc")
}

func TestWatcher_FastPoll_DedupsUnchangedItem(t *testing.T) {
	// Relist seeds cache with doc-1 at t1. Poll returns doc-1 at t1 → dedup.
	fd := newFakeDynamo()
	fd.setScanItems(stubItems(map[string]time.Time{"doc-1": t1}))

	// Poll returns doc-1 at same t1 — all subsequent calls return this too.
	setQueryAllShards(fd, stubItems(map[string]time.Time{"doc-1": t1}))

	onChange, get := collectOnChange()
	w := newTestWatcher(fd, onChange, 20*time.Millisecond, 50*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go w.Run(ctx)
	defer w.Stop()

	// Wait for the relist (one onChange for doc-1).
	waitForIDs(t, "relist fires for doc-1", get, 3*time.Second, "doc-1")

	// Let the poll tick at least twice (20ms each).
	time.Sleep(100 * time.Millisecond)

	// Still exactly one call — poll with same updateTime should dedup.
	if n := len(get()); n != 1 {
		t.Errorf("dedup: expected exactly 1 onChange call total, got %d: %v", n, get())
	}
}

func TestWatcher_FastPoll_FiresWhenUpdateTimeChanges(t *testing.T) {
	// Relist seeds cache with doc-1 at t1. Poll returns doc-1 at t2 → fires.
	fd := newFakeDynamo()
	fd.setScanItems(stubItems(map[string]time.Time{"doc-1": t1}))

	// Poll returns doc-1 at newer t2.
	setQueryAllShards(fd, stubItems(map[string]time.Time{"doc-1": t2}))

	onChange, get := collectOnChange()
	// relistInterval=50ms ensures relist fires at t=50ms, seeding cache before poll at t=520ms.
	w := newTestWatcher(fd, onChange, 20*time.Millisecond, 50*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go w.Run(ctx)
	defer w.Stop()

	// At least 2 calls: first relist (doc-1 new at t1) + first poll (doc-1 updated to t2).
	waitForAtLeast(t, "relist + poll onChange", get, 2, 3*time.Second)
	for _, id := range get() {
		if id != "doc-1" {
			t.Errorf("unexpected onChange for %q", id)
		}
	}
}

func TestWatcher_FastPoll_UpdatesCacheAfterFiring(t *testing.T) {
	// After poll fires for doc-1 at t2, cache must be updated to t2.
	// A subsequent poll returning doc-1 at t2 must dedup.
	fd := newFakeDynamo()
	// Relist seeds cache with doc-1 at t1.
	fd.setScanItems(stubItems(map[string]time.Time{"doc-1": t1}))

	// First poll tick: doc-1 at t2 → fires onChange, updates cache to t2.
	setQueryAllShards(fd, stubItems(map[string]time.Time{"doc-1": t2}))
	// Second poll tick: doc-1 at t2 again → dedup (cache already has t2).
	setQueryAllShards(fd, stubItems(map[string]time.Time{"doc-1": t2}))

	onChange, get := collectOnChange()
	// relistInterval=50ms so relist fires at t=50ms, cache seeded before poll at t=520ms.
	w := newTestWatcher(fd, onChange, 20*time.Millisecond, 50*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go w.Run(ctx)
	defer w.Stop()

	// Wait for exactly 2 onChange calls: relist (doc-1 at t1) + first poll (doc-1 at t2).
	waitForAtLeast(t, "relist + first poll", get, 2, 3*time.Second)
	// Let at least one more poll tick pass to confirm the second poll deduped.
	time.Sleep(80 * time.Millisecond)

	// Must still be exactly 2 — second poll deduped (cache has t2, poll returns t2).
	if n := len(get()); n != 2 {
		t.Errorf("expected 2 total onChange calls (relist + first poll), got %d: %v", n, get())
	}
}

func TestWatcher_FastPoll_DoesNotDetectHardDeletes(t *testing.T) {
	// GSI poll can only detect adds/modifies within the lookback window.
	// Hard deletes (item removed from DynamoDB) will NOT appear in the GSI
	// poll result — they require the full consistent relist to detect.
	fd := newFakeDynamo()
	fd.setScanItems(stubItems(map[string]time.Time{
		"doc-exists": t1,
		"doc-gone":   t2,
	}))

	// Poll returns only doc-exists (doc-gone was hard-deleted, not in GSI).
	setQueryAllShards(fd, stubItems(map[string]time.Time{"doc-exists": t1}))

	onChange, get := collectOnChange()
	// relistInterval=50ms so relist fires and seeds cache before poll.
	w := newTestWatcher(fd, onChange, 20*time.Millisecond, 50*time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go w.Run(ctx)
	defer w.Stop()

	// Relist fires for both (both are new to empty cache).
	waitForIDs(t, "relist fires for both docs", get, 3*time.Second, "doc-exists", "doc-gone")

	// Let poll tick at least once (poll starts at t=500ms, ticks every 20ms).
	time.Sleep(600 * time.Millisecond)

	// doc-gone must NOT have received a second callback from the poll
	// (it's absent from the GSI poll result — hard deletes are invisible to poll).
	deleteCalls := 0
	for _, id := range get() {
		if id == "doc-gone" {
			deleteCalls++
		}
	}
	if deleteCalls > 1 {
		t.Errorf("poll should NOT re-fire for hard-deleted item; got %d calls for doc-gone", deleteCalls)
	}

	// doc-exists at same t1 should also not fire a second time (dedup).
	existsCalls := 0
	for _, id := range get() {
		if id == "doc-exists" {
			existsCalls++
		}
	}
	if existsCalls != 1 {
		t.Errorf("doc-exists: expected 1 call (relist only, poll deduped), got %d", existsCalls)
	}
}

// ---------------------------------------------------------------------------
// Expanding lookback window tests (via Cache directly — unit-level)
// ---------------------------------------------------------------------------

func TestWatcher_ExpandingLookback_BeforeFirstRelist_UsesMax(t *testing.T) {
	c := newCache()
	max := 5 * time.Minute
	got := c.EffectiveLookback(max)
	if got != max {
		t.Errorf("before first relist: lookback = %v, want %v", got, max)
	}
}

func TestWatcher_ExpandingLookback_NearZeroImmediatelyAfterRelist(t *testing.T) {
	c := newCache()
	c.ApplyRelist(map[string]time.Time{})
	max := 5 * time.Minute
	got := c.EffectiveLookback(max)
	if got > 200*time.Millisecond {
		t.Errorf("immediately after relist: lookback = %v, want < 200ms", got)
	}
}

func TestWatcher_ExpandingLookback_GrowsByElapsedTime(t *testing.T) {
	c := newCache()
	max := 5 * time.Minute

	past := time.Now().Add(-30 * time.Second)
	c.lastRelistAt = &past

	got := c.EffectiveLookback(max)
	if got < 28*time.Second || got > 32*time.Second {
		t.Errorf("30s elapsed: lookback = %v, want ~30s (±2s)", got)
	}
}

func TestWatcher_ExpandingLookback_CappedAtMax(t *testing.T) {
	c := newCache()
	max := 5 * time.Minute

	past := time.Now().Add(-10 * time.Minute) // >> max
	c.lastRelistAt = &past

	got := c.EffectiveLookback(max)
	if got != max {
		t.Errorf("elapsed >> max: got %v, want capped at %v", got, max)
	}
}

// ---------------------------------------------------------------------------
// Engine lifecycle tests
// ---------------------------------------------------------------------------

func TestWatcher_Stop_StopsCleanly(t *testing.T) {
	fd := newFakeDynamo()
	fd.setScanItems(nil)

	onChange, _ := collectOnChange()
	w := newTestWatcher(fd, onChange, time.Hour, 50*time.Millisecond)

	ctx := context.Background()
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		w.Run(ctx)
	}()

	// Wait for the first relist to fire, then stop.
	waitForClosed(t, "Done()", w.Done(), 3*time.Second)
	w.Stop()

	select {
	case <-runDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Run() did not return within 2s after Stop()")
	}
}

func TestWatcher_ContextCancel_StopsCleanly(t *testing.T) {
	fd := newFakeDynamo()
	fd.setScanItems(nil)

	onChange, _ := collectOnChange()
	w := newTestWatcher(fd, onChange, time.Hour, 50*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		w.Run(ctx)
	}()

	waitForClosed(t, "Done()", w.Done(), 3*time.Second)
	cancel()

	select {
	case <-runDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Run() did not return within 2s after context cancel")
	}
}

func TestWatcher_Stop_BeforeFirstRelist_StopsCleanly(t *testing.T) {
	// Stop() before the relist ticker fires should still return cleanly.
	fd := newFakeDynamo()
	fd.setScanItems(nil)

	onChange, _ := collectOnChange()
	// Very long relist interval so the relist ticker never fires during the test.
	w := newTestWatcher(fd, onChange, time.Hour, time.Hour)

	ctx := context.Background()
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		w.Run(ctx)
	}()

	w.Stop()

	select {
	case <-runDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Run() did not return within 2s after early Stop()")
	}
}

func TestWatcher_StartupOrder_RelistStartsBeforePoll(t *testing.T) {
	// Startup sequence (watcher.go Run()):
	//   1. Relist goroutine starts immediately.
	//   2. startupRelistDelay (500ms) pause.
	//   3. Fast poll goroutine starts.
	//
	// We verify: the first Scan (relist) fires before the first Query (poll).
	// With relistInterval=50ms the first Scan fires at t≈50ms.
	// The poll goroutine doesn't start until t=500ms, so first Query fires at t≈520ms.
	// Therefore relistAt < pollAt is guaranteed.
	var relistAt, pollAt int64 // unix nanoseconds; 0 = not yet called
	var scanCount, queryCount int32

	inner := newFakeDynamo()
	inner.setScanItems(nil)
	for s := 0; s < GSIShardCount; s++ {
		inner.setQueryItems(fmt.Sprintf("%d", s), nil)
	}

	var eventsMu sync.Mutex
	var events []callOrderEvent

	recordingFD := &recordingFakeDynamo{
		inner:       inner,
		scanCalled:  &relistAt,
		queryCalled: &pollAt,
		eventsMu:    &eventsMu,
		events:      &events,
		scanCount:   &scanCount,
		queryCount:  &queryCount,
	}

	onChange, _ := collectOnChange()
	w := New(
		recordingFD.newClient(),
		"test-table",
		onChange,
		Options{
			PollInterval:   20 * time.Millisecond,
			RelistInterval: 50 * time.Millisecond,
			ShardCount:     GSIShardCount,
		},
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go w.Run(ctx)
	defer w.Stop()

	// Wait for at least one relist and one poll.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&scanCount) >= 1 && atomic.LoadInt32(&queryCount) >= 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	if atomic.LoadInt32(&scanCount) == 0 {
		t.Fatal("relist (Scan) never fired")
	}
	if atomic.LoadInt32(&queryCount) == 0 {
		t.Fatal("fast poll (Query) never fired")
	}

	rAt := atomic.LoadInt64(&relistAt)
	pAt := atomic.LoadInt64(&pollAt)
	if rAt >= pAt {
		t.Errorf("relist should fire before poll: relist at %v, poll at %v (delta %v)",
			time.Unix(0, rAt), time.Unix(0, pAt), time.Duration(pAt-rAt))
	}
}

// ---------------------------------------------------------------------------
// recordingFakeDynamo — wraps fakeDynamo and records Scan/Query call timestamps
// ---------------------------------------------------------------------------

type callOrderEvent struct {
	which string
	when  time.Time
}

type recordingFakeDynamo struct {
	inner       *fakeDynamo
	scanCalled  *int64 // first Scan call, nanoseconds
	queryCalled *int64 // first Query call, nanoseconds
	eventsMu    *sync.Mutex
	events      *[]callOrderEvent
	scanCount   *int32
	queryCount  *int32
}

func (r *recordingFakeDynamo) RoundTrip(req *http.Request) (*http.Response, error) {
	target := req.Header.Get("X-Amz-Target")
	now := time.Now().UnixNano()

	if strings.HasSuffix(target, "Scan") {
		atomic.CompareAndSwapInt64(r.scanCalled, 0, now)
		atomic.AddInt32(r.scanCount, 1)
		r.eventsMu.Lock()
		*r.events = append(*r.events, callOrderEvent{"scan", time.Now()})
		r.eventsMu.Unlock()
	} else if strings.HasSuffix(target, "Query") {
		atomic.CompareAndSwapInt64(r.queryCalled, 0, now)
		atomic.AddInt32(r.queryCount, 1)
		r.eventsMu.Lock()
		*r.events = append(*r.events, callOrderEvent{"query", time.Now()})
		r.eventsMu.Unlock()
	}

	return r.inner.RoundTrip(req)
}

func (r *recordingFakeDynamo) newClient() *dynamodb.Client {
	cfg := r.inner.awsConfig()
	cfg.HTTPClient = &http.Client{Transport: r}
	return dynamodb.NewFromConfig(cfg)
}

// ---------------------------------------------------------------------------
// Error-path tests
// ---------------------------------------------------------------------------

func TestWatcher_RelistScanError_OnChangeNotCalled(t *testing.T) {
	// When ScanAll returns an error, onChange must NOT be called.
	// Use HTTP 400 (ValidationException) — non-retryable, keeps test fast.
	fd := &errorFakeDynamo{statusCode: 400}

	onChange, get := collectOnChange()
	w := New(
		fd.newClient(),
		"test-table",
		onChange,
		Options{
			PollInterval:   time.Hour,
			RelistInterval: 30 * time.Millisecond,
			ShardCount:     GSIShardCount,
		},
	)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	go w.Run(ctx)
	defer w.Stop()

	// Let several relist ticks pass (or timeout).
	<-ctx.Done()

	if n := len(get()); n != 0 {
		t.Errorf("onChange should not be called when scan errors; got %d calls: %v", n, get())
	}
}

func TestWatcher_PollQueryError_OnChangeNotCalled(t *testing.T) {
	// When QuerySince returns an error, onChange must NOT be called.
	// Seed the relist normally (empty table), then error on all Query calls.
	fd := newFakeDynamo()
	fd.setScanItems(nil) // relist OK, empty table — onChange never called from relist

	// Use HTTP 400 (ValidationException) — non-retryable, keeps test fast.
	errorFD := &errorOnQueryFakeDynamo{
		scanFD: fd,
		status: 400,
	}

	onChange, get := collectOnChange()
	w := New(
		errorFD.newClient(),
		"test-table",
		onChange,
		Options{
			PollInterval:   20 * time.Millisecond,
			RelistInterval: 5 * time.Second,
			ShardCount:     GSIShardCount,
		},
	)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	go w.Run(ctx)
	defer w.Stop()

	<-ctx.Done()

	// onChange should never have been called (relist had nothing, polls errored).
	if n := len(get()); n != 0 {
		t.Errorf("onChange should not be called when polls error; got %d calls: %v", n, get())
	}
}

// ---------------------------------------------------------------------------
// Error fake helpers
// ---------------------------------------------------------------------------

// errorFakeDynamo always returns the given HTTP status code for all requests.
type errorFakeDynamo struct {
	statusCode int
}

func (e *errorFakeDynamo) RoundTrip(req *http.Request) (*http.Response, error) {
	errType := "InternalServerError"
	if e.statusCode < 500 {
		errType = "ValidationException"
	}
	body := fmt.Sprintf(`{"__type":%q,"message":"fake error"}`, errType)
	return &http.Response{
		StatusCode: e.statusCode,
		Header:     http.Header{"Content-Type": []string{"application/x-amz-json-1.0"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}, nil
}

func (e *errorFakeDynamo) newClient() *dynamodb.Client {
	cfg := aws.Config{
		Region: "us-east-1",
		Credentials: aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "fake", SecretAccessKey: "fake", SessionToken: "fake"}, nil
		}),
		HTTPClient: &http.Client{Transport: e},
	}
	return dynamodb.NewFromConfig(cfg)
}

// errorOnQueryFakeDynamo routes Scan to scanFD and returns a non-retryable
// error for all Query calls.
type errorOnQueryFakeDynamo struct {
	scanFD *fakeDynamo
	status int
}

func (e *errorOnQueryFakeDynamo) RoundTrip(req *http.Request) (*http.Response, error) {
	target := req.Header.Get("X-Amz-Target")
	if strings.HasSuffix(target, "Query") {
		body := `{"__type":"ValidationException","message":"fake query error"}`
		return &http.Response{
			StatusCode: e.status,
			Header:     http.Header{"Content-Type": []string{"application/x-amz-json-1.0"}},
			Body:       io.NopCloser(strings.NewReader(body)),
		}, nil
	}
	return e.scanFD.RoundTrip(req)
}

func (e *errorOnQueryFakeDynamo) newClient() *dynamodb.Client {
	cfg := aws.Config{
		Region: "us-east-1",
		Credentials: aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "fake", SecretAccessKey: "fake", SessionToken: "fake"}, nil
		}),
		HTTPClient: &http.Client{Transport: e},
	}
	return dynamodb.NewFromConfig(cfg)
}
