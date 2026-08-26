package dynamodb

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// testDecodeFn is a minimal decodeFn that returns a PartialObjectMetadata
// keyed by documentID. Used in WatchAdapter tests where the exact object type
// does not matter — only that a correctly-typed non-nil object is emitted.
func testDecodeFn(item Item) (runtime.Object, error) {
	docID := ""
	if v, ok := item["documentID"]; ok {
		if s, ok := v.(*types.AttributeValueMemberS); ok {
			docID = s.Value
		}
	}
	return &metav1.PartialObjectMetadata{
		ObjectMeta: metav1.ObjectMeta{Name: docID},
	}, nil
}

func TestListWatchWithoutWatchListSemantics_IsUnsupported(t *testing.T) {
	lw := ListWatchWithoutWatchListSemantics{ListWatch: &cache.ListWatch{}}
	if !lw.IsWatchListSemanticsUnSupported() {
		t.Error("IsWatchListSemanticsUnSupported() should return true")
	}
}

// TestWatchAdapter_DeliversModified verifies that an OnChange callback from
// the underlying Watcher is translated into a watch.Modified event on
// ResultChan carrying a correctly-typed runtime.Object.
func TestWatchAdapter_DeliversModified(t *testing.T) {
	fd := newFakeDynamo()
	fd.setScanItems(stubItems(map[string]time.Time{"doc-a": t1}))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	adapter := NewWatchAdapter(ctx, fd.newClient(), "test-table", Options{
		PollInterval:   time.Hour,
		RelistInterval: 50 * time.Millisecond,
		ShardCount:     GSIShardCount,
	}, testDecodeFn)
	defer adapter.Stop()

	select {
	case evt, ok := <-adapter.ResultChan():
		if !ok {
			t.Fatal("ResultChan closed before receiving event")
		}
		if evt.Type != watch.Modified {
			t.Errorf("event type: got %v, want %v", evt.Type, watch.Modified)
		}
		pom, ok := evt.Object.(*metav1.PartialObjectMetadata)
		if !ok {
			t.Fatalf("event object: got %T, want *metav1.PartialObjectMetadata", evt.Object)
		}
		if pom.Name != "doc-a" {
			t.Errorf("event object name: got %q, want %q", pom.Name, "doc-a")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for Modified event on ResultChan")
	}
}

// TestWatchAdapter_ResultChanClosesOnCtxCancel verifies that cancelling the
// context causes the WatchAdapter's Run goroutine to exit and close resultCh.
func TestWatchAdapter_ResultChanClosesOnCtxCancel(t *testing.T) {
	fd := newFakeDynamo()
	fd.setScanItems(nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	adapter := NewWatchAdapter(ctx, fd.newClient(), "test-table", Options{
		PollInterval:   time.Hour,
		RelistInterval: 50 * time.Millisecond,
		ShardCount:     GSIShardCount,
	}, testDecodeFn)

	// Cancel the context to drive Run to exit.
	cancel()

	deadline := time.After(3 * time.Second)
	for {
		select {
		case _, ok := <-adapter.ResultChan():
			if !ok {
				return // channel closed as expected
			}
		case <-deadline:
			t.Fatal("ResultChan was not closed within 3s after ctx cancel")
		}
	}
}

// TestWatchAdapter_DeliversDeleted verifies that when a relist detects a hard
// delete (item present in first scan, absent in second), the WatchAdapter emits
// a watch.Deleted event on ResultChan carrying a tombstone object whose Name
// equals the deleted documentID.
//
// This exercises the tombstoneObject → watch.Deleted code path in the OnChange
// callback, which is not covered by the Modified test above.
func TestWatchAdapter_DeliversDeleted(t *testing.T) {
	fd := newFakeDynamo()
	// Eager relist: doc-deleted is present.
	fd.setScanItems(stubItems(map[string]time.Time{"doc-deleted": t1}))
	// Second relist tick: doc-deleted is gone from the table.
	fd.setScanItems(nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	adapter := NewWatchAdapter(ctx, fd.newClient(), "test-table", Options{
		PollInterval:   time.Hour,
		RelistInterval: 30 * time.Millisecond,
		ShardCount:     GSIShardCount,
	}, testDecodeFn)
	defer adapter.Stop()

	// Collect events until we see a Deleted or time out.
	deadline := time.After(3 * time.Second)
	for {
		select {
		case evt, ok := <-adapter.ResultChan():
			if !ok {
				t.Fatal("ResultChan closed before receiving Deleted event")
			}
			if evt.Type != watch.Deleted {
				// Skip non-Deleted events (e.g. the Modified from the eager relist).
				continue
			}
			pom, ok := evt.Object.(*metav1.PartialObjectMetadata)
			if !ok {
				t.Fatalf("Deleted event object: got %T, want *metav1.PartialObjectMetadata", evt.Object)
			}
			if pom.Name != "doc-deleted" {
				t.Errorf("Deleted event name: got %q, want %q", pom.Name, "doc-deleted")
			}
			return
		case <-deadline:
			t.Fatal("timed out waiting for Deleted event on ResultChan")
		}
	}
}

// TestWatchAdapter_Stop_ClosesResultChan verifies that calling Stop() on the
// adapter causes the underlying Watcher to stop and resultCh to close.
func TestWatchAdapter_Stop_ClosesResultChan(t *testing.T) {
	fd := newFakeDynamo()
	fd.setScanItems(nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	adapter := NewWatchAdapter(ctx, fd.newClient(), "test-table", Options{
		PollInterval:   time.Hour,
		RelistInterval: 50 * time.Millisecond,
		ShardCount:     GSIShardCount,
	}, testDecodeFn)

	adapter.Stop()

	deadline := time.After(3 * time.Second)
	for {
		select {
		case _, ok := <-adapter.ResultChan():
			if !ok {
				return // channel closed as expected
			}
		case <-deadline:
			t.Fatal("ResultChan not closed within 3s after Stop()")
		}
	}
}
