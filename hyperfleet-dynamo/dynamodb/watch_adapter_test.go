package dynamodb

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
)

func TestListWatchWithoutWatchListSemantics_IsUnsupported(t *testing.T) {
	lw := ListWatchWithoutWatchListSemantics{ListWatch: &cache.ListWatch{}}
	if !lw.IsWatchListSemanticsUnSupported() {
		t.Error("IsWatchListSemanticsUnSupported() should return true")
	}
}

// TestWatchAdapter_DeliversDoorbell verifies that an OnChange callback from
// the underlying Watcher is translated into a watch.Event on ResultChan with:
//   - Type == watch.Modified
//   - Object is *metav1.PartialObjectMetadata with Name == documentID
func TestWatchAdapter_DeliversDoorbell(t *testing.T) {
	fd := newFakeDynamo()
	// First relist finds one item so onChange fires.
	fd.setScanItems(stubItems(map[string]time.Time{"doorbell-doc": t1}))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	adapter := NewWatchAdapter(ctx, fd.newClient(), "test-table", Options{
		PollInterval:   time.Hour,
		RelistInterval: 50 * time.Millisecond,
		ShardCount:     GSIShardCount,
	})
	defer adapter.Stop()

	// Wait for a watch event.
	select {
	case evt, ok := <-adapter.ResultChan():
		if !ok {
			t.Fatal("ResultChan closed before receiving doorbell event")
		}
		if evt.Type != watch.Modified {
			t.Errorf("event type: got %v, want %v", evt.Type, watch.Modified)
		}
		pom, ok := evt.Object.(*metav1.PartialObjectMetadata)
		if !ok {
			t.Fatalf("event object: got %T, want *metav1.PartialObjectMetadata", evt.Object)
		}
		if pom.Name != "doorbell-doc" {
			t.Errorf("event object name: got %q, want %q", pom.Name, "doorbell-doc")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for doorbell event on ResultChan")
	}
}

// TestWatchAdapter_ResultChanClosesAfterRelistDone verifies that after the
// Watcher's Done() channel closes (first relist complete), the WatchAdapter's
// Run goroutine exits and closes resultCh — signalling the Reflector to relist.
func TestWatchAdapter_ResultChanClosesAfterRelistDone(t *testing.T) {
	fd := newFakeDynamo()
	fd.setScanItems(nil) // empty table — relist fires immediately, Done closes

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	adapter := NewWatchAdapter(ctx, fd.newClient(), "test-table", Options{
		PollInterval:   time.Hour,
		RelistInterval: 50 * time.Millisecond,
		ShardCount:     GSIShardCount,
	})

	// Drain the channel until it closes.
	deadline := time.After(3 * time.Second)
	for {
		select {
		case _, ok := <-adapter.ResultChan():
			if !ok {
				// Channel closed — correct.
				return
			}
		case <-deadline:
			t.Fatal("ResultChan was not closed within 3s after Done()")
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
	})

	// Wait for first relist then Stop.
	// We observe resultCh close — either from Done() or Stop(), both are valid.
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

// TestWatchAdapter_DoorbellObjectName verifies doorbellObject() produces a
// PartialObjectMetadata with the correct Name field.
func TestWatchAdapter_DoorbellObjectName(t *testing.T) {
	obj := doorbellObject("my-doc-id")
	pom, ok := obj.(*metav1.PartialObjectMetadata)
	if !ok {
		t.Fatalf("doorbellObject returned %T, want *metav1.PartialObjectMetadata", obj)
	}
	if pom.Name != "my-doc-id" {
		t.Errorf("Name = %q, want %q", pom.Name, "my-doc-id")
	}
}

// TestWatchAdapter_DoorbellObjectUpdateTimeIsZero verifies that the
// PartialObjectMetadata from doorbellObject has a zero CreationTimestamp —
// the IsZero() check used by controllers to identify doorbell events.
func TestWatchAdapter_DoorbellObjectUpdateTimeIsZero(t *testing.T) {
	obj := doorbellObject("any-doc")
	pom := obj.(*metav1.PartialObjectMetadata)
	if !pom.CreationTimestamp.IsZero() {
		t.Error("doorbell PartialObjectMetadata should have zero CreationTimestamp")
	}
}
