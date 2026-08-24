package dynamodb

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// ListWatchWithoutWatchListSemantics wraps a *cache.ListWatch and signals to
// the client-go Reflector that WatchList bookmark semantics are not supported.
// Without this the Reflector deadlocks waiting for a bookmark event that
// DynamoDB never emits.
type ListWatchWithoutWatchListSemantics struct {
	*cache.ListWatch
}

func (ListWatchWithoutWatchListSemantics) IsWatchListSemanticsUnSupported() bool {
	return true
}

// WatchAdapter wraps a Watcher and implements watch.Interface. It translates
// OnChange callbacks into watch.Events suitable for a SharedIndexInformer.
//
// The decodeFn converts a raw DynamoDB Item into the typed runtime.Object
// expected by the Reflector (e.g. *kubeapplier.ApplyDesire). This ensures
// watch.Event.Object is always the correct concrete type, which is required by
// the Reflector's type-check at reflector.go:1014.
//
// For modifications and additions, a watch.Modified event is emitted with the
// fully decoded object. For deletions (item == nil, relist only), a
// watch.Deleted event is emitted carrying a zero-value tombstone object with
// only Name set so the Reflector can remove the correct entry from the store.
//
// WatchAdapter is created fresh for each WatchFuncWithContext call by the
// SharedIndexInformer. A new underlying Watcher is created on each call, so
// the expanding lookback window restarts correctly after each informer relist.
//
// The resultChan is closed when the underlying Watcher's Run returns,
// signalling the Reflector to perform a fresh List and then call
// WatchFuncWithContext again.
type WatchAdapter struct {
	watcher  *Watcher
	resultCh chan watch.Event
}

// NewWatchAdapter creates a WatchAdapter backed by a new Watcher and
// immediately starts the watcher. decodeFn must convert a raw DynamoDB Item
// into the concrete runtime.Object type expected by the Reflector (e.g.
// *kubeapplier.ApplyDesire or *kubeapplier.ReadDesire). The caller must call
// Stop when done.
func NewWatchAdapter(
	ctx context.Context,
	client *dynamodb.Client,
	tableName string,
	opts Options,
	decodeFn func(Item) (runtime.Object, error),
) *WatchAdapter {
	a := &WatchAdapter{
		resultCh: make(chan watch.Event, 100),
	}

	a.watcher = New(client, tableName, func(docID string, item Item) {
		var event watch.Event

		if item == nil {
			// Deletion detected by the relist. Emit a Deleted event carrying
			// a zero-value tombstone with Name=documentID so the Reflector can
			// locate and remove the correct store entry.
			event = watch.Event{
				Type:   watch.Deleted,
				Object: tombstoneObject(docID, decodeFn),
			}
		} else {
			obj, err := decodeFn(item)
			if err != nil {
				opts.logger().Error(err, "WatchAdapter: failed to decode item, skipping",
					"table", tableName, "documentID", docID)
				return
			}
			event = watch.Event{
				Type:   watch.Modified,
				Object: obj,
			}
		}

		select {
		case a.resultCh <- event:
		default:
			// Channel full — drop. The relist safety net will catch it.
			opts.logger().Info("WatchAdapter: result channel full, dropping event",
				"table", tableName, "documentID", docID, "type", event.Type)
		}
	}, opts)

	go func() {
		a.watcher.Run(ctx)
		// Watcher exited (ctx cancelled or Stop called). Close resultCh to
		// signal the Reflector that this watch has ended and it should relist.
		close(a.resultCh)
	}()

	return a
}

// ResultChan implements watch.Interface.
func (a *WatchAdapter) ResultChan() <-chan watch.Event {
	return a.resultCh
}

// Stop implements watch.Interface. Stops the underlying Watcher.
func (a *WatchAdapter) Stop() {
	a.watcher.Stop()
}

// tombstoneObject returns a zero-value instance of the type produced by
// decodeFn with only ObjectMeta.Name set to documentID. It is used as the
// Object in watch.Deleted events so the Reflector can key into the store and
// remove the correct entry. We call decodeFn with a minimal attribute map
// containing just documentID; if that fails we fall back to PartialObjectMetadata
// (the Reflector will log a type warning but still process the deletion via the
// DeletionHandling path).
func tombstoneObject(documentID string, decodeFn func(Item) (runtime.Object, error)) runtime.Object {
	// Provide a minimal item so decodeFn can construct a zero-value typed
	// object. The only field that matters for the Reflector's deletion path
	// is the object's Name (derived from documentID).
	minimalItem := Item{
		"documentID": &types.AttributeValueMemberS{Value: documentID},
	}
	obj, err := decodeFn(minimalItem)
	if err != nil {
		// Fallback: PartialObjectMetadata carries the name; the Reflector will
		// log a type mismatch warning but the DeletionHandling in the informer
		// will still remove the item from the store.
		return &metav1.PartialObjectMetadata{
			ObjectMeta: metav1.ObjectMeta{Name: documentID},
		}
	}
	return obj
}
