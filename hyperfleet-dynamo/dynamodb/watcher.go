package dynamodb

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/go-logr/logr"
)

// Item is a raw DynamoDB attribute map as returned by GetItem, BatchGetItem,
// or Scan. It is the full representation of a single table item.
type Item = map[string]types.AttributeValue

// OnChange is called whenever the watcher detects that an item was added,
// modified, or deleted.
//
//   - For additions and modifications, item contains the full DynamoDB
//     attribute map fetched from the base table.
//   - For deletions (detected by the relist only), item is nil.
//
// Implementations must be non-blocking or fast; the watcher does not run
// callbacks concurrently.
type OnChange func(documentID string, item Item)

// Options configures a Watcher. Zero values use the package defaults.
type Options struct {
	// PollInterval is how often the fast GSI poll runs.
	// Defaults to DefaultPollInterval (15s).
	PollInterval time.Duration

	// RelistInterval is how often the full consistent relist runs.
	// Defaults to DefaultRelistInterval (5m).
	RelistInterval time.Duration

	// MaxLookbackWindow caps the expanding lookback window used by the fast
	// GSI poller. It must be <= RelistInterval; if zero or larger than
	// RelistInterval it defaults to RelistInterval.
	//
	// Shortening this below RelistInterval is useful when you want tight
	// polling without querying a very large time range on the GSI.
	MaxLookbackWindow time.Duration

	// ShardCount is the number of GSI shard buckets to query.
	// Defaults to GSIShardCount (4).
	ShardCount int

	// StartupDelay is the pause between starting the relist goroutine and the
	// fast poll goroutine, giving the first relist time to seed the cache
	// before fast polls begin. Defaults to startupRelistDelay (500ms).
	// Set to a small value in tests to avoid slow startup.
	StartupDelay time.Duration

	// Logger is used for structured logging. If the zero value is passed,
	// logr.Discard() is used. Callers should pass klog.Background() (kube-applier)
	// or ctrl.Log (operator) so logs flow through the standard k8s logging pipeline.
	Logger logr.Logger
}

func (o Options) pollInterval() time.Duration {
	if o.PollInterval <= 0 {
		return DefaultPollInterval
	}
	return o.PollInterval
}

func (o Options) relistInterval() time.Duration {
	if o.RelistInterval <= 0 {
		return DefaultRelistInterval
	}
	return o.RelistInterval
}

func (o Options) maxLookbackWindow() time.Duration {
	relist := o.relistInterval()
	if o.MaxLookbackWindow <= 0 || o.MaxLookbackWindow > relist {
		return relist
	}
	return o.MaxLookbackWindow
}

func (o Options) shardCount() int {
	if o.ShardCount <= 0 {
		return GSIShardCount
	}
	return o.ShardCount
}

func (o Options) startupDelay() time.Duration {
	if o.StartupDelay <= 0 {
		return startupRelistDelay
	}
	return o.StartupDelay
}

func (o Options) logger() logr.Logger {
	if o.Logger.GetSink() == nil {
		return logr.Discard()
	}
	return o.Logger
}

// Watcher implements the two-speed DynamoDB polling watcher described in the
// package documentation. It delivers full item payloads (or nil for deletes)
// via OnChange for items that have been added, modified, or deleted.
//
// Call Run to start the watcher. It blocks until Stop is called or the context
// is cancelled. Done returns a channel that is closed after the first relist
// completes; Managers use this to detect natural expiry and restart.
type Watcher struct {
	client    *dynamodb.Client
	tableName string
	onChange  OnChange
	opts      Options
	cache     *Cache
	stopCh    chan struct{}
	doneCh    chan struct{}
}

// New creates a Watcher. Call Run to start it.
func New(client *dynamodb.Client, tableName string, onChange OnChange, opts Options) *Watcher {
	return &Watcher{
		client:    client,
		tableName: tableName,
		onChange:  onChange,
		opts:      opts,
		cache:     newCache(),
		stopCh:    make(chan struct{}),
		doneCh:    make(chan struct{}),
	}
}

// Done returns a channel that is closed after the first full relist completes.
// statusstream.Manager uses this to detect when a Watcher has finished its
// first relist cycle and should be replaced with a fresh instance (maintaining
// the unconditional-relist guarantee).
func (w *Watcher) Done() <-chan struct{} {
	return w.doneCh
}

// Stop signals the watcher to stop and waits for it to exit cleanly.
func (w *Watcher) Stop() {
	close(w.stopCh)
}

// Run starts the watcher's two loops and blocks until the context is cancelled
// or Stop is called.
//
// Startup sequence (mirrors watcher.py lines 981-986):
//  1. Relist goroutine starts immediately.
//  2. After startupRelistDelay (500ms), fast poll goroutine starts.
//
// This ensures the cache is seeded from a consistent scan before the fast poll
// begins, so the first fast poll has a populated cache to dedup against.
func (w *Watcher) Run(ctx context.Context) {
	log := w.opts.logger().WithValues("table", w.tableName, "component", "hyperfleet-dynamo/watcher")
	log.Info("watcher starting",
		"pollInterval", w.opts.pollInterval(),
		"relistInterval", w.opts.relistInterval(),
		"maxLookback", w.opts.maxLookbackWindow(),
		"shardCount", w.opts.shardCount(),
	)

	relistDone := make(chan struct{})
	pollDone := make(chan struct{})

	go func() {
		defer close(relistDone)
		w.relistLoop(ctx)
	}()

	// Give the relist goroutine a head start so the first relist can seed the
	// cache before the fast poll fires.
	select {
	case <-ctx.Done():
		<-relistDone
		return
	case <-w.stopCh:
		<-relistDone
		return
	case <-time.After(w.opts.startupDelay()):
	}

	go func() {
		defer close(pollDone)
		w.fastPollLoop(ctx)
	}()

	<-relistDone
	<-pollDone
}

// relistLoop runs the full consistent relist on a ticker. After the first
// relist it closes doneCh to signal natural expiry to the Manager.
func (w *Watcher) relistLoop(ctx context.Context) {
	log := w.opts.logger().WithValues("table", w.tableName, "component", "hyperfleet-dynamo/relist")
	ticker := time.NewTicker(w.opts.relistInterval())
	defer ticker.Stop()

	firstRelist := true

	log.Info("relist loop started", "interval", w.opts.relistInterval())

	for {
		select {
		case <-ctx.Done():
			log.Info("relist loop stopped")
			return
		case <-w.stopCh:
			log.Info("relist loop stopped")
			return
		case <-ticker.C:
			log.Info("relist tick firing")
			if w.runRelist(ctx, log) && firstRelist {
				firstRelist = false
				close(w.doneCh)
			}
		}
	}
}

func (w *Watcher) runRelist(ctx context.Context, log logr.Logger) bool {
	// Full consistent scan — returns complete item attribute maps.
	fullItems, err := ScanAll(ctx, w.client, w.tableName)
	if err != nil {
		log.Error(err, "relist scan failed")
		return false
	}

	// Extract stubs (documentID → updateTime) for cache diffing.
	stubs := make(map[string]time.Time, len(fullItems))
	for docID, item := range fullItems {
		_, ut, ok := extractDocumentIDAndUpdateTime(item)
		if ok {
			stubs[docID] = ut
		}
	}

	added, modified, deleted := w.cache.ApplyRelist(stubs)
	log.Info("relist complete",
		"scanned", len(fullItems),
		"added", len(added),
		"modified", len(modified),
		"deleted", len(deleted),
	)

	for _, docID := range added {
		w.onChange(docID, fullItems[docID])
	}
	for _, docID := range modified {
		w.onChange(docID, fullItems[docID])
	}
	// Deleted items are gone from the table — deliver nil to signal deletion.
	for _, docID := range deleted {
		w.onChange(docID, nil)
	}
	return true
}

// fastPollLoop queries the GSI on a ticker using the expanding lookback window.
func (w *Watcher) fastPollLoop(ctx context.Context) {
	log := w.opts.logger().WithValues("table", w.tableName, "component", "hyperfleet-dynamo/poll")
	ticker := time.NewTicker(w.opts.pollInterval())
	defer ticker.Stop()

	log.Info("fast poll loop started", "interval", w.opts.pollInterval())

	for {
		select {
		case <-ctx.Done():
			log.Info("fast poll loop stopped")
			return
		case <-w.stopCh:
			log.Info("fast poll loop stopped")
			return
		case <-ticker.C:
			w.runFastPoll(ctx, log)
		}
	}
}

func (w *Watcher) runFastPoll(ctx context.Context, log logr.Logger) {
	lookback := w.cache.EffectiveLookback(w.opts.maxLookbackWindow())
	since := time.Now().UTC().Add(-lookback)

	log.Info("fast poll tick", "lookback", lookback, "since", since.Format(time.RFC3339))

	// Step 1: query the GSI for stubs (documentID + updateTime) of recently
	// updated items.
	stubs, err := QuerySince(ctx, w.client, w.tableName, since, w.opts.shardCount())
	if err != nil {
		log.Error(err, "GSI fast poll failed")
		return
	}

	log.Info("fast poll GSI result", "stubs", len(stubs))

	// Step 2: dedup against the stub cache — only proceed for changed IDs.
	changed := w.cache.FindChanged(stubs)
	if len(changed) == 0 {
		return
	}

	// Step 3: BatchGet full items for the changed IDs from the base table.
	fetched, err := BatchGetItems(ctx, w.client, w.tableName, changed)
	if err != nil {
		log.Error(err, "BatchGetItems failed")
		return
	}

	// Step 4: update the stub cache for items we successfully fetched.
	// Items absent from fetched were deleted between the GSI query and the
	// BatchGet; the relist will detect and deliver those deletions.
	fetchedStubs := make(map[string]time.Time, len(fetched))
	for docID, item := range fetched {
		_, ut, ok := extractDocumentIDAndUpdateTime(item)
		if ok {
			fetchedStubs[docID] = ut
		}
	}
	w.cache.ApplyStubs(fetchedStubs)

	log.Info("fast poll found changes", "count", len(changed), "fetched", len(fetched), "docIDs", changed)

	// Step 5: deliver full items to the consumer. Skip IDs that vanished
	// from the BatchGet response (deleted); the relist handles those.
	for _, docID := range changed {
		item, ok := fetched[docID]
		if !ok {
			log.Info("fast poll: item absent from BatchGet (likely deleted, relist will confirm)", "documentID", docID)
			continue
		}
		w.onChange(docID, item)
	}
}
