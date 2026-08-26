// Package dynamodb implements a two-speed DynamoDB polling watcher.
//
// # Architecture
//
// The Watcher runs two concurrent loops that share an in-memory stub cache
// (documentID → updateTime):
//
//   - Fast poll loop (every PollInterval, default 15s): queries the
//     updateTime-index GSI across all shard buckets in parallel, deduplicates
//     results against the cache using updateTime, fetches full item attribute
//     maps via BatchGetItem for changed documents, and delivers them via
//     OnChange(docID string, item Item).
//
//   - Full relist loop (every RelistInterval, default 5m): performs a
//     consistent full-table Scan (no ProjectionExpression — all attributes),
//     diffs the result against the cache to detect additions, modifications,
//     and hard deletions, and delivers full items via OnChange. For hard
//     deletes, item is nil. Resets the expanding lookback window (see below).
//
// # OnChange Contract
//
// OnChange is called with the full DynamoDB attribute map for the item:
//
//	type OnChange func(documentID string, item Item)
//
// item is nil only for hard deletes detected during a full relist (items that
// have disappeared from the table entirely). Fast-poll events always carry a
// non-nil item because BatchGetItem is used to fetch the full record.
//
// Consumers decide how to use the item:
//
//   - kube-applier-aws: the WatchAdapter passes item through decodeFn to
//     produce a typed runtime.Object (*ApplyDesire or *ReadDesire) and emits
//     a watch.Modified/Deleted event to the SharedIndexInformer.
//
//   - hyperfleet-operator: statusstream.Manager ignores item and dispatches
//     only documentID to the EventRouter, which enqueues a GenericEvent to
//     the controller workqueue. The controller performs its own consistent
//     GetItem when it reconciles.
//
// # Expanding Lookback Window
//
// Inspired by box-tricks/watcher.py. Immediately after a relist the fast-poll
// lookback window starts near zero and grows by elapsed real time on each tick,
// capped at RelistInterval. This dramatically reduces unnecessary scan costs
// under normal operation compared to a fixed-width window.
//
//	Before first relist:   lookback = RelistInterval  (safe full-window default)
//	t=0 after relist:      lookback ≈ 0
//	t=N*PollInterval:      lookback ≈ N*PollInterval
//	t≥RelistInterval:      lookback = RelistInterval
//
// # GSI Sharding
//
// The fast poll queries the updateTime-index GSI (hash_key=shard,
// range_key=updateTime). Items are assigned to shard buckets "0"–"3" by
// ComputeShard. All shards are queried in parallel. The shard count
// (GSIShardCount=4) MUST match the Terraform definition in
// rosa-hyperfleet/terraform/modules/kube-applier-dynamodb/main.tf.
//
// Items must have shard and updateTime attributes set at write time for the
// fast poller to see them.
package dynamodb

