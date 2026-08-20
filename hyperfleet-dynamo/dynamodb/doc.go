// Package dynamodb implements a two-speed DynamoDB polling watcher.
//
// # Architecture
//
// The Watcher runs two concurrent loops that share an in-memory stub cache
// (documentID → updateTime):
//
//   - Fast poll loop (every PollInterval, default 15s): queries the
//     updateTime-index GSI across all shard buckets in parallel, deduplicates
//     results against the cache using updateTime, and emits doorbell callbacks
//     for changed document IDs.
//
//   - Full relist loop (every RelistInterval, default 5m): performs a
//     consistent full-table Scan, diffs the result against the cache to detect
//     additions, modifications, and deletions, and emits doorbell callbacks.
//     Resets the expanding lookback window (see below).
//
// # Doorbell Contract
//
// The watcher delivers only documentID strings. It never fetches or delivers
// full item content. Consumers are responsible for performing their own
// consistent GetItem after receiving a doorbell. This avoids double-reads and
// ensures consumers always act on strongly-consistent data.
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
// # Consumers
//
// Two consumers use this package:
//
//   - hyperfleet-operator: statusstream.Manager creates one Watcher per
//     (ManagementCluster, table-suffix) pair. The OnChange callback calls
//     EventRouter.Dispatch(documentID), which enqueues a GenericEvent to the
//     controller workqueue.
//
//   - kube-applier-aws: informers.go creates a WatchAdapter wrapping a Watcher
//     per specs table. The WatchAdapter implements watch.Interface for the
//     SharedIndexInformer.
package dynamodb
