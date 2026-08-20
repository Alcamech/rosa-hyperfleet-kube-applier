package dynamodb

import "time"

const (
	// GSIShardCount is the number of shard buckets on the updateTime-index GSI.
	// MUST match the Terraform definition in:
	//   rosa-hyperfleet/terraform/modules/kube-applier-dynamodb/main.tf
	GSIShardCount = 4

	// GSIName is the DynamoDB GSI used for fast polling.
	// MUST match the Terraform definition.
	GSIName = "updateTime-index"

	// DefaultPollInterval is how often the fast GSI poll runs.
	DefaultPollInterval = 15 * time.Second

	// DefaultRelistInterval is how often the full consistent relist runs.
	// After each relist the expanding lookback window resets to near-zero.
	DefaultRelistInterval = 5 * time.Minute

	// startupRelistDelay is the pause between starting the relist goroutine
	// and the fast poll goroutine, giving the first relist time to seed the
	// cache before fast polls begin. Mirrors watcher.py lines 981-986.
	startupRelistDelay = 500 * time.Millisecond
)
