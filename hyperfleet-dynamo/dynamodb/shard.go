package dynamodb

import (
	"fmt"
	"strconv"
	"strings"
)

// ComputeShard returns the shard bucket string ("0"–"3" for shardCount=4) for
// a documentID. The algorithm matches box-tricks/create_table_and_load.py:
//
//  1. Strip hyphens from the UUID string.
//  2. Parse the first 8 hex characters as a base-16 uint64.
//  3. Take modulo shardCount and return as a string.
//
// The shard value written to DynamoDB items and queried via the GSI must use
// the same algorithm. ComputeShardDefault uses GSIShardCount (4).
func ComputeShard(documentID string, shardCount int) string {
	stripped := strings.ReplaceAll(documentID, "-", "")
	if len(stripped) < 8 {
		return "0"
	}
	v, err := strconv.ParseUint(stripped[:8], 16, 64)
	if err != nil {
		return "0"
	}
	return fmt.Sprintf("%d", int(v)%shardCount)
}

// ComputeShardDefault calls ComputeShard with GSIShardCount.
func ComputeShardDefault(documentID string) string {
	return ComputeShard(documentID, GSIShardCount)
}
