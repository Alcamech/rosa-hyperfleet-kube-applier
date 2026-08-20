package dynamodb

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// QuerySince queries the updateTime-index GSI across all shard buckets in
// parallel and returns a map of documentID → updateTime for items updated after
// since.
//
// Uses eventually-consistent reads (ConsistentRead is not supported on GSIs).
// Only "documentID" and "updateTime" attributes are projected — callers use
// BatchGetItems to fetch full item payloads for changed IDs.
func QuerySince(
	ctx context.Context,
	client *dynamodb.Client,
	tableName string,
	since time.Time,
	shardCount int,
) (map[string]time.Time, error) {
	sinceStr := since.UTC().Format(time.RFC3339)

	var mu sync.Mutex
	results := make(map[string]time.Time)

	var wg sync.WaitGroup
	errs := make([]error, shardCount)

	for s := 0; s < shardCount; s++ {
		s := s
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := queryShard(ctx, client, tableName, fmt.Sprintf("%d", s), sinceStr, &mu, results); err != nil {
				errs[s] = err
			}
		}()
	}
	wg.Wait()

	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}
	return results, nil
}

func queryShard(
	ctx context.Context,
	client *dynamodb.Client,
	tableName string,
	shard string,
	sinceStr string,
	mu *sync.Mutex,
	results map[string]time.Time,
) error {
	var lastKey map[string]types.AttributeValue
	for {
		input := &dynamodb.QueryInput{
			TableName:              aws.String(tableName),
			IndexName:              aws.String(GSIName),
			KeyConditionExpression: aws.String("#s = :shard AND #t > :since"),
			ExpressionAttributeNames: map[string]string{
				"#s": "shard",
				"#t": "updateTime",
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":shard": &types.AttributeValueMemberS{Value: shard},
				":since": &types.AttributeValueMemberS{Value: sinceStr},
			},
			ProjectionExpression: aws.String("documentID, updateTime"),
			ExclusiveStartKey:    lastKey,
		}

		out, err := client.Query(ctx, input)
		if err != nil {
			return fmt.Errorf("GSI query shard %s of %s: %w", shard, tableName, err)
		}

		mu.Lock()
		for _, item := range out.Items {
			docID, ut, ok := extractDocumentIDAndUpdateTime(item)
			if !ok {
				continue
			}
			results[docID] = ut
		}
		mu.Unlock()

		lastKey = out.LastEvaluatedKey
		if len(lastKey) == 0 {
			break
		}
	}
	return nil
}

const batchGetMax = 100

// BatchGetItems fetches full items for the given documentIDs from the base
// table. Returns a map of documentID → raw attribute map for items that exist.
// Items absent from the response were deleted between the GSI query and the
// BatchGet; the relist will detect and deliver those deletions on its next
// cycle.
//
// Uses eventually-consistent reads to halve RCU cost; any stale reads are
// corrected on the next poll tick or relist.
func BatchGetItems(
	ctx context.Context,
	client *dynamodb.Client,
	tableName string,
	docIDs []string,
) (map[string]map[string]types.AttributeValue, error) {
	results := make(map[string]map[string]types.AttributeValue, len(docIDs))

	for i := 0; i < len(docIDs); i += batchGetMax {
		end := i + batchGetMax
		if end > len(docIDs) {
			end = len(docIDs)
		}
		chunk := docIDs[i:end]

		keys := make([]map[string]types.AttributeValue, len(chunk))
		for j, id := range chunk {
			keys[j] = map[string]types.AttributeValue{
				"documentID": &types.AttributeValueMemberS{Value: id},
			}
		}

		remaining := map[string]types.KeysAndAttributes{
			tableName: {Keys: keys},
		}

		for len(remaining) > 0 {
			out, err := client.BatchGetItem(ctx, &dynamodb.BatchGetItemInput{
				RequestItems: remaining,
			})
			if err != nil {
				return nil, fmt.Errorf("BatchGetItem %s: %w", tableName, err)
			}

			for _, item := range out.Responses[tableName] {
				docID, _, ok := extractDocumentIDAndUpdateTime(item)
				if !ok {
					continue
				}
				results[docID] = item
			}

			// Retry any unprocessed keys (throttling).
			remaining = out.UnprocessedKeys
		}
	}

	return results, nil
}

// extractDocumentIDAndUpdateTime reads the documentID and updateTime attributes
// from a DynamoDB item returned by GSI query, Scan, or BatchGetItem.
func extractDocumentIDAndUpdateTime(item map[string]types.AttributeValue) (string, time.Time, bool) {
	docAV, ok := item["documentID"]
	if !ok {
		return "", time.Time{}, false
	}
	docS, ok := docAV.(*types.AttributeValueMemberS)
	if !ok {
		return "", time.Time{}, false
	}

	utAV, ok := item["updateTime"]
	if !ok {
		return "", time.Time{}, false
	}
	utS, ok := utAV.(*types.AttributeValueMemberS)
	if !ok {
		return "", time.Time{}, false
	}

	ut, err := time.Parse(time.RFC3339, utS.Value)
	if err != nil {
		return "", time.Time{}, false
	}

	return docS.Value, ut.UTC(), true
}
