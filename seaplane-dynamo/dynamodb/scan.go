package dynamodb

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// ScanAll performs a consistent full-table scan and returns a map of
// documentID → full raw DynamoDB attribute map for every item in the table.
//
// ConsistentRead=true ensures the relist sees the latest writes.
// No ProjectionExpression is set — all attributes are returned so callers can
// both update the stub cache (via extractDocumentIDAndUpdateTime) and deliver
// full item payloads to consumers via OnChange.
func ScanAll(
	ctx context.Context,
	client *dynamodb.Client,
	tableName string,
) (map[string]map[string]types.AttributeValue, error) {
	results := make(map[string]map[string]types.AttributeValue)
	var lastKey map[string]types.AttributeValue

	for {
		input := &dynamodb.ScanInput{
			TableName:         aws.String(tableName),
			ConsistentRead:    aws.Bool(true),
			ExclusiveStartKey: lastKey,
		}

		out, err := client.Scan(ctx, input)
		if err != nil {
			return nil, fmt.Errorf("scan %s: %w", tableName, err)
		}

		for _, item := range out.Items {
			docID, _, ok := extractDocumentIDAndUpdateTime(item)
			if !ok {
				continue
			}
			results[docID] = item
		}

		lastKey = out.LastEvaluatedKey
		if len(lastKey) == 0 {
			break
		}
	}
	return results, nil
}
