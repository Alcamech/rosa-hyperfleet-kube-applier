package database

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	kubeapplier "github.com/openshift-online/rosa-hyperfleet-kube-applier/api/kubeapplier"
	hd "github.com/openshift-online/rosa-hyperfleet-kube-applier/hyperfleet-dynamo/dynamodb"
)

// desire is the type constraint for the generic CRUD implementations. It
// requires DynamoDB metadata access, KubeContent access (for the manual
// RawExtension codec), and DeepCopy.
type desire[T any] interface {
	*T
	DynamoDBMetadataAccessor
	KubeContentAccessor
	DeepCopy() *T
}

// attributeDocumentID is the DynamoDB partition key attribute name.
const attributeDocumentID = "documentID"

// isConditionalCheckFailed returns true when DynamoDB rejected the write
// because the ConditionExpression was not satisfied.
func isConditionalCheckFailed(err error) bool {
	var ccf *types.ConditionalCheckFailedException
	return errors.As(err, &ccf)
}

// itemToDesire converts a raw DynamoDB item map into a typed desire value.
// It uses attributevalue.UnmarshalMap for the struct fields and then manually
// reads the documentID partition key and kubeContent string attributes.
func itemToDesire[T any, PT desire[T]](item map[string]types.AttributeValue) (*T, error) {
	var obj T
	if err := attributevalue.UnmarshalMap(item, &obj); err != nil {
		return nil, fmt.Errorf("unmarshal item: %w", err)
	}
	pt := PT(&obj)

	// Extract the partition key (documentID) — not in the struct body because
	// the struct field carries dynamodbav:"-".
	if av, ok := item[attributeDocumentID]; ok {
		if sv, ok := av.(*types.AttributeValueMemberS); ok {
			pt.SetDocumentID(sv.Value)
		}
	}

	// Read kubeContent string attributes.
	if err := kubeContentReadFromItem(pt, item); err != nil {
		return nil, err
	}
	return &obj, nil
}

// desireToItem converts a typed desire value to a DynamoDB item map ready for
// PutItem. The documentID partition key is added explicitly. kubeContent
// fields are merged in as S attributes. The shard attribute is computed and
// added so the updateTime-index GSI can index the item.
func desireToItem[T any, PT desire[T]](pt PT) (map[string]types.AttributeValue, error) {
	item, err := attributevalue.MarshalMap(pt)
	if err != nil {
		return nil, fmt.Errorf("marshal item: %w", err)
	}
	// Explicitly set the partition key.
	item[attributeDocumentID] = &types.AttributeValueMemberS{Value: pt.GetDocumentID()}

	// Set the shard attribute for the updateTime-index GSI.
	// GSIShardCount must match the Terraform definition in
	// rosa-hyperfleet/terraform/modules/kube-applier-dynamodb/main.tf.
	item["shard"] = &types.AttributeValueMemberS{Value: hd.ComputeShardDefault(pt.GetDocumentID())}

	// Merge kubeContent string attributes.
	kubeAttrs, err := kubeContentAttributeValues(pt)
	if err != nil {
		return nil, err
	}
	for k, v := range kubeAttrs {
		item[k] = v
	}
	return item, nil
}

// -------------------------------------------------------------------
// dynamoDBSpecReader — implements SpecReader[T] (read-only)
// -------------------------------------------------------------------

type dynamoDBSpecReader[T any, PT desire[T]] struct {
	client *dynamodb.Client
	table  string
}

func (r *dynamoDBSpecReader[T, PT]) Get(ctx context.Context, documentID string) (*T, error) {
	out, err := r.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      aws.String(r.table),
		ConsistentRead: aws.Bool(true),
		Key: map[string]types.AttributeValue{
			attributeDocumentID: &types.AttributeValueMemberS{Value: documentID},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("dynamodb Get %s/%s: %w", r.table, documentID, err)
	}
	if len(out.Item) == 0 {
		return nil, NewNotFoundError()
	}
	return itemToDesire[T, PT](out.Item)
}

func (r *dynamoDBSpecReader[T, PT]) List(ctx context.Context) ([]*T, error) {
	var result []*T
	paginator := dynamodb.NewScanPaginator(r.client, &dynamodb.ScanInput{
		TableName:      aws.String(r.table),
		ConsistentRead: aws.Bool(true),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("dynamodb Scan %s: %w", r.table, err)
		}
		for _, item := range page.Items {
			obj, err := itemToDesire[T, PT](item)
			if err != nil {
				return nil, fmt.Errorf("dynamodb Scan %s convert: %w", r.table, err)
			}
			result = append(result, obj)
		}
	}
	return result, nil
}

// -------------------------------------------------------------------
// dynamoDBDesireCRUD — implements ResourceCRUD[T]
// -------------------------------------------------------------------

type dynamoDBDesireCRUD[T any, PT desire[T]] struct {
	client *dynamodb.Client
	table  string
}

func (c *dynamoDBDesireCRUD[T, PT]) Get(ctx context.Context, documentID string) (*T, error) {
	out, err := c.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      aws.String(c.table),
		ConsistentRead: aws.Bool(true),
		Key: map[string]types.AttributeValue{
			attributeDocumentID: &types.AttributeValueMemberS{Value: documentID},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("dynamodb Get %s/%s: %w", c.table, documentID, err)
	}
	if len(out.Item) == 0 {
		return nil, NewNotFoundError()
	}
	return itemToDesire[T, PT](out.Item)
}

func (c *dynamoDBDesireCRUD[T, PT]) List(ctx context.Context) ([]*T, error) {
	var result []*T
	paginator := dynamodb.NewScanPaginator(c.client, &dynamodb.ScanInput{
		TableName:      aws.String(c.table),
		ConsistentRead: aws.Bool(true),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("dynamodb Scan %s: %w", c.table, err)
		}
		for _, item := range page.Items {
			obj, err := itemToDesire[T, PT](item)
			if err != nil {
				return nil, fmt.Errorf("dynamodb Scan %s convert: %w", c.table, err)
			}
			result = append(result, obj)
		}
	}
	return result, nil
}

func (c *dynamoDBDesireCRUD[T, PT]) Create(ctx context.Context, obj *T) (*T, error) {
	pt := PT(obj)
	docID := pt.GetDocumentID()
	if docID == "" {
		return nil, fmt.Errorf("dynamodb Create %s: DocumentID is empty", c.table)
	}

	now := time.Now().UTC()
	out := pt.DeepCopy()
	op := PT(out)
	op.SetVersion(1)
	op.SetUpdateTime(now)
	op.SetCreateTime(now)

	item, err := desireToItem[T, PT](op)
	if err != nil {
		return nil, fmt.Errorf("dynamodb Create %s/%s: %w", c.table, docID, err)
	}

	_, err = c.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:           aws.String(c.table),
		Item:                item,
		ConditionExpression: aws.String("attribute_not_exists(#pk)"),
		ExpressionAttributeNames: map[string]string{
			"#pk": attributeDocumentID,
		},
	})
	if err != nil {
		if isConditionalCheckFailed(err) {
			return nil, NewAlreadyExistsError()
		}
		return nil, fmt.Errorf("dynamodb Create %s/%s: %w", c.table, docID, err)
	}
	return out, nil
}

func (c *dynamoDBDesireCRUD[T, PT]) Replace(ctx context.Context, obj *T) (*T, error) {
	pt := PT(obj)
	docID := pt.GetDocumentID()
	expectedVersion := pt.GetVersion()

	out := pt.DeepCopy()
	op := PT(out)
	op.SetVersion(expectedVersion + 1)
	op.SetUpdateTime(time.Now().UTC())

	item, err := desireToItem[T, PT](op)
	if err != nil {
		return nil, fmt.Errorf("dynamodb Replace %s/%s: %w", c.table, docID, err)
	}

	_, err = c.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:           aws.String(c.table),
		Item:                item,
		ConditionExpression: aws.String("#v = :expected"),
		ExpressionAttributeNames: map[string]string{
			"#v": "version",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":expected": &types.AttributeValueMemberN{Value: strconv.FormatInt(expectedVersion, 10)},
		},
	})
	if err != nil {
		if isConditionalCheckFailed(err) {
			return nil, NewPreconditionFailedError()
		}
		return nil, fmt.Errorf("dynamodb Replace %s/%s: %w", c.table, docID, err)
	}
	return out, nil
}

func (c *dynamoDBDesireCRUD[T, PT]) Delete(ctx context.Context, documentID string) error {
	_, err := c.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(c.table),
		Key: map[string]types.AttributeValue{
			attributeDocumentID: &types.AttributeValueMemberS{Value: documentID},
		},
	})
	if err != nil {
		return fmt.Errorf("dynamodb Delete %s/%s: %w", c.table, documentID, err)
	}
	return nil
}

// --- Exported conversion helpers used by the informers package ---

// ItemToApplyDesire converts a raw DynamoDB attribute map (from a table item or
// stream image) to an *ApplyDesire.
func ItemToApplyDesire(item map[string]types.AttributeValue) (*kubeapplier.ApplyDesire, error) {
	return itemToDesire[kubeapplier.ApplyDesire, *kubeapplier.ApplyDesire](item)
}

// ItemToReadDesire converts a raw DynamoDB attribute map to a *ReadDesire.
func ItemToReadDesire(item map[string]types.AttributeValue) (*kubeapplier.ReadDesire, error) {
	return itemToDesire[kubeapplier.ReadDesire, *kubeapplier.ReadDesire](item)
}
