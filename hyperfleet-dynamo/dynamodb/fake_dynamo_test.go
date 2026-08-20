package dynamodb

// fakeDynamo provides a round-trip fake for the DynamoDB HTTP API used in
// unit tests. It intercepts Scan and Query calls and returns configurable
// JSON responses so that ScanAll, QuerySince, and the full Watcher engine can
// be tested without a real AWS endpoint or LocalStack.
//
// Usage:
//
//	fd := newFakeDynamo()
//	fd.setScanItems(items)             // items returned by the next Scan call
//	fd.setQueryItems(shard, items)     // items returned by Query for a given shard
//	client := dynamodb.NewFromConfig(fd.awsConfig())

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// dynamoItem is a helper to build DynamoDB JSON attribute maps for S (string)
// values — the only attribute type used by this package.
func dynamoItem(docID, updateTime string) map[string]any {
	return map[string]any{
		"documentID": map[string]any{"S": docID},
		"updateTime": map[string]any{"S": updateTime},
	}
}

// fakeDynamo is a configurable fake DynamoDB HTTP backend.
type fakeDynamo struct {
	mu sync.Mutex

	// Scan responses: each call pops the first entry from the queue.
	// If the queue is empty the last entry is reused (stable state).
	scanQueue [][]map[string]any

	// Query responses keyed by shard string ("0"–"3").
	// Each call pops the first entry from the shard's queue.
	queryQueues map[string][][]map[string]any

	// Record of calls made.
	scanCalls  int
	queryCalls map[string]int // shard → call count
}

func newFakeDynamo() *fakeDynamo {
	return &fakeDynamo{
		queryQueues: make(map[string][][]map[string]any),
		queryCalls:  make(map[string]int),
	}
}

// setScanItems queues a single Scan response. Multiple calls append to the
// queue; each Scan call dequeues one entry (the last is reused once exhausted).
func (f *fakeDynamo) setScanItems(items []map[string]any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.scanQueue = append(f.scanQueue, items)
}

// setQueryItems queues a Query response for a specific shard.
func (f *fakeDynamo) setQueryItems(shard string, items []map[string]any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.queryQueues[shard] = append(f.queryQueues[shard], items)
}

func (f *fakeDynamo) nextScanItems() []map[string]any {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.scanCalls++
	if len(f.scanQueue) == 0 {
		return nil
	}
	items := f.scanQueue[0]
	if len(f.scanQueue) > 1 {
		f.scanQueue = f.scanQueue[1:]
	}
	return items
}

func (f *fakeDynamo) nextQueryItems(shard string) []map[string]any {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.queryCalls[shard]++
	q := f.queryQueues[shard]
	if len(q) == 0 {
		return nil
	}
	items := q[0]
	if len(q) > 1 {
		f.queryQueues[shard] = q[1:]
	}
	return items
}

// RoundTrip intercepts DynamoDB HTTP calls and routes them to the appropriate
// fake handler based on the X-Amz-Target header.
func (f *fakeDynamo) RoundTrip(req *http.Request) (*http.Response, error) {
	target := req.Header.Get("X-Amz-Target")

	switch {
	case strings.HasSuffix(target, "Scan"):
		return f.handleScan(req)
	case strings.HasSuffix(target, "Query"):
		return f.handleQuery(req)
	default:
		return nil, fmt.Errorf("fakeDynamo: unhandled target %q", target)
	}
}

func (f *fakeDynamo) handleScan(req *http.Request) (*http.Response, error) {
	items := f.nextScanItems()
	return f.jsonResponse(200, map[string]any{
		"Count":            len(items),
		"Items":            items,
		"LastEvaluatedKey": nil,
	})
}

func (f *fakeDynamo) handleQuery(req *http.Request) (*http.Response, error) {
	// Parse the request body to extract the shard value from
	// ExpressionAttributeValues[":shard"].S
	body, _ := io.ReadAll(req.Body)
	var payload map[string]any
	_ = json.Unmarshal(body, &payload)

	shard := ""
	if eav, ok := payload["ExpressionAttributeValues"].(map[string]any); ok {
		if sv, ok := eav[":shard"].(map[string]any); ok {
			if s, ok := sv["S"].(string); ok {
				shard = s
			}
		}
	}

	items := f.nextQueryItems(shard)
	return f.jsonResponse(200, map[string]any{
		"Count":            len(items),
		"Items":            items,
		"LastEvaluatedKey": nil,
	})
}

func (f *fakeDynamo) jsonResponse(status int, body any) (*http.Response, error) {
	b, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	return &http.Response{
		StatusCode: status,
		Header:     http.Header{"Content-Type": []string{"application/x-amz-json-1.0"}},
		Body:       io.NopCloser(bytes.NewReader(b)),
	}, nil
}

// awsConfig returns an aws.Config that routes all DynamoDB calls through the
// fake RoundTripper. No real AWS credentials or endpoint are needed.
func (f *fakeDynamo) awsConfig() aws.Config {
	return aws.Config{
		Region: "us-east-1",
		Credentials: aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
			return aws.Credentials{
				AccessKeyID:     "fake",
				SecretAccessKey: "fake",
				SessionToken:    "fake",
			}, nil
		}),
		HTTPClient: &http.Client{Transport: f},
	}
}

// newTestClient creates a *dynamodb.Client backed by the fakeDynamo.
func (f *fakeDynamo) newClient() *dynamodb.Client {
	return dynamodb.NewFromConfig(f.awsConfig())
}

// stubItems converts a map[documentID]updateTime into the DynamoDB item format
// expected by dynamoItem.
func stubItems(stubs map[string]time.Time) []map[string]any {
	items := make([]map[string]any, 0, len(stubs))
	for docID, ut := range stubs {
		items = append(items, dynamoItem(docID, ut.UTC().Format(time.RFC3339)))
	}
	return items
}
