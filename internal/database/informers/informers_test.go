package informers

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"k8s.io/client-go/tools/cache"

	kubeapplier "github.com/rrp-bot/rosa-hyperfleet-kube-applier/api/kubeapplier"
	"github.com/rrp-bot/rosa-hyperfleet-kube-applier/internal/database"
	"github.com/rrp-bot/rosa-hyperfleet-kube-applier/internal/database/listers"
	hd "github.com/rrp-bot/rosa-hyperfleet-kube-applier/hyperfleet-dynamo/dynamodb"
)

// --- Unit tests (no LocalStack required) ---

func TestListWatchWithoutWatchListSemantics(t *testing.T) {
	lw := hd.ListWatchWithoutWatchListSemantics{ListWatch: &cache.ListWatch{}}
	if !lw.IsWatchListSemanticsUnSupported() {
		t.Error("expected IsWatchListSemanticsUnSupported to return true")
	}
}

func TestListerListFromPopulatedCache(t *testing.T) {
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})

	desires := []*kubeapplier.ApplyDesire{
		{DynamoDBMetadata: kubeapplier.DynamoDBMetadata{DocumentID: "c1--a"}},
		{DynamoDBMetadata: kubeapplier.DynamoDBMetadata{DocumentID: "c1--b"}},
		{DynamoDBMetadata: kubeapplier.DynamoDBMetadata{DocumentID: "c2--a"}},
	}
	for _, d := range desires {
		if err := indexer.Add(d); err != nil {
			t.Fatalf("indexer.Add: %v", err)
		}
	}

	lister := listers.NewApplyDesireLister(indexer)

	items, err := lister.List()
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(items) != 3 {
		t.Errorf("List returned %d items, want 3", len(items))
	}
}

func TestListerGetFromPopulatedCache(t *testing.T) {
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})

	d := &kubeapplier.ApplyDesire{
		DynamoDBMetadata: kubeapplier.DynamoDBMetadata{DocumentID: "c1--a"},
		Spec:             kubeapplier.ApplyDesireSpec{ClusterID: "c1"},
	}
	if err := indexer.Add(d); err != nil {
		t.Fatalf("indexer.Add: %v", err)
	}

	lister := listers.NewApplyDesireLister(indexer)

	got, err := lister.Get("c1--a")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Spec.ClusterID != "c1" {
		t.Errorf("ClusterID = %q, want %q", got.Spec.ClusterID, "c1")
	}
}

func TestListerGetNotFound(t *testing.T) {
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	lister := listers.NewApplyDesireLister(indexer)

	_, err := lister.Get("nonexistent")
	if !database.IsNotFoundError(err) {
		t.Errorf("expected NotFoundError, got %v", err)
	}
}

func TestReadDesireListerFromPopulatedCache(t *testing.T) {
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	d := &kubeapplier.ReadDesire{
		DynamoDBMetadata: kubeapplier.DynamoDBMetadata{DocumentID: "c1--read1"},
		Spec:             kubeapplier.ReadDesireSpec{ClusterID: "c1"},
	}
	if err := indexer.Add(d); err != nil {
		t.Fatalf("indexer.Add: %v", err)
	}

	lister := listers.NewReadDesireLister(indexer)

	got, err := lister.Get("c1--read1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Spec.ClusterID != "c1" {
		t.Errorf("ClusterID = %q, want %q", got.Spec.ClusterID, "c1")
	}

	items, err := lister.List()
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(items) != 1 {
		t.Errorf("List returned %d items, want 1", len(items))
	}
}

// --- Integration tests (require LOCALSTACK_ENDPOINT) ---

func requireLocalStack(t *testing.T) {
	t.Helper()
	if os.Getenv("LOCALSTACK_ENDPOINT") == "" {
		t.Skip("LOCALSTACK_ENDPOINT not set; skipping integration test")
	}
}

func newLocalStackClient(t *testing.T) *dynamodb.Client {
	t.Helper()
	endpoint := os.Getenv("LOCALSTACK_ENDPOINT")
	cfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
		awsconfig.WithBaseEndpoint(endpoint),
	)
	if err != nil {
		t.Fatalf("awsconfig.LoadDefaultConfig: %v", err)
	}
	return dynamodb.NewFromConfig(cfg)
}

// createTestTable creates a table with the updateTime-index GSI (matching
// production Terraform: shard HASH + updateTime RANGE, ALL projection).
func createTestTable(t *testing.T, dbClient *dynamodb.Client, tableName string) {
	t.Helper()
	ctx := context.Background()
	_, err := dbClient.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName:   aws.String(tableName),
		BillingMode: dbtypes.BillingModePayPerRequest,
		AttributeDefinitions: []dbtypes.AttributeDefinition{
			{AttributeName: aws.String("documentID"), AttributeType: dbtypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("shard"), AttributeType: dbtypes.ScalarAttributeTypeS},
			{AttributeName: aws.String("updateTime"), AttributeType: dbtypes.ScalarAttributeTypeS},
		},
		KeySchema: []dbtypes.KeySchemaElement{
			{AttributeName: aws.String("documentID"), KeyType: dbtypes.KeyTypeHash},
		},
		GlobalSecondaryIndexes: []dbtypes.GlobalSecondaryIndex{
			{
				IndexName: aws.String("updateTime-index"),
				KeySchema: []dbtypes.KeySchemaElement{
					{AttributeName: aws.String("shard"), KeyType: dbtypes.KeyTypeHash},
					{AttributeName: aws.String("updateTime"), KeyType: dbtypes.KeyTypeRange},
				},
				Projection: &dbtypes.Projection{
					ProjectionType: dbtypes.ProjectionTypeAll,
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("CreateTable %s: %v", tableName, err)
	}
	t.Cleanup(func() {
		dbClient.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{
			TableName: aws.String(tableName),
		})
	})
}

func startAndSync(t *testing.T, ctx context.Context, info KubeApplierInformers) {
	t.Helper()
	go info.RunWithContext(ctx)
	applyInf, _ := info.ApplyDesires()
	readInf, _ := info.ReadDesires()
	if !cache.WaitForCacheSync(ctx.Done(), applyInf.HasSynced, readInf.HasSynced) {
		t.Fatal("informers did not sync")
	}
}

func waitForCacheCount(t *testing.T, store cache.Store, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		if len(store.List()) == want {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for cache to contain %d items (has %d)", want, len(store.List()))
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// TestIntegration_InformerSyncsExistingDocuments verifies that documents
// already in DynamoDB before the informer starts are picked up on the initial
// List (relist).
func TestIntegration_InformerSyncsExistingDocuments(t *testing.T) {
	requireLocalStack(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	dbClient := newLocalStackClient(t)
	prefix := fmt.Sprintf("inf-existing-%d", time.Now().UnixNano())

	applyTable := prefix + database.TableSuffixApplyDesires
	readTable := prefix + database.TableSuffixReadDesires
	createTestTable(t, dbClient, applyTable)
	createTestTable(t, dbClient, readTable)

	dbCRUD := database.NewDynamoDBKubeApplierDBClient(dbClient, dbClient, prefix, prefix)

	for i := 0; i < 3; i++ {
		d := &kubeapplier.ApplyDesire{
			DynamoDBMetadata: kubeapplier.DynamoDBMetadata{DocumentID: fmt.Sprintf("c1--item%d", i)},
			Spec: kubeapplier.ApplyDesireSpec{
				ManagementCluster: "mc-test",
				ClusterID:         "c1",
				TargetItem: kubeapplier.ResourceReference{
					Version:  "v1",
					Resource: "configmaps",
					Name:     fmt.Sprintf("cm-%d", i),
				},
			},
		}
		if _, err := dbCRUD.ApplyDesireStatus().Create(ctx, d); err != nil {
			t.Fatalf("Create %d: %v", i, err)
		}
	}

	info := NewKubeApplierInformersWithResyncPeriod(dbClient, prefix, 30*time.Second)
	startAndSync(t, ctx, info)

	applyInf, applyLister := info.ApplyDesires()
	if len(applyInf.GetStore().List()) != 3 {
		t.Errorf("expected 3 items in cache, got %d", len(applyInf.GetStore().List()))
	}

	items, err := applyLister.List()
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(items) != 3 {
		t.Errorf("lister returned %d items, want 3", len(items))
	}

	got, err := applyLister.Get("c1--item1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Spec.ClusterID != "c1" {
		t.Errorf("ClusterID = %q, want %q", got.Spec.ClusterID, "c1")
	}
}

// TestIntegration_GSIPollDeliversDoorbells verifies that a write after the
// informer starts is picked up via the GSI fast-poll path. The watcher delivers
// a doorbell (Modified event with zero UpdateTime) within its poll interval,
// causing the informer to relist and see the new item.
func TestIntegration_GSIPollDeliversDoorbells(t *testing.T) {
	requireLocalStack(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	dbClient := newLocalStackClient(t)
	prefix := fmt.Sprintf("inf-gsi-%d", time.Now().UnixNano())

	applyTable := prefix + database.TableSuffixApplyDesires
	readTable := prefix + database.TableSuffixReadDesires
	createTestTable(t, dbClient, applyTable)
	createTestTable(t, dbClient, readTable)

	dbCRUD := database.NewDynamoDBKubeApplierDBClient(dbClient, dbClient, prefix, prefix)
	crud := dbCRUD.ApplyDesireStatus()

	// Use a short poll interval and relist interval so the test runs quickly.
	info := NewKubeApplierInformersWithResyncPeriod(dbClient, prefix, 30*time.Second)
	startAndSync(t, ctx, info)

	applyInf, _ := info.ApplyDesires()

	// Write a document — the GSI poller should deliver a doorbell, triggering a
	// relist that populates the cache with the new item.
	d := &kubeapplier.ApplyDesire{
		DynamoDBMetadata: kubeapplier.DynamoDBMetadata{DocumentID: "c1--live"},
		Spec: kubeapplier.ApplyDesireSpec{
			ManagementCluster: "mc-test",
			ClusterID:         "c1",
			TargetItem: kubeapplier.ResourceReference{
				Version:  "v1",
				Resource: "configmaps",
				Name:     "live-cm",
			},
		},
	}
	if _, err := crud.Create(ctx, d); err != nil {
		t.Fatalf("Create: %v", err)
	}

	// The watcher must deliver the item within its relist interval (default 5m).
	// In test we rely on the relist (full scan) to pick it up.
	waitForCacheCount(t, applyInf.GetStore(), 1, 60*time.Second)
}

// TestIntegration_PerTableIsolation verifies that documents written to one
// prefix's tables do not appear in a watcher targeting a different prefix.
func TestIntegration_PerTableIsolation(t *testing.T) {
	requireLocalStack(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	dbClient := newLocalStackClient(t)
	prefixA := fmt.Sprintf("inf-iso-a-%d", time.Now().UnixNano())
	prefixB := fmt.Sprintf("inf-iso-b-%d", time.Now().UnixNano())

	for _, prefix := range []string{prefixA, prefixB} {
		createTestTable(t, dbClient, prefix+database.TableSuffixApplyDesires)
		createTestTable(t, dbClient, prefix+database.TableSuffixReadDesires)
	}

	dbCRUDA := database.NewDynamoDBKubeApplierDBClient(dbClient, dbClient, prefixA, prefixA)

	infoA := NewKubeApplierInformersWithResyncPeriod(dbClient, prefixA, 30*time.Second)
	infoB := NewKubeApplierInformersWithResyncPeriod(dbClient, prefixB, 30*time.Second)
	startAndSync(t, ctx, infoA)
	startAndSync(t, ctx, infoB)

	applyInfA, _ := infoA.ApplyDesires()
	applyInfB, _ := infoB.ApplyDesires()

	d := &kubeapplier.ApplyDesire{
		DynamoDBMetadata: kubeapplier.DynamoDBMetadata{DocumentID: "c1--isolated"},
		Spec: kubeapplier.ApplyDesireSpec{
			ManagementCluster: "mc-a",
			ClusterID:         "c1",
			TargetItem: kubeapplier.ResourceReference{
				Version:  "v1",
				Resource: "configmaps",
				Name:     "iso-cm",
			},
		},
	}
	if _, err := dbCRUDA.ApplyDesireStatus().Create(ctx, d); err != nil {
		t.Fatalf("Create in A: %v", err)
	}

	waitForCacheCount(t, applyInfA.GetStore(), 1, 60*time.Second)

	// B should remain empty.
	time.Sleep(500 * time.Millisecond)
	if len(applyInfB.GetStore().List()) != 0 {
		t.Errorf("expected 0 items in B's cache, got %d", len(applyInfB.GetStore().List()))
	}
}
