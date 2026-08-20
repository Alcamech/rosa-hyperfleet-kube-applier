package database

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	kubeapplier "github.com/rrp-bot/rosa-hyperfleet-kube-applier/api/kubeapplier"
)

func requireLocalStack(t *testing.T) {
	t.Helper()
	if os.Getenv("LOCALSTACK_ENDPOINT") == "" {
		t.Skip("LOCALSTACK_ENDPOINT not set; skipping integration test")
	}
}

// newTestClients creates DynamoDB client + DB client pair with unique table
// prefixes. The tables are created and registered for cleanup via t.Cleanup.
func newTestClients(t *testing.T) (*dynamodb.Client, KubeApplierDBClient, string) {
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

	dbClient := dynamodb.NewFromConfig(cfg)
	prefix := fmt.Sprintf("test-%d", time.Now().UnixNano())

	for _, suffix := range []string{TableSuffixApplyDesires, TableSuffixReadDesires} {
		tableName := prefix + suffix
		createTable(t, dbClient, tableName)
	}

	kubeClient := NewDynamoDBKubeApplierDBClient(dbClient, dbClient, prefix, prefix)
	return dbClient, kubeClient, prefix
}

func createTable(t *testing.T, dbClient *dynamodb.Client, tableName string) {
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

func TestIntegration_ApplyDesireCRUDRoundTrip(t *testing.T) {
	requireLocalStack(t)
	ctx := context.Background()
	_, dbClient, _ := newTestClients(t)
	crud := dbClient.ApplyDesireStatus()

	d := &kubeapplier.ApplyDesire{
		DynamoDBMetadata: kubeapplier.DynamoDBMetadata{DocumentID: "cluster1--cm1"},
		Spec: kubeapplier.ApplyDesireSpec{
			ManagementCluster: "mc-test",
			ClusterID:         "cluster1",
			TargetItem: kubeapplier.ResourceReference{
				Version:  "v1",
				Resource: "configmaps",
				Name:     "test-cm",
			},
			ServerSideApply: &kubeapplier.ServerSideApplyConfig{
				KubeContent: &runtime.RawExtension{
					Raw: []byte(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"test-cm"},"data":{"key":"value"}}`),
				},
			},
		},
		Status: kubeapplier.ApplyDesireStatus{
			Conditions: []metav1.Condition{
				{
					Type:               kubeapplier.ConditionTypeSuccessful,
					Status:             metav1.ConditionTrue,
					Reason:             kubeapplier.ConditionReasonNoErrors,
					Message:            "applied successfully",
					LastTransitionTime: metav1.NewTime(time.Date(2026, 1, 15, 10, 30, 0, 0, time.UTC)),
				},
			},
		},
	}

	// Create
	created, err := crud.Create(ctx, d)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if created.DocumentID != "cluster1--cm1" {
		t.Errorf("DocumentID = %q, want %q", created.DocumentID, "cluster1--cm1")
	}
	if created.Version != 1 {
		t.Errorf("Version = %d, want 1 after Create", created.Version)
	}
	if created.UpdateTime.IsZero() {
		t.Error("UpdateTime should be set after Create")
	}
	if created.CreateTime.IsZero() {
		t.Error("CreateTime should be set after Create")
	}

	// Get
	got, err := crud.Get(ctx, "cluster1--cm1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Spec.ClusterID != "cluster1" {
		t.Errorf("ClusterID = %q, want %q", got.Spec.ClusterID, "cluster1")
	}
	if got.Spec.TargetItem.Name != "test-cm" {
		t.Errorf("TargetItem.Name = %q, want %q", got.Spec.TargetItem.Name, "test-cm")
	}

	// Replace
	got.Spec.ClusterID = "cluster2"
	replaced, err := crud.Replace(ctx, got)
	if err != nil {
		t.Fatalf("Replace: %v", err)
	}
	if replaced.Spec.ClusterID != "cluster2" {
		t.Errorf("after Replace, ClusterID = %q, want %q", replaced.Spec.ClusterID, "cluster2")
	}
	if replaced.Version != got.Version+1 {
		t.Errorf("Version should increment: got %d, want %d", replaced.Version, got.Version+1)
	}

	// Verify replacement persisted
	got2, err := crud.Get(ctx, "cluster1--cm1")
	if err != nil {
		t.Fatalf("Get after Replace: %v", err)
	}
	if got2.Spec.ClusterID != "cluster2" {
		t.Errorf("persisted ClusterID = %q, want %q", got2.Spec.ClusterID, "cluster2")
	}

	// Delete
	if err := crud.Delete(ctx, "cluster1--cm1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// Get after Delete
	_, err = crud.Get(ctx, "cluster1--cm1")
	if !IsNotFoundError(err) {
		t.Errorf("expected NotFoundError after Delete, got %v", err)
	}
}

func TestIntegration_DeleteDesireCRUDRoundTrip(t *testing.T) {
	requireLocalStack(t)
	ctx := context.Background()
	_, dbClient, _ := newTestClients(t)
	crud := dbClient.ApplyDesireStatus()

	d := &kubeapplier.ApplyDesire{
		DynamoDBMetadata: kubeapplier.DynamoDBMetadata{DocumentID: "cluster1--del1"},
		Spec: kubeapplier.ApplyDesireSpec{
			ManagementCluster: "mc-test",
			ClusterID:         "cluster1",
			Type:              kubeapplier.ApplyDesireTypeDelete,
			TargetItem: kubeapplier.ResourceReference{
				Version:  "v1",
				Resource: "configmaps",
				Name:     "test-cm",
			},
		},
	}

	created, err := crud.Create(ctx, d)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	got, err := crud.Get(ctx, "cluster1--del1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Version != created.Version {
		t.Errorf("Version mismatch: got %d, want %d", got.Version, created.Version)
	}
	if got.Spec.Type != kubeapplier.ApplyDesireTypeDelete {
		t.Errorf("Type = %q, want %q", got.Spec.Type, kubeapplier.ApplyDesireTypeDelete)
	}
	if err := crud.Delete(ctx, "cluster1--del1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	_, err = crud.Get(ctx, "cluster1--del1")
	if !IsNotFoundError(err) {
		t.Errorf("expected NotFoundError, got %v", err)
	}
}

func TestIntegration_ReadDesireCRUDRoundTrip(t *testing.T) {
	requireLocalStack(t)
	ctx := context.Background()
	_, dbClient, _ := newTestClients(t)
	crud := dbClient.ReadDesireStatus()

	d := &kubeapplier.ReadDesire{
		DynamoDBMetadata: kubeapplier.DynamoDBMetadata{DocumentID: "cluster1--read1"},
		Spec: kubeapplier.ReadDesireSpec{
			ManagementCluster: "mc-test",
			ClusterID:         "cluster1",
			TargetItem: kubeapplier.ResourceReference{
				Version:  "v1",
				Resource: "configmaps",
				Name:     "test-cm",
			},
		},
		Status: kubeapplier.ReadDesireStatus{
			KubeContent: &runtime.RawExtension{
				Raw: []byte(`{"data":{"key":"value"}}`),
			},
		},
	}

	created, err := crud.Create(ctx, d)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	got, err := crud.Get(ctx, "cluster1--read1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Version != created.Version {
		t.Errorf("Version mismatch")
	}
	if got.Status.KubeContent == nil {
		t.Error("ReadDesire Status.KubeContent should round-trip through DynamoDB")
	}
}

func TestIntegration_OptimisticConcurrencyConflict(t *testing.T) {
	requireLocalStack(t)
	ctx := context.Background()
	_, dbClient, _ := newTestClients(t)
	crud := dbClient.ApplyDesireStatus()

	d := &kubeapplier.ApplyDesire{
		DynamoDBMetadata: kubeapplier.DynamoDBMetadata{DocumentID: "cluster1--conflict"},
		Spec: kubeapplier.ApplyDesireSpec{
			ManagementCluster: "mc-test",
			ClusterID:         "cluster1",
			TargetItem: kubeapplier.ResourceReference{
				Version:  "v1",
				Resource: "configmaps",
				Name:     "conflict-cm",
			},
		},
	}

	if _, err := crud.Create(ctx, d); err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Read the document twice (simulating two concurrent readers).
	read1, err := crud.Get(ctx, "cluster1--conflict")
	if err != nil {
		t.Fatalf("Get read1: %v", err)
	}
	read2, err := crud.Get(ctx, "cluster1--conflict")
	if err != nil {
		t.Fatalf("Get read2: %v", err)
	}

	// First writer succeeds.
	read1.Spec.ClusterID = "writer1"
	_, err = crud.Replace(ctx, read1)
	if err != nil {
		t.Fatalf("Replace read1: %v", err)
	}

	// Second writer fails — Version is now stale.
	read2.Spec.ClusterID = "writer2"
	_, err = crud.Replace(ctx, read2)
	if !IsPreconditionFailedError(err) {
		t.Errorf("expected PreconditionFailedError, got %v", err)
	}
}

func TestIntegration_RawExtensionRoundTrip(t *testing.T) {
	requireLocalStack(t)
	ctx := context.Background()
	_, dbClient, _ := newTestClients(t)
	crud := dbClient.ApplyDesireStatus()

	rawJSON := []byte(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"test"},"data":{"key":"value","nested":{"a":1}}}`)

	d := &kubeapplier.ApplyDesire{
		DynamoDBMetadata: kubeapplier.DynamoDBMetadata{DocumentID: "cluster1--rawext"},
		Spec: kubeapplier.ApplyDesireSpec{
			ManagementCluster: "mc-test",
			ClusterID:         "cluster1",
			TargetItem: kubeapplier.ResourceReference{
				Version:  "v1",
				Resource: "configmaps",
				Name:     "test",
			},
			ServerSideApply: &kubeapplier.ServerSideApplyConfig{
				KubeContent: &runtime.RawExtension{Raw: rawJSON},
			},
		},
	}

	if _, err := crud.Create(ctx, d); err != nil {
		t.Fatalf("Create: %v", err)
	}

	got, err := crud.Get(ctx, "cluster1--rawext")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.GetSpecKubeContent() == nil {
		t.Fatal("KubeContent is nil after round-trip")
	}
	// DynamoDB stores JSON as a string; compare JSON equivalence (not byte equality).
	var original, roundTripped map[string]any
	if err := json.Unmarshal(rawJSON, &original); err != nil {
		t.Fatalf("unmarshal original: %v", err)
	}
	if err := json.Unmarshal(got.GetSpecKubeContent().Raw, &roundTripped); err != nil {
		t.Fatalf("unmarshal roundTripped: %v", err)
	}
	origBytes, _ := json.Marshal(original)
	rtBytes, _ := json.Marshal(roundTripped)
	if !bytes.Equal(origBytes, rtBytes) {
		t.Errorf("KubeContent JSON mismatch:\n  got:  %s\n  want: %s", rtBytes, origBytes)
	}
}

func TestIntegration_MetaV1TimeRoundTrip(t *testing.T) {
	requireLocalStack(t)
	ctx := context.Background()
	_, dbClient, _ := newTestClients(t)
	crud := dbClient.ApplyDesireStatus()

	ts := metav1.NewTime(time.Date(2026, 6, 15, 14, 30, 45, 0, time.UTC))

	d := &kubeapplier.ApplyDesire{
		DynamoDBMetadata: kubeapplier.DynamoDBMetadata{DocumentID: "cluster1--time"},
		Spec: kubeapplier.ApplyDesireSpec{
			ManagementCluster: "mc-test",
			ClusterID:         "cluster1",
			TargetItem: kubeapplier.ResourceReference{
				Version:  "v1",
				Resource: "configmaps",
				Name:     "time-cm",
			},
		},
		Status: kubeapplier.ApplyDesireStatus{
			Conditions: []metav1.Condition{
				{
					Type:               kubeapplier.ConditionTypeSuccessful,
					Status:             metav1.ConditionTrue,
					Reason:             kubeapplier.ConditionReasonNoErrors,
					LastTransitionTime: ts,
				},
			},
		},
	}

	if _, err := crud.Create(ctx, d); err != nil {
		t.Fatalf("Create: %v", err)
	}

	got, err := crud.Get(ctx, "cluster1--time")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if len(got.Status.Conditions) != 1 {
		t.Fatalf("expected 1 condition, got %d", len(got.Status.Conditions))
	}
	gotTime := got.Status.Conditions[0].LastTransitionTime
	if !gotTime.Time.Equal(ts.Time) {
		t.Errorf("LastTransitionTime mismatch: got %v, want %v", gotTime.Time, ts.Time)
	}
}

func TestIntegration_ListReturnsAll(t *testing.T) {
	requireLocalStack(t)
	ctx := context.Background()
	_, dbClient, _ := newTestClients(t)
	crud := dbClient.ApplyDesireStatus()

	for i := 0; i < 3; i++ {
		d := &kubeapplier.ApplyDesire{
			DynamoDBMetadata: kubeapplier.DynamoDBMetadata{DocumentID: fmt.Sprintf("cluster1--item%d", i)},
			Spec: kubeapplier.ApplyDesireSpec{
				ManagementCluster: "mc-test",
				ClusterID:         "cluster1",
				TargetItem: kubeapplier.ResourceReference{
					Version:  "v1",
					Resource: "configmaps",
					Name:     fmt.Sprintf("cm-%d", i),
				},
			},
		}
		if _, err := crud.Create(ctx, d); err != nil {
			t.Fatalf("Create %d: %v", i, err)
		}
	}

	items, err := crud.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(items) != 3 {
		t.Errorf("expected 3 items, got %d", len(items))
	}
}

func TestIntegration_PerTableIsolation(t *testing.T) {
	requireLocalStack(t)
	ctx := context.Background()
	_, dbClient, _ := newTestClients(t)

	ad := &kubeapplier.ApplyDesire{
		DynamoDBMetadata: kubeapplier.DynamoDBMetadata{DocumentID: "cluster1--shared-id"},
		Spec: kubeapplier.ApplyDesireSpec{
			ManagementCluster: "mc-test",
			ClusterID:         "cluster1",
			TargetItem: kubeapplier.ResourceReference{
				Version:  "v1",
				Resource: "configmaps",
				Name:     "cm",
			},
		},
	}
	if _, err := dbClient.ApplyDesireStatus().Create(ctx, ad); err != nil {
		t.Fatalf("Create ApplyDesire: %v", err)
	}

	// Same document ID in ReadDesires should not conflict — different table.
	rd := &kubeapplier.ReadDesire{
		DynamoDBMetadata: kubeapplier.DynamoDBMetadata{DocumentID: "cluster1--shared-id"},
		Spec: kubeapplier.ReadDesireSpec{
			ManagementCluster: "mc-test",
			ClusterID:         "cluster1",
			TargetItem: kubeapplier.ResourceReference{
				Version:  "v1",
				Resource: "configmaps",
				Name:     "cm",
			},
		},
	}
	if _, err := dbClient.ReadDesireStatus().Create(ctx, rd); err != nil {
		t.Fatalf("Create ReadDesire should not conflict: %v", err)
	}

	applyList, err := dbClient.ApplyDesireStatus().List(ctx)
	if err != nil {
		t.Fatalf("ApplyDesires.List: %v", err)
	}
	readList, err := dbClient.ReadDesireStatus().List(ctx)
	if err != nil {
		t.Fatalf("ReadDesires.List: %v", err)
	}
	if len(applyList) != 1 {
		t.Errorf("expected 1 ApplyDesire, got %d", len(applyList))
	}
	if len(readList) != 1 {
		t.Errorf("expected 1 ReadDesire, got %d", len(readList))
	}
}

func TestIntegration_GetNotFound(t *testing.T) {
	requireLocalStack(t)
	ctx := context.Background()
	_, dbClient, _ := newTestClients(t)

	_, err := dbClient.ApplyDesireStatus().Get(ctx, "nonexistent")
	if !IsNotFoundError(err) {
		t.Errorf("expected NotFoundError, got %v", err)
	}
}

func TestIntegration_CreateDuplicate(t *testing.T) {
	requireLocalStack(t)
	ctx := context.Background()
	_, dbClient, _ := newTestClients(t)
	crud := dbClient.ApplyDesireStatus()

	d := &kubeapplier.ApplyDesire{
		DynamoDBMetadata: kubeapplier.DynamoDBMetadata{DocumentID: "cluster1--dup"},
		Spec: kubeapplier.ApplyDesireSpec{
			ManagementCluster: "mc-test",
			ClusterID:         "cluster1",
			TargetItem: kubeapplier.ResourceReference{
				Version:  "v1",
				Resource: "configmaps",
				Name:     "dup-cm",
			},
		},
	}

	if _, err := crud.Create(ctx, d); err != nil {
		t.Fatalf("first Create: %v", err)
	}
	_, err := crud.Create(ctx, d)
	if !IsAlreadyExistsError(err) {
		t.Errorf("expected AlreadyExistsError on duplicate Create, got %v", err)
	}
}

func TestIntegration_ClientClose(t *testing.T) {
	requireLocalStack(t)
	_, dbClient, _ := newTestClients(t)
	if err := dbClient.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestIntegration_SpecReaderSeparateFromStatus(t *testing.T) {
	requireLocalStack(t)
	ctx := context.Background()
	rawClient, _, prefix := newTestClients(t)

	// Write an ApplyDesire to the specs prefix tables via a separate "specs" client.
	specsClient := NewDynamoDBKubeApplierDBClient(rawClient, rawClient, prefix, prefix)

	d := &kubeapplier.ApplyDesire{
		DynamoDBMetadata: kubeapplier.DynamoDBMetadata{DocumentID: "c1--spec"},
		Spec: kubeapplier.ApplyDesireSpec{
			ManagementCluster: "mc-test",
			ClusterID:         "c1",
			TargetItem: kubeapplier.ResourceReference{
				Version:  "v1",
				Resource: "configmaps",
				Name:     "spec-cm",
			},
		},
	}
	if _, err := specsClient.ApplyDesireStatus().Create(ctx, d); err != nil {
		t.Fatalf("Create in specs: %v", err)
	}

	// Read via SpecReader.
	got, err := specsClient.ApplyDesireSpecs().Get(ctx, "c1--spec")
	if err != nil {
		t.Fatalf("SpecReader.Get: %v", err)
	}
	if got.Spec.ClusterID != "c1" {
		t.Errorf("ClusterID = %q, want %q", got.Spec.ClusterID, "c1")
	}

	// List via SpecReader.
	list, err := specsClient.ApplyDesireSpecs().List(ctx)
	if err != nil {
		t.Fatalf("SpecReader.List: %v", err)
	}
	if len(list) != 1 {
		t.Errorf("expected 1 item, got %d", len(list))
	}
}
