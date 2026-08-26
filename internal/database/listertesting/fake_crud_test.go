package listertesting

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	kubeapplier "github.com/openshift-online/rosa-hyperfleet-kube-applier/api/kubeapplier"
	"github.com/openshift-online/rosa-hyperfleet-kube-applier/internal/database"
)

func newTestApplyDesire(docID string) *kubeapplier.ApplyDesire {
	return &kubeapplier.ApplyDesire{
		DynamoDBMetadata: kubeapplier.DynamoDBMetadata{DocumentID: docID},
		Spec: kubeapplier.ApplyDesireSpec{
			ManagementCluster: "mc-test",
			ClusterID:         "cluster1",
			TargetItem: kubeapplier.ResourceReference{
				Version:  "v1",
				Resource: "configmaps",
				Name:     "test-cm",
			},
			ServerSideApply: &kubeapplier.ServerSideApplyConfig{
				KubeContent: &runtime.RawExtension{Raw: []byte(`{"apiVersion":"v1","kind":"ConfigMap"}`)},
			},
		},
	}
}

func TestFakeCRUD_GetExisting(t *testing.T) {
	ctx := context.Background()
	crud := NewFakeCRUD[kubeapplier.ApplyDesire, *kubeapplier.ApplyDesire]()
	d := newTestApplyDesire("cluster1--cm1")
	created, err := crud.Create(ctx, d)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	got, err := crud.Get(ctx, "cluster1--cm1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.DocumentID != "cluster1--cm1" {
		t.Errorf("DocumentID = %q, want %q", got.DocumentID, "cluster1--cm1")
	}
	if got.Version != created.Version {
		t.Errorf("Version = %d, want %d", got.Version, created.Version)
	}
	if got.CreateTime.IsZero() {
		t.Error("CreateTime should be set")
	}
}

func TestFakeCRUD_GetMissing(t *testing.T) {
	ctx := context.Background()
	crud := NewFakeCRUD[kubeapplier.ApplyDesire, *kubeapplier.ApplyDesire]()
	_, err := crud.Get(ctx, "nonexistent")
	if !database.IsNotFoundError(err) {
		t.Errorf("expected NotFoundError, got %v", err)
	}
}

func TestFakeCRUD_ListEmpty(t *testing.T) {
	ctx := context.Background()
	crud := NewFakeCRUD[kubeapplier.ApplyDesire, *kubeapplier.ApplyDesire]()
	items, err := crud.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(items) != 0 {
		t.Errorf("expected empty list, got %d items", len(items))
	}
}

func TestFakeCRUD_ListPopulated(t *testing.T) {
	ctx := context.Background()
	crud := NewFakeCRUD[kubeapplier.ApplyDesire, *kubeapplier.ApplyDesire]()
	for _, id := range []string{"cluster1--a", "cluster1--b", "cluster1--c"} {
		d := newTestApplyDesire(id)
		if _, err := crud.Create(ctx, d); err != nil {
			t.Fatalf("Create %s: %v", id, err)
		}
	}
	items, err := crud.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(items) != 3 {
		t.Fatalf("expected 3 items, got %d", len(items))
	}
	// List returns sorted by DocumentID.
	if items[0].DocumentID != "cluster1--a" || items[1].DocumentID != "cluster1--b" || items[2].DocumentID != "cluster1--c" {
		t.Errorf("unexpected order: %v, %v, %v", items[0].DocumentID, items[1].DocumentID, items[2].DocumentID)
	}
}

func TestFakeCRUD_CreateNew(t *testing.T) {
	ctx := context.Background()
	crud := NewFakeCRUD[kubeapplier.ApplyDesire, *kubeapplier.ApplyDesire]()
	d := newTestApplyDesire("cluster1--cm1")
	created, err := crud.Create(ctx, d)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if created.DocumentID != "cluster1--cm1" {
		t.Errorf("DocumentID = %q, want %q", created.DocumentID, "cluster1--cm1")
	}
	if created.Version != 1 {
		t.Errorf("Version = %d, want 1", created.Version)
	}
	if created.UpdateTime.IsZero() {
		t.Error("UpdateTime should be set after Create")
	}
	if created.CreateTime.IsZero() {
		t.Error("CreateTime should be set after Create")
	}
	if !created.UpdateTime.Equal(created.CreateTime) {
		t.Error("UpdateTime and CreateTime should be equal on Create")
	}
}

func TestFakeCRUD_CreateDuplicate(t *testing.T) {
	ctx := context.Background()
	crud := NewFakeCRUD[kubeapplier.ApplyDesire, *kubeapplier.ApplyDesire]()
	d := newTestApplyDesire("cluster1--cm1")
	if _, err := crud.Create(ctx, d); err != nil {
		t.Fatalf("first Create: %v", err)
	}
	_, err := crud.Create(ctx, d)
	if !database.IsAlreadyExistsError(err) {
		t.Errorf("expected AlreadyExistsError on duplicate Create, got %v", err)
	}
}

func TestFakeCRUD_CreateEmptyDocID(t *testing.T) {
	ctx := context.Background()
	crud := NewFakeCRUD[kubeapplier.ApplyDesire, *kubeapplier.ApplyDesire]()
	d := newTestApplyDesire("")
	_, err := crud.Create(ctx, d)
	if err == nil {
		t.Fatal("expected error when DocumentID is empty")
	}
}

func TestFakeCRUD_ReplaceSuccess(t *testing.T) {
	ctx := context.Background()
	crud := NewFakeCRUD[kubeapplier.ApplyDesire, *kubeapplier.ApplyDesire]()
	d := newTestApplyDesire("cluster1--cm1")
	created, err := crud.Create(ctx, d)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	created.Spec.ClusterID = "cluster2"
	replaced, err := crud.Replace(ctx, created)
	if err != nil {
		t.Fatalf("Replace: %v", err)
	}
	if replaced.Spec.ClusterID != "cluster2" {
		t.Errorf("ClusterID = %q, want %q", replaced.Spec.ClusterID, "cluster2")
	}
	if replaced.Version != created.Version+1 {
		t.Errorf("Version = %d, want %d", replaced.Version, created.Version+1)
	}
	if replaced.UpdateTime.Equal(created.UpdateTime) {
		t.Error("UpdateTime should be bumped after Replace")
	}
	if !replaced.CreateTime.Equal(created.CreateTime) {
		t.Error("CreateTime should be preserved after Replace")
	}
}

func TestFakeCRUD_ReplaceStaleVersion(t *testing.T) {
	ctx := context.Background()
	crud := NewFakeCRUD[kubeapplier.ApplyDesire, *kubeapplier.ApplyDesire]()
	d := newTestApplyDesire("cluster1--cm1")
	created, err := crud.Create(ctx, d)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	// First replace succeeds and increments Version.
	if _, err := crud.Replace(ctx, created); err != nil {
		t.Fatalf("first Replace: %v", err)
	}
	// Second replace with the original (now stale) Version should fail.
	_, err = crud.Replace(ctx, created)
	if !database.IsPreconditionFailedError(err) {
		t.Errorf("expected PreconditionFailedError, got %v", err)
	}
}

func TestFakeCRUD_ReplaceMissing(t *testing.T) {
	ctx := context.Background()
	crud := NewFakeCRUD[kubeapplier.ApplyDesire, *kubeapplier.ApplyDesire]()
	d := newTestApplyDesire("cluster1--cm1")
	d.Version = 1
	_, err := crud.Replace(ctx, d)
	if !database.IsNotFoundError(err) {
		t.Errorf("expected NotFoundError, got %v", err)
	}
}

func TestFakeCRUD_DeleteExisting(t *testing.T) {
	ctx := context.Background()
	crud := NewFakeCRUD[kubeapplier.ApplyDesire, *kubeapplier.ApplyDesire]()
	d := newTestApplyDesire("cluster1--cm1")
	if _, err := crud.Create(ctx, d); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := crud.Delete(ctx, "cluster1--cm1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	_, err := crud.Get(ctx, "cluster1--cm1")
	if !database.IsNotFoundError(err) {
		t.Errorf("expected NotFoundError after Delete, got %v", err)
	}
}

func TestFakeCRUD_DeleteMissing(t *testing.T) {
	ctx := context.Background()
	crud := NewFakeCRUD[kubeapplier.ApplyDesire, *kubeapplier.ApplyDesire]()
	err := crud.Delete(ctx, "nonexistent")
	if !database.IsNotFoundError(err) {
		t.Errorf("expected NotFoundError, got %v", err)
	}
}

func TestFakeCRUD_ReturnedObjectsAreIsolated(t *testing.T) {
	ctx := context.Background()
	crud := NewFakeCRUD[kubeapplier.ApplyDesire, *kubeapplier.ApplyDesire]()
	d := newTestApplyDesire("cluster1--cm1")
	created, err := crud.Create(ctx, d)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	// Mutate the returned object.
	created.Spec.ClusterID = "mutated"
	// Fetch from store again — should not see the mutation.
	got, err := crud.Get(ctx, "cluster1--cm1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Spec.ClusterID == "mutated" {
		t.Error("mutation of returned object affected the store")
	}
}

func TestFakeCRUD_DeterministicUpdateTimes(t *testing.T) {
	ctx := context.Background()
	crud := NewFakeCRUD[kubeapplier.ApplyDesire, *kubeapplier.ApplyDesire]()
	d1 := newTestApplyDesire("cluster1--a")
	d2 := newTestApplyDesire("cluster1--b")
	c1, _ := crud.Create(ctx, d1)
	c2, _ := crud.Create(ctx, d2)
	if !c1.UpdateTime.Before(c2.UpdateTime) {
		t.Errorf("expected c1.UpdateTime < c2.UpdateTime: %v vs %v", c1.UpdateTime, c2.UpdateTime)
	}
}

func TestFakeCRUD_DeleteDesireType(t *testing.T) {
	ctx := context.Background()
	crud := NewFakeCRUD[kubeapplier.ApplyDesire, *kubeapplier.ApplyDesire]()
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
	if got.DocumentID != created.DocumentID {
		t.Errorf("DocumentID mismatch: %q vs %q", got.DocumentID, created.DocumentID)
	}
	if got.Spec.Type != kubeapplier.ApplyDesireTypeDelete {
		t.Errorf("Type = %q, want %q", got.Spec.Type, kubeapplier.ApplyDesireTypeDelete)
	}
}

func TestFakeCRUD_ReadDesireType(t *testing.T) {
	ctx := context.Background()
	crud := NewFakeCRUD[kubeapplier.ReadDesire, *kubeapplier.ReadDesire]()
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
			KubeContent: &runtime.RawExtension{Raw: []byte(`{"data":{"key":"value"}}`)},
			Conditions: []metav1.Condition{
				{
					Type:               "Successful",
					Status:             metav1.ConditionTrue,
					LastTransitionTime: metav1.Now(),
				},
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
	if got.Status.KubeContent == nil {
		t.Fatal("KubeContent should not be nil")
	}
	if string(got.Status.KubeContent.Raw) != `{"data":{"key":"value"}}` {
		t.Errorf("KubeContent.Raw = %s, want %s", got.Status.KubeContent.Raw, `{"data":{"key":"value"}}`)
	}
	if len(got.Status.Conditions) != 1 {
		t.Fatalf("expected 1 condition, got %d", len(got.Status.Conditions))
	}
	if !got.Status.Conditions[0].LastTransitionTime.Equal(&created.Status.Conditions[0].LastTransitionTime) {
		t.Error("condition LastTransitionTime should survive round-trip")
	}
}

func TestFakeClient_ImplementsInterface(t *testing.T) {
	client := NewFakeKubeApplierDBClient()
	ctx := context.Background()

	d := newTestApplyDesire("cluster1--cm1")
	if _, err := client.ApplyDesireStatus().Create(ctx, d); err != nil {
		t.Fatalf("ApplyDesireStatus().Create: %v", err)
	}

	rd := &kubeapplier.ReadDesire{
		DynamoDBMetadata: kubeapplier.DynamoDBMetadata{DocumentID: "cluster1--read1"},
		Spec:             kubeapplier.ReadDesireSpec{ClusterID: "cluster1"},
	}
	if _, err := client.ReadDesireStatus().Create(ctx, rd); err != nil {
		t.Fatalf("ReadDesireStatus().Create: %v", err)
	}

	if err := client.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestFakeClient_CollectionsAreIsolated(t *testing.T) {
	client := NewFakeKubeApplierDBClient()
	ctx := context.Background()

	d := newTestApplyDesire("cluster1--cm1")
	if _, err := client.ApplyDesireStatus().Create(ctx, d); err != nil {
		t.Fatalf("Create: %v", err)
	}
	// Same DocumentID in ReadDesires should not conflict.
	rd := &kubeapplier.ReadDesire{
		DynamoDBMetadata: kubeapplier.DynamoDBMetadata{DocumentID: "cluster1--cm1"},
		Spec:             kubeapplier.ReadDesireSpec{ClusterID: "cluster1"},
	}
	if _, err := client.ReadDesireStatus().Create(ctx, rd); err != nil {
		t.Fatalf("ReadDesireStatus().Create should not conflict with ApplyDesireStatus: %v", err)
	}
}
