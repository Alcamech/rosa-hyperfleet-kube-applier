package listertesting

import (
	kubeapplier "github.com/openshift-online/rosa-hyperfleet-kube-applier/api/kubeapplier"
	"github.com/openshift-online/rosa-hyperfleet-kube-applier/internal/database"
)

// FakeKubeApplierDBClient is an in-memory implementation of
// database.KubeApplierDBClient for unit testing. It models the two-table
// architecture: separate spec (read-only) and status (read-write) stores
// per desire type.
type FakeKubeApplierDBClient struct {
	applyDesireSpecs *FakeCRUD[kubeapplier.ApplyDesire, *kubeapplier.ApplyDesire]
	readDesireSpecs  *FakeCRUD[kubeapplier.ReadDesire, *kubeapplier.ReadDesire]

	applyDesireStatus *FakeCRUD[kubeapplier.ApplyDesire, *kubeapplier.ApplyDesire]
	readDesireStatus  *FakeCRUD[kubeapplier.ReadDesire, *kubeapplier.ReadDesire]
}

var _ database.KubeApplierDBClient = (*FakeKubeApplierDBClient)(nil)

// NewFakeKubeApplierDBClient returns a ready-to-use fake client with empty
// spec and status stores.
func NewFakeKubeApplierDBClient() *FakeKubeApplierDBClient {
	return &FakeKubeApplierDBClient{
		applyDesireSpecs:  NewFakeCRUD[kubeapplier.ApplyDesire, *kubeapplier.ApplyDesire](),
		readDesireSpecs:   NewFakeCRUD[kubeapplier.ReadDesire, *kubeapplier.ReadDesire](),
		applyDesireStatus: NewFakeCRUD[kubeapplier.ApplyDesire, *kubeapplier.ApplyDesire](),
		readDesireStatus:  NewFakeCRUD[kubeapplier.ReadDesire, *kubeapplier.ReadDesire](),
	}
}

func (c *FakeKubeApplierDBClient) ApplyDesireSpecs() database.SpecReader[kubeapplier.ApplyDesire] {
	return c.applyDesireSpecs
}

func (c *FakeKubeApplierDBClient) ReadDesireSpecs() database.SpecReader[kubeapplier.ReadDesire] {
	return c.readDesireSpecs
}

func (c *FakeKubeApplierDBClient) ApplyDesireStatus() database.ResourceCRUD[kubeapplier.ApplyDesire] {
	return c.applyDesireStatus
}

func (c *FakeKubeApplierDBClient) ReadDesireStatus() database.ResourceCRUD[kubeapplier.ReadDesire] {
	return c.readDesireStatus
}

func (c *FakeKubeApplierDBClient) Close() error { return nil }

// Spec CRUD accessors — for seeding test data.
func (c *FakeKubeApplierDBClient) ApplyDesireSpecsCRUD() *FakeCRUD[kubeapplier.ApplyDesire, *kubeapplier.ApplyDesire] {
	return c.applyDesireSpecs
}
func (c *FakeKubeApplierDBClient) ReadDesireSpecsCRUD() *FakeCRUD[kubeapplier.ReadDesire, *kubeapplier.ReadDesire] {
	return c.readDesireSpecs
}

// Status CRUD accessors — for seeding test data.
func (c *FakeKubeApplierDBClient) ApplyDesireStatusCRUD() *FakeCRUD[kubeapplier.ApplyDesire, *kubeapplier.ApplyDesire] {
	return c.applyDesireStatus
}
func (c *FakeKubeApplierDBClient) ReadDesireStatusCRUD() *FakeCRUD[kubeapplier.ReadDesire, *kubeapplier.ReadDesire] {
	return c.readDesireStatus
}
