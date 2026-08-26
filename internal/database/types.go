package database

import (
	"context"
	"time"

	kubeapplier "github.com/openshift-online/rosa-hyperfleet-kube-applier/api/kubeapplier"
)

// DynamoDBMetadataAccessor provides generic access to the DynamoDB-specific
// metadata fields (DocumentID, Version, UpdateTime, CreateTime) that every
// desire type carries via the embedded DynamoDBMetadata struct.
type DynamoDBMetadataAccessor interface {
	GetDocumentID() string
	GetVersion() int64
	GetUpdateTime() time.Time
	GetCreateTime() time.Time
	SetDocumentID(string)
	SetVersion(int64)
	SetUpdateTime(time.Time)
	SetCreateTime(time.Time)
}

// SpecReader provides read-only access to spec documents in the specs table.
// The agent uses this to read desire specifications written by the backend.
type SpecReader[T any] interface {
	Get(ctx context.Context, documentID string) (*T, error)
	List(ctx context.Context) ([]*T, error)
}

// ResourceCRUD is the generic CRUD interface for a single DynamoDB table.
// Used for status documents in the status table where the agent has full
// read-write access.
//
// Get returns NewNotFoundError() when the item doesn't exist.
// Create returns NewAlreadyExistsError() when the item already exists.
// Replace uses optimistic concurrency via Version counter; it returns
// NewPreconditionFailedError() when the item has been modified since last read.
type ResourceCRUD[T any] interface {
	Get(ctx context.Context, documentID string) (*T, error)
	List(ctx context.Context) ([]*T, error)
	Create(ctx context.Context, obj *T) (*T, error)
	Replace(ctx context.Context, obj *T) (*T, error)
	Delete(ctx context.Context, documentID string) error
}

// KubeApplierDBClient is the per-management-cluster handle that wraps two
// sets of DynamoDB tables: specs (read-only for the agent) and status
// (read-write for the agent). IAM policies enforce directional isolation.
//
// Delete desires now use the applydesires tables (Type=Delete on ApplyDesire).
type KubeApplierDBClient interface {
	ApplyDesireSpecs() SpecReader[kubeapplier.ApplyDesire]
	ReadDesireSpecs() SpecReader[kubeapplier.ReadDesire]

	ApplyDesireStatus() ResourceCRUD[kubeapplier.ApplyDesire]
	ReadDesireStatus() ResourceCRUD[kubeapplier.ReadDesire]

	Close() error
}
