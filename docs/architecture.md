# Architecture

`kube-applier-aws` is the AWS/DynamoDB port of `kube-applier-gcp`. It runs one
controller replica per Management Cluster (MC) and bridges the desire loop
written by [hyperfleet-operator](https://github.com/typeid/hyperfleet-operator)
into live Kubernetes objects on that cluster.

## System overview

```mermaid
graph LR
    HF["hyperfleet-operator\n(RC account)"]
    DBS["DynamoDB specs tables\n(RC account)"]
    KA["kube-applier-aws\n(MC account)"]
    API["MC kube-apiserver"]
    DBT["DynamoDB status tables\n(RC account)"]

    HF -->|writes desires| DBS
    DBS -->|GSI two-speed poll\n(15s fast / 5m relist)| KA
    KA -->|SSA / Delete / Watch| API
    KA -->|writes status| DBT
    DBT -->|reads status| HF
```

The controller holds no state beyond leader-election leases. All desire input
and status output flows through DynamoDB.

## DynamoDB table layout

Four tables are provisioned per MC in the RC account. Table names follow the
prefix convention passed to the controller via `--specs-table` and
`--status-table` flags (ARN form in production):

| Table suffix | Direction | GSI | PITR |
|---|---|---|---|
| `{mc}-specs-applydesires` | hyperfleet-operator → controller | `updateTime-index` | non-ephemeral |
| `{mc}-specs-readdesires` | hyperfleet-operator → controller | `updateTime-index` | non-ephemeral |
| `{mc}-status-applydesires` | controller → hyperfleet-operator | — | non-ephemeral |
| `{mc}-status-readdesires` | controller → hyperfleet-operator | — | non-ephemeral |

The partition key in every table is `documentID` (string). The specs tables
carry a `updateTime-index` GSI (hash key: `shard`, range key: `updateTime`)
used by the two-speed polling watcher. DynamoDB Streams are not used.

Deletion is modelled as `ApplyDesire` with `spec.type=Delete` — there are no
separate `deletedesires` tables.

## Document ID scheme

Every desire document carries a deterministic UUID v5 as its `documentID`:

```
documentID = uuid.NewSHA1(NamespaceUUID, "{taskKey}/{group}/{version}/{resource}/{namespace}/{name}")

NamespaceUUID = a3f1b2c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c
```

The same inputs always yield the same UUID. Crash-and-retry by the writer
computes the same ID and hits the `ErrAlreadyExists` guard on the DynamoDB
`ConditionExpression` rather than creating a duplicate. Different `taskKey`
values (e.g. different hyperfleet-operator field managers) produce different
UUIDs for the same Kubernetes resource.

The namespace UUID is shared between this repository and hyperfleet-operator.
It must never be changed after initial deployment — doing so would invalidate
all existing document IDs.

## Informers and change detection

Each specs table is watched via the `hyperfleet-dynamo` two-speed engine:

1. **Initial list (Scan)** — on startup the informer issues a full `Scan` to
   populate its in-memory store.
2. **Fast poll (default 15 s)** — fans out parallel `Query` calls across 4 GSI
   shard buckets for items updated since the expanding lookback window. Changed
   document IDs are fetched in full via `BatchGetItem` and delivered as
   `watch.Modified` events. No consumer limit; no stream shard management.
3. **Full relist (default 5 m)** — consistent full-table `Scan` diffs against
   the stub cache to detect additions, modifications, and deletions. Resets the
   expanding lookback window.

The `WatchAdapter` translates `OnChange` callbacks into typed `watch.Event`
objects (`*ApplyDesire` / `*ReadDesire`) consumed by the `SharedIndexInformer`.
See `hyperfleet-dynamo/dynamodb/doc.go` for the full engine description.

## Optimistic concurrency

The status tables use a `version` counter and a DynamoDB
`ConditionExpression` to prevent lost updates:

- `Create` — succeeds only when the item does not exist
  (`attribute_not_exists(documentID)`).
- `Replace` — increments `version` and conditions on the previous value
  (`version = :expected`).

Concurrent writes by stale replicas (before leader election converges) are
rejected with `ErrPreconditionFailed`. The status writer retries with a fresh
`Get` → mutate → `Replace` cycle.

## Leader election

One controller replica holds the leader lease at any time. Leases are stored as
Kubernetes `Lease` objects in the controller's own namespace. The lease name is
configurable via `--leader-election-id` (default: `kube-applier`). Only the
leader runs the reconcile workers and starts the DynamoDB informers; standby
replicas wait.

## Cross-account access

The MC account holds the EKS cluster. The RC account holds the DynamoDB tables.
[EKS Pod Identity](https://docs.aws.amazon.com/eks/latest/userguide/pod-identities.html)
associates the `kube-applier` ServiceAccount in the `kube-applier` namespace
to an IAM role in the MC account. That role carries two inline policies
(specs: read + GSI query; status: read-write) scoped to the RC account table ARNs.
No static credentials are used. See [deployment.md](deployment.md) for the
full IAM details.

The `spec.managementCluster` field carried inside each desire document is
metadata only. Isolation between MCs is enforced by table naming (`{mc}-*`)
and IAM — each controller role is scoped to its own MC's tables.

## Scale characteristics

| Dimension | Behaviour |
|---|---|
| Replicas | 1 active (leader elected); N standby |
| Per-table concurrency | Configurable worker threadiness (default: 4 for apply, 1 for read manager) |
| GSI fast-poll interval | 15 s (configurable via `--dynamo-poll-interval`) |
| Full relist interval | 5 m (configurable via `--dynamo-relist-interval`) |
| ApplyDesire SSA cooldown | 10 min for unchanged desires |
| ApplyDesire Delete cooldown | 1 min (short to poll for finalizer completion) |
| ReadDesire resync | 60 s unconditional ticker |

## Divergence from kube-applier-gcp

| Dimension | GCP (Firestore) | AWS (DynamoDB) |
|---|---|---|
| Storage | Firestore named databases | DynamoDB tables |
| Change stream | gRPC `collection.Snapshots()` | GSI two-speed poll (15 s fast / 5 m relist) |
| Concurrency guard | Firestore `LastUpdateTime` precondition | `version` counter + `ConditionExpression` |
| API errors | gRPC status codes | Go sentinel errors (`ErrNotFound`, `ErrPreconditionFailed`, `ErrAlreadyExists`) |
| Metadata type | `FirestoreMetadata` | `DynamoDBMetadata` |
| `KubeContent` wire format | Firestore nested map | JSON string in `spec_kubeContent` / `status_kubeContent` S attribute |
| Credential delivery | GKE Workload Identity | EKS Pod Identity (cross-account) |

For the hyperfleet-operator side of the desire loop — how desires are written
and how status is consumed — see the
[hyperfleet-operator architecture doc](https://github.com/typeid/hyperfleet-operator/blob/main/docs/architecture.md).
