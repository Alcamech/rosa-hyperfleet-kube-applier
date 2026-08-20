# Deployment

## Production deployment

The production deployment of `kube-applier-aws` is defined in the
`rosa-regional-platform` repository (`team/kas-poc` branch). One controller
replica runs per Management Cluster, deployed via ArgoCD and provisioned via
Terraform + CodePipeline.

### DynamoDB table provisioning

Tables are created and managed by the `MintDynamoDB` CodeBuild action, which
runs in the RC account as part of the MC onboarding pipeline.

**Terraform modules:**

| Module | Location | Purpose |
|---|---|---|
| `kube-applier-dynamodb` | `terraform/modules/kube-applier-dynamodb/` | Declares the 4 DynamoDB tables, `updateTime-index` GSI, and cross-account resource-based policies |
| Top-level config | `terraform/config/kube-applier-dynamodb-provisioning/` | Calls the module; one invocation per MC |

**Pipeline integration:**

The `MintDynamoDB` CodeBuild project runs in the RC account. It is triggered
as part of the `Mint-IoT` pipeline stage, in parallel with
`MintIoTCertificate`. The buildspec is at
`scripts/buildspec/buildspec-dynamodb-mint.yml`; the entrypoint script is
`scripts/buildspec/dynamodb-mint.sh`.

Terraform state is stored in S3:

```
s3://terraform-state-{rc-account-id}-{region}/kube-applier-dynamodb/{cluster-id}.tfstate
```

**PITR:** enabled for all non-ephemeral environments. For ephemeral environments
`enable_pitr = false`.

**Deletion:** set `delete: true` in the MC config file
(`deploy/{env}/{region}/pipeline-management-cluster-{mc}-inputs/terraform.json`)
or pass `IS_DESTROY=true` to the CodeBuild job.

**Table naming:**

```
{mc}-specs-applydesires    (GSI: updateTime-index)
{mc}-specs-readdesires     (GSI: updateTime-index)
{mc}-status-applydesires
{mc}-status-readdesires
```

Partition key is `documentID` (string) in every table. The specs tables carry a
`updateTime-index` GSI (hash key: `shard`, range key: `updateTime`) used by
the two-speed polling watcher. DynamoDB Streams are not enabled. Deletion is
modelled as `ApplyDesire` with `spec.type=Delete` — there are no separate
`deletedesires` tables.

### IAM

Two IAM roles are involved.

#### MC account — controller role

Managed by `terraform/modules/kube-applier/` (called per MC in the MC account).

**Role name:** `{mc}-kube-applier`

**Trust policy:** `pods.eks.amazonaws.com` (EKS Pod Identity)

**Inline policy — specs (read + GSI query):**

| Action | Resource |
|---|---|
| `dynamodb:GetItem`, `Scan`, `Query`, `BatchGetItem` | `arn:aws:dynamodb:{region}:{rc-account}:table/{mc}-specs-*` |
| `dynamodb:Query` | `arn:aws:dynamodb:{region}:{rc-account}:table/{mc}-specs-*/index/*` |

**Inline policy — status (read-write):**

| Action | Resource |
|---|---|
| `dynamodb:GetItem`, `Scan`, `PutItem`, `DeleteItem` | `arn:aws:dynamodb:{region}:{rc-account}:table/{mc}-status-*` |

**Pod Identity association:**

```
EKS cluster:      {mc}
Namespace:        kube-applier
ServiceAccount:   kube-applier
IAM role ARN:     arn:aws:iam::{mc-account}:role/{mc}-kube-applier
```

#### RC account — table resource-based policies

The `kube-applier-dynamodb` module attaches `aws_dynamodb_resource_policy`
resources to each table, granting the MC controller role the same actions as
the identity-based policies above. Both layers are required for cross-account
DynamoDB access.

### Helm chart and ArgoCD

The Helm chart lives at
`argocd/config/management-cluster/kube-applier/` in `rosa-regional-platform`.
ArgoCD deploys it via an ApplicationSet that injects `global.cluster_name`
(the MC name) and `global.aws_region` at render time.

**Namespace:** `kube-applier`

**Deployment args (rendered by Helm):**

| Flag | Value |
|---|---|
| `--management-cluster` | `global.cluster_name` |
| `--aws-region` | `global.aws_region` |
| `--namespace` | `kube-applier` (`.Release.Namespace`) |
| `--specs-table` | `arn:aws:dynamodb:{region}:{rc-account}:table/{cluster_name}-specs` |
| `--status-table` | `arn:aws:dynamodb:{region}:{rc-account}:table/{cluster_name}-status` |
| `--metrics-listen-address` | `:8081` |
| `--healthz-listen-address` | `:8083` |
| `--leader-election-id` | `kube-applier` |
| `--log-verbosity` | `0` |
| `--exit-on-panic` | `true` |
| `--aws-endpoint-url` | only rendered when `kubeApplier.config.awsEndpointUrl` is non-empty |

**Health probes:** liveness and readiness both hit `GET /healthz` on port 8083
(initial delay 15 s / 5 s respectively).

**Resources:**

| | CPU | Memory |
|---|---|---|
| Request | 100m | 128Mi |
| Limit | 500m | 256Mi |

**ServiceMonitor:** disabled by default (`metrics.serviceMonitor.enabled: false`).
Enable when Prometheus Operator is present in the cluster.

**Image:**

| Stage | Image |
|---|---|
| Builder | `registry.access.redhat.com/ubi9/go-toolset:9.8-1782219569` |
| Runtime | `registry.access.redhat.com/ubi9/ubi-minimal:9.8-1782191395` |

Production image is pushed to `quay.io/psav/kube-applier-aws`.

### RBAC

The Helm chart creates a `ClusterRole` and binds it to the `kube-applier`
ServiceAccount via a `ClusterRoleBinding`.

**ClusterRole rules:**

| API groups | Resources | Verbs |
|---|---|---|
| `*` | `*` | `get`, `list`, `watch`, `create`, `update`, `patch`, `delete` |
| `coordination.k8s.io` | `leases` | `get`, `create`, `update`, `patch`, `delete`, `list`, `watch` |
| `""` | `events` | `create`, `patch` |

The wildcard rule is required because `ApplyDesire` can target any resource
type (including deletion via `spec.type=Delete`). `leases` are used for leader
election in the `kube-applier` namespace. `events` are used for controller
status reporting.

---

## Local development

See the [README quickstart](../README.md#quick-start) for instructions on
running the controller locally against LocalStack (DynamoDB) and a local
kind cluster. The `make integration-test` target runs the full end-to-end
integration test suite using both.
