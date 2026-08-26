package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"

	kubeapplier "github.com/openshift-online/rosa-hyperfleet-kube-applier/api/kubeapplier"
	"github.com/openshift-online/rosa-hyperfleet-kube-applier/internal/database"
	"github.com/openshift-online/rosa-hyperfleet-kube-applier/internal/desireid"
)

func newGetCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get TYPE [NAME]",
		Short: "Display one or many resources from the cluster",
		Long: `Get live resource state from the cluster by creating ReadDesires
and polling for the observed state.

Examples:
  desirectl get deployments
  desirectl get deployment my-app -n production
  desirectl get cm my-config -o yaml`,
		Args: cobra.RangeArgs(1, 2),
		RunE: func(cmd *cobra.Command, args []string) error {
			resInfo, err := resolveResourceType(args[0])
			if err != nil {
				return err
			}

			path := DefaultConfigPath()
			cfg, err := LoadConfig(path)
			if err != nil {
				return err
			}
			activeCtx, err := cfg.ResolveActiveContext(flagContext)
			if err != nil {
				return err
			}

			timeout, err := time.ParseDuration(flagTimeout)
			if err != nil {
				return fmt.Errorf("invalid timeout: %w", err)
			}

			ctx := context.Background()
			clients, err := newClientPair(ctx, activeCtx)
			if err != nil {
				return err
			}
			defer clients.Close()

			format := parseOutputFormat(flagOutput)
			namespace := activeCtx.EffectiveNamespace(flagNamespace)

			if len(args) == 2 {
				return runGetNamed(ctx, clients, activeCtx, resInfo, args[1], namespace, timeout, format)
			}
			return runGetList(ctx, clients, activeCtx, resInfo, namespace, timeout, format)
		},
	}

	return cmd
}

func runGetNamed(ctx context.Context, clients *clientPair, activeCtx *Context, resInfo *ResourceInfo, name, namespace string, timeout time.Duration, format OutputFormat) error {
	ref := resourceRefFromGVR(resInfo, namespace, name)
	docID := desireid.NewDocumentID(taskKey, ref.Group, ref.Version, ref.Resource, ref.Namespace, ref.Name)

	specTime, err := ensureReadDesire(ctx, clients.specsDB, activeCtx, ref, docID)
	if err != nil {
		return err
	}

	readStatus, err := pollReadDesireStatus(ctx, clients.statusDB, docID, specTime, timeout)
	if err != nil {
		return err
	}

	if readStatus.KubeContent == nil || len(readStatus.KubeContent.Raw) == 0 {
		return fmt.Errorf("no data returned for %s", formatResourceName(resInfo, name))
	}

	switch format {
	case OutputJSON:
		return printResourceJSON(os.Stdout, readStatus.KubeContent.Raw)
	case OutputYAML:
		return printResourceYAML(os.Stdout, readStatus.KubeContent.Raw)
	default:
		rows := []tableRow{extractTableRow(resInfo.Resource, readStatus.KubeContent.Raw)}
		printResourceTable(resInfo.Resource, rows)
		return nil
	}
}

func runGetList(ctx context.Context, clients *clientPair, activeCtx *Context, resInfo *ResourceInfo, namespace string, timeout time.Duration, format OutputFormat) error {
	applyDesires, err := clients.specsDB.ApplyDesireSpecs().List(ctx)
	if err != nil {
		return fmt.Errorf("listing resources: %w", err)
	}

	var matching []*kubeapplier.ApplyDesire
	for _, d := range applyDesires {
		if d.Spec.TargetItem.Resource != resInfo.Resource {
			continue
		}
		if d.Spec.TargetItem.Group != resInfo.Group {
			continue
		}
		if namespace != "" && d.Spec.TargetItem.Namespace != namespace {
			continue
		}
		matching = append(matching, d)
	}

	if len(matching) == 0 {
		fmt.Printf("No resources found.\n")
		return nil
	}

	type result struct {
		name      string
		namespace string
		content   []byte
		err       error
	}

	results := make([]result, len(matching))
	for i, d := range matching {
		ref := d.Spec.TargetItem
		docID := desireid.NewDocumentID(taskKey, ref.Group, ref.Version, ref.Resource, ref.Namespace, ref.Name)

		specTime, err := ensureReadDesire(ctx, clients.specsDB, activeCtx, ref, docID)
		if err != nil {
			results[i] = result{name: ref.Name, namespace: ref.Namespace, err: err}
			continue
		}

		readStatus, err := pollReadDesireStatus(ctx, clients.statusDB, docID, specTime, timeout)
		if err != nil {
			results[i] = result{name: ref.Name, namespace: ref.Namespace, err: err}
			continue
		}

		var content []byte
		if readStatus.KubeContent != nil {
			content = readStatus.KubeContent.Raw
		}
		results[i] = result{name: ref.Name, namespace: ref.Namespace, content: content}
	}

	switch format {
	case OutputJSON:
		var items [][]byte
		for _, r := range results {
			if r.content != nil {
				items = append(items, r.content)
			}
		}
		return printResourceListJSON(os.Stdout, items)
	case OutputYAML:
		var items [][]byte
		for _, r := range results {
			if r.content != nil {
				items = append(items, r.content)
			}
		}
		return printResourceListYAML(os.Stdout, items)
	default:
		var rows []tableRow
		for _, r := range results {
			if r.err != nil {
				fmt.Fprintf(os.Stderr, "warning: %s/%s: %v\n", r.namespace, r.name, r.err)
				continue
			}
			if r.content != nil {
				rows = append(rows, extractTableRow(resInfo.Resource, r.content))
			} else {
				rows = append(rows, tableRow{
					name:      r.name,
					namespace: r.namespace,
					extra:     map[string]string{},
				})
			}
		}
		if len(rows) == 0 {
			fmt.Printf("No resources found.\n")
			return nil
		}
		printResourceTable(resInfo.Resource, rows)
		return nil
	}
}

func ensureReadDesire(ctx context.Context, specsDB database.KubeApplierDBClient, activeCtx *Context, ref kubeapplier.ResourceReference, docID string) (time.Time, error) {
	existing, err := specsDB.ReadDesireStatus().Get(ctx, docID)
	if err == nil {
		// Touch the spec to get fresh status.
		_, err = specsDB.ReadDesireStatus().Replace(ctx, existing)
		if err != nil {
			return time.Time{}, fmt.Errorf("refreshing read desire: %w", err)
		}
		updated, err := specsDB.ReadDesireStatus().Get(ctx, docID)
		if err != nil {
			return time.Time{}, fmt.Errorf("getting updated read desire: %w", err)
		}
		return updated.GetUpdateTime(), nil
	}

	if !database.IsNotFoundError(err) {
		return time.Time{}, fmt.Errorf("checking read desire: %w", err)
	}

	desire := &kubeapplier.ReadDesire{
		Spec: kubeapplier.ReadDesireSpec{
			ManagementCluster: activeCtx.ManagementCluster,
			ClusterID:         activeCtx.ClusterID,
			NodePoolName:      activeCtx.NodePool,
			TargetItem:        ref,
		},
	}
	desire.SetDocumentID(docID)

	created, err := specsDB.ReadDesireStatus().Create(ctx, desire)
	if err != nil {
		if database.IsAlreadyExistsError(err) {
			existing, err := specsDB.ReadDesireStatus().Get(ctx, docID)
			if err != nil {
				return time.Time{}, fmt.Errorf("getting existing read desire: %w", err)
			}
			return existing.GetUpdateTime(), nil
		}
		return time.Time{}, fmt.Errorf("creating read desire: %w", err)
	}
	return created.GetUpdateTime(), nil
}

func pollReadDesireStatus(ctx context.Context, statusDB database.KubeApplierDBClient, docID string, afterTime time.Time, timeout time.Duration) (*kubeapplier.ReadDesireStatus, error) {
	deadline := time.Now().Add(timeout)
	interval := 200 * time.Millisecond
	maxInterval := 2 * time.Second

	for {
		readDesire, err := statusDB.ReadDesireStatus().Get(ctx, docID)
		if err != nil {
			if database.IsNotFoundError(err) {
				if time.Now().After(deadline) {
					return nil, fmt.Errorf("timed out waiting for status (no status document yet)")
				}
				time.Sleep(interval)
				interval = min(interval*2, maxInterval)
				continue
			}
			return nil, fmt.Errorf("getting status: %w", err)
		}

		// If the controller has already observed this version of the desire
		// (ObservedDesireUpdateTime >= afterTime), return immediately.
		fresh := !readDesire.Status.ObservedDesireUpdateTime.IsZero() &&
			!readDesire.Status.ObservedDesireUpdateTime.Before(afterTime)

		// Also accept any existing KubeContent even if the controller hasn't
		// re-synced yet — avoids waiting a full resync period when the data
		// is already present from a prior reconcile.
		hasContent := readDesire.Status.KubeContent != nil && len(readDesire.Status.KubeContent.Raw) > 0

		if fresh || hasContent {
			for _, c := range readDesire.Status.Conditions {
				if c.Type == kubeapplier.ConditionTypeSuccessful && c.Status == "False" {
					return nil, fmt.Errorf("read failed: %s: %s", c.Reason, c.Message)
				}
			}
			return &readDesire.Status, nil
		}

		if time.Now().After(deadline) {
			return nil, fmt.Errorf("timed out waiting for fresh status")
		}

		time.Sleep(interval)
		interval = min(interval*2, maxInterval)
	}
}
