package main

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/runtime"

	kubeapplier "github.com/openshift-online/rosa-hyperfleet-kube-applier/api/kubeapplier"
	"github.com/openshift-online/rosa-hyperfleet-kube-applier/internal/database"
	"github.com/openshift-online/rosa-hyperfleet-kube-applier/internal/desireid"
)

func newApplyCmd() *cobra.Command {
	var files []string

	cmd := &cobra.Command{
		Use:   "apply -f FILENAME",
		Short: "Apply a configuration to a resource by file",
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(files) == 0 {
				return fmt.Errorf("must specify -f (file or directory)")
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

			ctx := context.Background()
			clients, err := newClientPair(ctx, activeCtx)
			if err != nil {
				return err
			}
			defer clients.Close()

			docs, err := readManifestFiles(files)
			if err != nil {
				return err
			}

			if len(docs) == 0 {
				return fmt.Errorf("no manifests found in specified files")
			}

			var errs []error
			for _, doc := range docs {
				jsonData, err := toJSON(doc)
				if err != nil {
					errs = append(errs, fmt.Errorf("converting manifest: %w", err))
					continue
				}

				action, name, err := applyManifest(ctx, clients, activeCtx, jsonData)
				if err != nil {
					errs = append(errs, err)
					continue
				}
				fmt.Printf("%s %s\n", name, action)
			}

			if len(errs) > 0 {
				for _, e := range errs {
					fmt.Fprintf(cmd.ErrOrStderr(), "error: %v\n", e)
				}
				return fmt.Errorf("%d error(s) occurred", len(errs))
			}
			return nil
		},
	}

	cmd.Flags().StringArrayVarP(&files, "filename", "f", nil, "file or directory containing manifests")

	return cmd
}

func applyManifest(ctx context.Context, clients *clientPair, activeCtx *Context, raw []byte) (action string, displayName string, err error) {
	ref, err := resourceRefFromManifest(raw)
	if err != nil {
		return "", "", fmt.Errorf("parsing manifest: %w", err)
	}

	info, _ := resolveResourceType(ref.Resource)
	if info != nil {
		displayName = formatResourceName(info, ref.Name)
	} else {
		displayName = fmt.Sprintf("%s/%s", ref.Resource, ref.Name)
	}

	docID := desireid.NewDocumentID(taskKey, ref.Group, ref.Version, ref.Resource, ref.Namespace, ref.Name)

	desire := &kubeapplier.ApplyDesire{
		Spec: kubeapplier.ApplyDesireSpec{
			ManagementCluster: activeCtx.ManagementCluster,
			ClusterID:         activeCtx.ClusterID,
			NodePoolName:      activeCtx.NodePool,
			TargetItem:        ref,
			ServerSideApply:   &kubeapplier.ServerSideApplyConfig{KubeContent: &runtime.RawExtension{Raw: raw}},
		},
	}
	desire.SetDocumentID(docID)

	_, err = clients.specsDB.ApplyDesireStatus().Create(ctx, desire)
	if err == nil {
		return "created", displayName, nil
	}

	if !database.IsAlreadyExistsError(err) {
		return "", displayName, fmt.Errorf("creating %s: %w", displayName, err)
	}

	existing, err := clients.specsDB.ApplyDesireStatus().Get(ctx, docID)
	if err != nil {
		return "", displayName, fmt.Errorf("getting %s for update: %w", displayName, err)
	}

	existing.Spec.ServerSideApply = &kubeapplier.ServerSideApplyConfig{KubeContent: &runtime.RawExtension{Raw: raw}}
	existing.Spec.TargetItem = ref
	existing.Spec.ManagementCluster = activeCtx.ManagementCluster
	existing.Spec.ClusterID = activeCtx.ClusterID
	existing.Spec.NodePoolName = activeCtx.NodePool

	_, err = clients.specsDB.ApplyDesireStatus().Replace(ctx, existing)
	if err != nil {
		return "", displayName, fmt.Errorf("updating %s: %w", displayName, err)
	}
	return "configured", displayName, nil
}
