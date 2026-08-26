package main

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"

	kubeapplier "github.com/openshift-online/rosa-hyperfleet-kube-applier/api/kubeapplier"
	"github.com/openshift-online/rosa-hyperfleet-kube-applier/internal/database"
	"github.com/openshift-online/rosa-hyperfleet-kube-applier/internal/desireid"
)

func newDeleteCmd() *cobra.Command {
	var files []string

	cmd := &cobra.Command{
		Use:   "delete TYPE NAME",
		Short: "Delete resources from the cluster",
		Long: `Delete resources by specifying the resource type and name, or by file.

Examples:
  desirectl delete deployment my-app -n production
  desirectl delete configmap my-config
  desirectl delete -f deployment.yaml`,
		RunE: func(cmd *cobra.Command, args []string) error {
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

			if len(files) > 0 {
				return runDeleteFromFile(ctx, cmd, clients, activeCtx, files)
			}

			if len(args) < 2 {
				return fmt.Errorf("must specify TYPE NAME or -f FILENAME")
			}

			resInfo, err := resolveResourceType(args[0])
			if err != nil {
				return err
			}

			namespace := activeCtx.EffectiveNamespace(flagNamespace)
			return runDelete(ctx, clients, activeCtx, resInfo, args[1], namespace)
		},
	}

	cmd.Flags().StringArrayVarP(&files, "filename", "f", nil, "file containing the resource to delete")

	return cmd
}

func runDelete(ctx context.Context, clients *clientPair, activeCtx *Context, resInfo *ResourceInfo, name, namespace string) error {
	ref := resourceRefFromGVR(resInfo, namespace, name)
	docID := desireid.NewDocumentID(taskKey, ref.Group, ref.Version, ref.Resource, ref.Namespace, ref.Name)

	desire := &kubeapplier.ApplyDesire{
		Spec: kubeapplier.ApplyDesireSpec{
			ManagementCluster: activeCtx.ManagementCluster,
			ClusterID:         activeCtx.ClusterID,
			NodePoolName:      activeCtx.NodePool,
			Type:              kubeapplier.ApplyDesireTypeDelete,
			TargetItem:        ref,
		},
	}
	desire.SetDocumentID(docID)

	_, err := clients.specsDB.ApplyDesireStatus().Create(ctx, desire)
	if err != nil {
		if database.IsAlreadyExistsError(err) {
			existing, getErr := clients.specsDB.ApplyDesireStatus().Get(ctx, docID)
			if getErr != nil {
				return fmt.Errorf("getting existing delete desire: %w", getErr)
			}
			existing.Spec = desire.Spec
			if _, replaceErr := clients.specsDB.ApplyDesireStatus().Replace(ctx, existing); replaceErr != nil {
				return fmt.Errorf("updating delete desire: %w", replaceErr)
			}
		} else {
			return fmt.Errorf("creating delete desire: %w", err)
		}
	}

	cleanupRelatedDesires(ctx, clients.specsDB, docID)

	fmt.Printf("%s deleted\n", formatResourceName(resInfo, name))
	return nil
}

func runDeleteFromFile(ctx context.Context, cmd *cobra.Command, clients *clientPair, activeCtx *Context, files []string) error {
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

		ref, err := resourceRefFromManifest(jsonData)
		if err != nil {
			errs = append(errs, fmt.Errorf("parsing manifest: %w", err))
			continue
		}

		info, _ := resourceTypeFromManifest("", ref.Resource)
		if info == nil {
			info = &ResourceInfo{
				Group:    ref.Group,
				Version:  ref.Version,
				Resource: ref.Resource,
			}
		}

		namespace := ref.Namespace
		if namespace == "" {
			namespace = activeCtx.EffectiveNamespace(flagNamespace)
		}

		if err := runDelete(ctx, clients, activeCtx, info, ref.Name, namespace); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		for _, e := range errs {
			fmt.Fprintf(cmd.ErrOrStderr(), "error: %v\n", e)
		}
		return fmt.Errorf("%d error(s) occurred", len(errs))
	}
	return nil
}

func cleanupRelatedDesires(ctx context.Context, specsDB database.KubeApplierDBClient, docID string) {
	specsDB.ApplyDesireStatus().Delete(ctx, docID)
	specsDB.ReadDesireStatus().Delete(ctx, docID)
}
