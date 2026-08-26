// Package keys defines typed workqueue keys for the kube-applier *Desire
// controllers. Each key is a small comparable struct that the controller
// can use to look up the desire directly.
package keys

import (
	"fmt"

	"github.com/openshift-online/rosa-hyperfleet-kube-applier/api/kubeapplier"
)

// ApplyDesireKey identifies a single ApplyDesire. It is used for both
// Type=ServerSideApply and Type=Delete desires, since both are stored in
// the applydesires table.
type ApplyDesireKey struct {
	ClusterID    string
	NodePoolName string
	Name         string
}

func (k ApplyDesireKey) IsNodePoolScoped() bool { return k.NodePoolName != "" }

func ApplyDesireKeyFromDesire(d *kubeapplier.ApplyDesire) (ApplyDesireKey, error) {
	if d.DocumentID == "" {
		return ApplyDesireKey{}, fmt.Errorf("ApplyDesire has empty DocumentID")
	}
	return ApplyDesireKey{
		ClusterID:    d.Spec.ClusterID,
		NodePoolName: d.Spec.NodePoolName,
		Name:         d.DocumentID,
	}, nil
}

// ReadDesireKey identifies a single ReadDesire.
type ReadDesireKey struct {
	ClusterID    string
	NodePoolName string
	Name         string
}

func (k ReadDesireKey) IsNodePoolScoped() bool { return k.NodePoolName != "" }

func ReadDesireKeyFromDesire(d *kubeapplier.ReadDesire) (ReadDesireKey, error) {
	if d.DocumentID == "" {
		return ReadDesireKey{}, fmt.Errorf("ReadDesire has empty DocumentID")
	}
	return ReadDesireKey{
		ClusterID:    d.Spec.ClusterID,
		NodePoolName: d.Spec.NodePoolName,
		Name:         d.DocumentID,
	}, nil
}
