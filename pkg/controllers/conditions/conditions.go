// Package conditions provides typed setters for the well-known
// kube-applier *Desire conditions (Successful, Degraded).
package conditions

import (
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/openshift-online/rosa-hyperfleet-kube-applier/api/kubeapplier"
)

// PreCheckError is an error type controllers raise when they cannot even reach
// the kube-apiserver — typically a malformed spec, a GVR that does not resolve,
// or a namespace mismatch.
type PreCheckError struct {
	Err error
}

func (e *PreCheckError) Error() string { return e.Err.Error() }
func (e *PreCheckError) Unwrap() error { return e.Err }

func NewPreCheckError(err error) error { return &PreCheckError{Err: err} }

// SetSuccessful records the result of a single sync attempt.
//   - nil err          -> Successful=True, reason=NoErrors
//   - *PreCheckError   -> Successful=False, reason=PreCheckFailed
//   - any other err    -> Successful=False, reason=KubeAPIError
func SetSuccessful(conds *[]metav1.Condition, err error) {
	if err == nil {
		meta.SetStatusCondition(conds, metav1.Condition{
			Type:    kubeapplier.ConditionTypeSuccessful,
			Status:  metav1.ConditionTrue,
			Reason:  kubeapplier.ConditionReasonNoErrors,
			Message: "As expected.",
		})
		return
	}
	reason := kubeapplier.ConditionReasonKubeAPIError
	if _, ok := err.(*PreCheckError); ok {
		reason = kubeapplier.ConditionReasonPreCheckFailed
	}
	meta.SetStatusCondition(conds, metav1.Condition{
		Type:    kubeapplier.ConditionTypeSuccessful,
		Status:  metav1.ConditionFalse,
		Reason:  reason,
		Message: err.Error(),
	})
}

// SetSuccessfulWaitingForDeletion records the "deletion is in flight"
// state for a DeleteDesire whose target still exists in the cluster.
func SetSuccessfulWaitingForDeletion(conds *[]metav1.Condition, deletionTime metav1.Time, uid types.UID) {
	meta.SetStatusCondition(conds, metav1.Condition{
		Type:   kubeapplier.ConditionTypeSuccessful,
		Status: metav1.ConditionFalse,
		Reason: kubeapplier.ConditionReasonWaitingForDeletion,
		Message: fmt.Sprintf("waiting for deletion: deletionTimestamp=%s uid=%s",
			deletionTime.UTC().Format(time.RFC3339), uid),
	})
}

// SetDegraded records controller-level health.
func SetDegraded(conds *[]metav1.Condition, err error) {
	if err == nil {
		meta.SetStatusCondition(conds, metav1.Condition{
			Type:    kubeapplier.ConditionTypeDegraded,
			Status:  metav1.ConditionFalse,
			Reason:  kubeapplier.ConditionReasonNoErrors,
			Message: "As expected.",
		})
		return
	}
	meta.SetStatusCondition(conds, metav1.Condition{
		Type:    kubeapplier.ConditionTypeDegraded,
		Status:  metav1.ConditionTrue,
		Reason:  kubeapplier.ConditionReasonFailed,
		Message: fmt.Sprintf("Had an error while syncing: %s", err.Error()),
	})
}
