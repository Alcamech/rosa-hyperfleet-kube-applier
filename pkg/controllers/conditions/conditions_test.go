package conditions

import (
	"errors"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/openshift-online/rosa-hyperfleet-kube-applier/api/kubeapplier"
)

func TestSetSuccessful_NilErr(t *testing.T) {
	var conds []metav1.Condition
	SetSuccessful(&conds, nil)

	if len(conds) != 1 {
		t.Fatalf("expected 1 condition, got %d", len(conds))
	}
	c := conds[0]
	if c.Type != kubeapplier.ConditionTypeSuccessful {
		t.Errorf("type: got %s, want %s", c.Type, kubeapplier.ConditionTypeSuccessful)
	}
	if c.Status != metav1.ConditionTrue {
		t.Errorf("status: got %s, want True", c.Status)
	}
	if c.Reason != kubeapplier.ConditionReasonNoErrors {
		t.Errorf("reason: got %s, want %s", c.Reason, kubeapplier.ConditionReasonNoErrors)
	}
}

func TestSetSuccessful_PreCheckError(t *testing.T) {
	var conds []metav1.Condition
	SetSuccessful(&conds, NewPreCheckError(errors.New("bad gvr")))

	if len(conds) != 1 {
		t.Fatalf("expected 1 condition, got %d", len(conds))
	}
	c := conds[0]
	if c.Status != metav1.ConditionFalse {
		t.Errorf("status: got %s, want False", c.Status)
	}
	if c.Reason != kubeapplier.ConditionReasonPreCheckFailed {
		t.Errorf("reason: got %s, want %s", c.Reason, kubeapplier.ConditionReasonPreCheckFailed)
	}
	if c.Message != "bad gvr" {
		t.Errorf("message: got %s, want bad gvr", c.Message)
	}
}

func TestSetSuccessful_KubeAPIError(t *testing.T) {
	var conds []metav1.Condition
	SetSuccessful(&conds, errors.New("connection refused"))

	c := conds[0]
	if c.Reason != kubeapplier.ConditionReasonKubeAPIError {
		t.Errorf("reason: got %s, want %s", c.Reason, kubeapplier.ConditionReasonKubeAPIError)
	}
}

func TestSetSuccessfulWaitingForDeletion(t *testing.T) {
	var conds []metav1.Condition
	dt := metav1.NewTime(time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC))
	uid := types.UID("abc-123")
	SetSuccessfulWaitingForDeletion(&conds, dt, uid)

	c := conds[0]
	if c.Reason != kubeapplier.ConditionReasonWaitingForDeletion {
		t.Errorf("reason: got %s, want %s", c.Reason, kubeapplier.ConditionReasonWaitingForDeletion)
	}
	if c.Status != metav1.ConditionFalse {
		t.Errorf("status: got %s, want False", c.Status)
	}
}

func TestSetDegraded_Nil(t *testing.T) {
	var conds []metav1.Condition
	SetDegraded(&conds, nil)

	c := conds[0]
	if c.Type != kubeapplier.ConditionTypeDegraded {
		t.Errorf("type: got %s, want %s", c.Type, kubeapplier.ConditionTypeDegraded)
	}
	if c.Status != metav1.ConditionFalse {
		t.Errorf("status: got %s, want False", c.Status)
	}
}

func TestSetDegraded_Error(t *testing.T) {
	var conds []metav1.Condition
	SetDegraded(&conds, errors.New("crash"))

	c := conds[0]
	if c.Status != metav1.ConditionTrue {
		t.Errorf("status: got %s, want True", c.Status)
	}
	if c.Reason != kubeapplier.ConditionReasonFailed {
		t.Errorf("reason: got %s, want %s", c.Reason, kubeapplier.ConditionReasonFailed)
	}
}

func TestPreCheckError_Unwrap(t *testing.T) {
	inner := errors.New("original")
	pce := NewPreCheckError(inner)
	if !errors.Is(pce, inner) {
		t.Error("PreCheckError should unwrap to inner error")
	}
}
