package controllers

import (
	"context"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/grafana/k6-operator/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestClaimSetupExecution(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		plz               bool
		setupStatus       *metav1.ConditionStatus
		wantClaimed       bool
		wantSetupExecuted bool
	}{
		{
			name:              "claims an unexecuted PLZ setup",
			plz:               true,
			setupStatus:       conditionStatus(metav1.ConditionFalse),
			wantClaimed:       true,
			wantSetupExecuted: true,
		},
		{
			name:              "does not claim an already executed PLZ setup",
			plz:               true,
			setupStatus:       conditionStatus(metav1.ConditionTrue),
			wantSetupExecuted: true,
		},
		{
			name:              "claims a PLZ setup when upgrading a resource without the condition",
			plz:               true,
			wantClaimed:       true,
			wantSetupExecuted: true,
		},
		{
			name:        "does not claim setup for a non-PLZ test run",
			setupStatus: conditionStatus(metav1.ConditionFalse),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			k6 := setupConditionTestRun(tt.plz, tt.setupStatus)
			r, objectKey := setupConditionTestReconciler(t, k6)

			claimed, err := claimSetupExecution(ctx, k6, r, logr.Discard())
			if err != nil {
				t.Fatalf("claimSetupExecution() error = %v", err)
			}
			if claimed != tt.wantClaimed {
				t.Errorf("claimSetupExecution() claimed = %v, want %v", claimed, tt.wantClaimed)
			}

			persisted := &v1alpha1.TestRun{}
			if err := r.Get(ctx, objectKey, persisted); err != nil {
				t.Fatalf("fetching persisted TestRun: %v", err)
			}
			if got := v1alpha1.IsTrue(persisted, v1alpha1.SetupExecuted); got != tt.wantSetupExecuted {
				t.Errorf("persisted SetupExecuted = %v, want %v", got, tt.wantSetupExecuted)
			}

			if tt.wantClaimed {
				claimedAgain, err := claimSetupExecution(ctx, persisted, r, logr.Discard())
				if err != nil {
					t.Fatalf("second claimSetupExecution() error = %v", err)
				}
				if claimedAgain {
					t.Error("second claimSetupExecution() claimed setup again")
				}
			}
		})
	}
}

func TestResetSetupExecution(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	k6 := setupConditionTestRun(
		true,
		conditionStatus(metav1.ConditionTrue),
	)
	r, objectKey := setupConditionTestReconciler(t, k6)

	if err := resetSetupExecution(ctx, k6, r, logr.Discard()); err != nil {
		t.Fatalf("resetSetupExecution() error = %v", err)
	}

	persisted := &v1alpha1.TestRun{}
	if err := r.Get(ctx, objectKey, persisted); err != nil {
		t.Fatalf("fetching persisted TestRun: %v", err)
	}
	if !v1alpha1.IsFalse(persisted, v1alpha1.SetupExecuted) {
		t.Error("SetupExecuted was not reset after a retryable setup error")
	}
}

func setupConditionTestRun(
	plz bool,
	setupStatus *metav1.ConditionStatus,
) *v1alpha1.TestRun {
	oldTransitionTime := metav1.NewTime(time.Now().Add(-time.Minute))
	conditions := []metav1.Condition{
		{
			Type:               v1alpha1.CloudPLZTestRun,
			Status:             metav1.ConditionFalse,
			LastTransitionTime: oldTransitionTime,
			Reason:             "CloudPLZTestRunFalse",
		},
	}
	if plz {
		conditions[0].Status = metav1.ConditionTrue
		conditions[0].Reason = "CloudPLZTestRunTrue"
	}
	if setupStatus != nil {
		conditions = append(conditions, metav1.Condition{
			Type:               v1alpha1.SetupExecuted,
			Status:             *setupStatus,
			LastTransitionTime: oldTransitionTime,
			Reason:             v1alpha1.SetupExecuted + string(*setupStatus),
		})
	}

	return &v1alpha1.TestRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-run",
			Namespace: "default",
		},
		Status: v1alpha1.TestRunStatus{
			Stage:      "created",
			Conditions: conditions,
		},
	}
}

func setupConditionTestReconciler(
	t *testing.T,
	k6 *v1alpha1.TestRun,
) (*TestRunReconciler, types.NamespacedName) {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("adding TestRun to scheme: %v", err)
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1alpha1.TestRun{}).
		WithObjects(k6).
		Build()

	return &TestRunReconciler{Client: client, Scheme: scheme}, k6.NamespacedName()
}

func conditionStatus(status metav1.ConditionStatus) *metav1.ConditionStatus {
	return &status
}
