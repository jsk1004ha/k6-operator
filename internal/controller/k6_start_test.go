package controllers

import (
	"context"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/grafana/k6-operator/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestClaimSetupExecution(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		plz         bool
		setupStatus *metav1.ConditionStatus
		wantState   setupExecutionState
		wantStatus  metav1.ConditionStatus
	}{
		{
			name:        "claims an unexecuted PLZ setup",
			plz:         true,
			setupStatus: conditionStatus(metav1.ConditionFalse),
			wantState:   setupExecutionClaimed,
			wantStatus:  metav1.ConditionUnknown,
		},
		{
			name:        "recognizes a completed PLZ setup",
			plz:         true,
			setupStatus: conditionStatus(metav1.ConditionTrue),
			wantState:   setupExecutionCompleted,
			wantStatus:  metav1.ConditionTrue,
		},
		{
			name:        "waits for a claimed PLZ setup",
			plz:         true,
			setupStatus: conditionStatus(metav1.ConditionUnknown),
			wantState:   setupExecutionInProgress,
			wantStatus:  metav1.ConditionUnknown,
		},
		{
			name:       "claims a PLZ setup when upgrading a resource without the condition",
			plz:        true,
			wantState:  setupExecutionClaimed,
			wantStatus: metav1.ConditionUnknown,
		},
		{
			name:        "does not claim setup for a non-PLZ test run",
			setupStatus: conditionStatus(metav1.ConditionFalse),
			wantState:   setupExecutionNotRequired,
			wantStatus:  metav1.ConditionFalse,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			r, objectKey := setupConditionTestReconciler(t, setupConditionTestRun(tt.plz, tt.setupStatus))
			current := getSetupConditionTestRun(t, ctx, r, objectKey)

			state, err := claimSetupExecution(ctx, current, r, logr.Discard())
			if err != nil {
				t.Fatalf("claimSetupExecution() error = %v", err)
			}
			if state != tt.wantState {
				t.Errorf("claimSetupExecution() state = %v, want %v", state, tt.wantState)
			}

			persisted := getSetupConditionTestRun(t, ctx, r, objectKey)
			condition := meta.FindStatusCondition(persisted.Status.Conditions, v1alpha1.SetupExecuted)
			if condition == nil {
				t.Fatal("SetupExecuted condition was not persisted")
			}
			if condition.Status != tt.wantStatus {
				t.Errorf("persisted SetupExecuted = %v, want %v", condition.Status, tt.wantStatus)
			}

			if tt.wantState == setupExecutionClaimed {
				stateAgain, err := claimSetupExecution(ctx, persisted, r, logr.Discard())
				if err != nil {
					t.Fatalf("second claimSetupExecution() error = %v", err)
				}
				if stateAgain != setupExecutionInProgress {
					t.Errorf("second claim state = %v, want %v", stateAgain, setupExecutionInProgress)
				}
			}
		})
	}
}

func TestClaimSetupExecutionUsesOptimisticLock(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	r, objectKey := setupConditionTestReconciler(
		t,
		setupConditionTestRun(true, conditionStatus(metav1.ConditionFalse)),
	)
	first := getSetupConditionTestRun(t, ctx, r, objectKey)
	stale := first.DeepCopy()

	state, err := claimSetupExecution(ctx, first, r, logr.Discard())
	if err != nil {
		t.Fatalf("first claimSetupExecution() error = %v", err)
	}
	if state != setupExecutionClaimed {
		t.Fatalf("first claim state = %v, want %v", state, setupExecutionClaimed)
	}

	state, err = claimSetupExecution(ctx, stale, r, logr.Discard())
	if state == setupExecutionClaimed {
		t.Error("stale reconciler unexpectedly claimed setup execution")
	}
	if err == nil {
		t.Fatal("stale reconciler did not receive an optimistic-lock error")
	}
	if !apierrors.IsConflict(err) {
		t.Errorf("stale reconciler error = %v, want a conflict", err)
	}
}

func TestResetSetupExecutionAllowsRetry(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	r, objectKey := setupConditionTestReconciler(
		t,
		setupConditionTestRun(true, conditionStatus(metav1.ConditionFalse)),
	)
	current := getSetupConditionTestRun(t, ctx, r, objectKey)

	state, err := claimSetupExecution(ctx, current, r, logr.Discard())
	if err != nil {
		t.Fatalf("claimSetupExecution() error = %v", err)
	}
	if state != setupExecutionClaimed {
		t.Fatalf("setup execution state = %v, want %v", state, setupExecutionClaimed)
	}

	if err := resetSetupExecution(ctx, current, r, logr.Discard()); err != nil {
		t.Fatalf("resetSetupExecution() error = %v", err)
	}

	persisted := getSetupConditionTestRun(t, ctx, r, objectKey)
	if !v1alpha1.IsFalse(persisted, v1alpha1.SetupExecuted) {
		t.Error("SetupExecuted was not reset after a retryable setup error")
	}

	state, err = claimSetupExecution(ctx, persisted, r, logr.Discard())
	if err != nil {
		t.Fatalf("retry claimSetupExecution() error = %v", err)
	}
	if state != setupExecutionClaimed {
		t.Errorf("retry claim state = %v, want %v", state, setupExecutionClaimed)
	}
}

func TestCompleteSetupExecutionAllowsStarter(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	r, objectKey := setupConditionTestReconciler(
		t,
		setupConditionTestRun(true, conditionStatus(metav1.ConditionFalse)),
	)
	current := getSetupConditionTestRun(t, ctx, r, objectKey)

	state, err := claimSetupExecution(ctx, current, r, logr.Discard())
	if err != nil {
		t.Fatalf("claimSetupExecution() error = %v", err)
	}
	if state != setupExecutionClaimed {
		t.Fatalf("setup execution state = %v, want %v", state, setupExecutionClaimed)
	}

	if err := completeSetupExecution(ctx, current, r, logr.Discard()); err != nil {
		t.Fatalf("completeSetupExecution() error = %v", err)
	}

	persisted := getSetupConditionTestRun(t, ctx, r, objectKey)
	if !v1alpha1.IsTrue(persisted, v1alpha1.SetupExecuted) {
		t.Error("SetupExecuted was not marked complete after setup succeeded")
	}

	state, err = claimSetupExecution(ctx, persisted, r, logr.Discard())
	if err != nil {
		t.Fatalf("completed claimSetupExecution() error = %v", err)
	}
	if state != setupExecutionCompleted {
		t.Errorf("completed setup state = %v, want %v", state, setupExecutionCompleted)
	}
	if !setupExecutionAllowsStarter(state) {
		t.Error("completed setup did not allow starter creation")
	}
}

func TestSetupExecutionAllowsStarter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		state setupExecutionState
		want  bool
	}{
		{state: setupExecutionNotRequired, want: true},
		{state: setupExecutionClaimed, want: false},
		{state: setupExecutionInProgress, want: false},
		{state: setupExecutionCompleted, want: true},
	}

	for _, tt := range tests {
		if got := setupExecutionAllowsStarter(tt.state); got != tt.want {
			t.Errorf("setupExecutionAllowsStarter(%v) = %v, want %v", tt.state, got, tt.want)
		}
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

func getSetupConditionTestRun(
	t *testing.T,
	ctx context.Context,
	r *TestRunReconciler,
	key types.NamespacedName,
) *v1alpha1.TestRun {
	t.Helper()

	k6 := &v1alpha1.TestRun{}
	if err := r.Get(ctx, key, k6); err != nil {
		t.Fatalf("fetching persisted TestRun: %v", err)
	}
	return k6
}

func conditionStatus(status metav1.ConditionStatus) *metav1.ConditionStatus {
	return &status
}
