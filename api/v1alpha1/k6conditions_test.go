package v1alpha1

import (
	"testing"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestInitializeExecutionConditions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		testRunID string
	}{
		{
			name: "regular test run",
		},
		{
			name:      "PLZ test run",
			testRunID: "test-run-id",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			k6 := &TestRun{Spec: TestRunSpec{TestRunID: tt.testRunID}}
			Initialize(k6)

			for _, conditionType := range []string{SetupExecuted, TeardownExecuted} {
				condition := meta.FindStatusCondition(k6.Status.Conditions, conditionType)
				if condition == nil {
					t.Fatalf("condition %q was not initialized", conditionType)
				}
				if condition.Status != metav1.ConditionFalse {
					t.Errorf("condition %q status = %q, want %q", conditionType, condition.Status, metav1.ConditionFalse)
				}
				if condition.Reason != conditionType+"False" {
					t.Errorf("condition %q reason = %q, want %q", conditionType, condition.Reason, conditionType+"False")
				}
			}
		})
	}
}
