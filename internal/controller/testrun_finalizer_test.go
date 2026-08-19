package controllers

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/go-logr/logr"
	"github.com/grafana/k6-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	k8sErrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

func TestNeedsTestRunFinalizer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		k6   *v1alpha1.TestRun
		want bool
	}{
		{
			name: "PLZ test run",
			k6: &v1alpha1.TestRun{Spec: v1alpha1.TestRunSpec{
				TestRunID: "123",
			}},
			want: true,
		},
		{
			name: "cloud output test run",
			k6: &v1alpha1.TestRun{Spec: v1alpha1.TestRunSpec{
				Arguments: "--out cloud",
			}},
			want: true,
		},
		{
			name: "cloud output test run with argv",
			k6: &v1alpha1.TestRun{Spec: v1alpha1.TestRunSpec{
				Args: []string{"--out", "cloud"},
			}},
			want: true,
		},
		{
			name: "cloud test identified by status",
			k6: &v1alpha1.TestRun{Status: v1alpha1.TestRunStatus{
				TestRunID: "456",
			}},
			want: true,
		},
		{
			name: "local test run",
			k6: &v1alpha1.TestRun{Spec: v1alpha1.TestRunSpec{
				Arguments: "--vus 10",
			}},
		},
		{
			name: "invalid CLI is not treated as cloud output",
			k6: &v1alpha1.TestRun{Spec: v1alpha1.TestRunSpec{
				Args: []string{"unexpected-positional-value"},
			}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := needsTestRunFinalizer(tt.k6); got != tt.want {
				t.Errorf("needsTestRunFinalizer() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestShouldNotifyCloudAboutDeletion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		testRunID string
		stage     v1alpha1.Stage
		aborted   bool
		finalized bool
		want      bool
	}{
		{
			name:      "active cloud test",
			testRunID: "123",
			stage:     "started",
			want:      true,
		},
		{
			name:  "cloud run not created yet",
			stage: "initialization",
		},
		{
			name:      "finished cloud test",
			testRunID: "123",
			stage:     "finished",
		},
		{
			name:      "already aborted cloud test",
			testRunID: "123",
			stage:     "stopped",
			aborted:   true,
		},
		{
			name:      "already finalized cloud test",
			testRunID: "123",
			stage:     "stopped",
			finalized: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			k6 := &v1alpha1.TestRun{
				Spec:   v1alpha1.TestRunSpec{TestRunID: tt.testRunID},
				Status: v1alpha1.TestRunStatus{Stage: tt.stage},
			}
			if tt.aborted {
				v1alpha1.UpdateCondition(k6, v1alpha1.CloudTestRunAborted, metav1.ConditionTrue)
			}
			if tt.finalized {
				v1alpha1.UpdateCondition(k6, v1alpha1.CloudTestRunFinalized, metav1.ConditionTrue)
			}

			if got := shouldNotifyCloudAboutDeletion(k6); got != tt.want {
				t.Errorf("shouldNotifyCloudAboutDeletion() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestReconcileAddsCloudTestRunFinalizer(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	k6 := &v1alpha1.TestRun{
		ObjectMeta: metav1.ObjectMeta{Name: "cloud-test", Namespace: "default"},
		Spec: v1alpha1.TestRunSpec{
			Parallelism: 1,
			Arguments:   "--out cloud",
		},
	}
	r := finalizerTestReconciler(t, k6)

	result, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: k6.NamespacedName()})
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if !result.Requeue {
		t.Error("Reconcile() did not request another pass after adding the finalizer")
	}

	persisted := &v1alpha1.TestRun{}
	if err := r.Get(ctx, k6.NamespacedName(), persisted); err != nil {
		t.Fatalf("fetching persisted TestRun: %v", err)
	}
	if !controllerutil.ContainsFinalizer(persisted, testRunFinalizer) {
		t.Errorf("finalizers = %v, want %q", persisted.Finalizers, testRunFinalizer)
	}
}

func TestFinalizeTestRunNotifiesCloudBeforeRemovingFinalizer(t *testing.T) {
	t.Parallel()

	var requests atomic.Int32
	bodyCh := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		requests.Add(1)
		if req.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", req.Method)
		}
		if req.URL.Path != "/orchestrator/v1/testruns/123/events" {
			t.Errorf("path = %s, want /orchestrator/v1/testruns/123/events", req.URL.Path)
		}
		data, err := io.ReadAll(req.Body)
		if err != nil {
			t.Errorf("reading request body: %v", err)
		}
		bodyCh <- string(data)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()

	ctx := context.Background()
	k6, secret := deletingPLZTestRun(server.URL+"/v1", "started")
	key := k6.NamespacedName()
	r := finalizerTestReconciler(t, k6, secret)

	if _, err := r.finalizeTestRun(ctx, k6, logr.Discard()); err != nil {
		t.Fatalf("finalizeTestRun() error = %v", err)
	}
	if requests.Load() != 1 {
		t.Fatalf("cloud event requests = %d, want 1", requests.Load())
	}
	body := <-bodyCh
	if !strings.Contains(body, `"event_type":"TestRunAbortEvent"`) {
		t.Errorf("request body %s does not contain an abort event", body)
	}
	if !strings.Contains(body, `"reason":"`+testRunDeletedReason+`"`) {
		t.Errorf("request body %s does not contain deletion reason", body)
	}
	if controllerutil.ContainsFinalizer(k6, testRunFinalizer) {
		t.Error("finalizer was not removed after Cloud acknowledged the abort")
	}
	assertTestRunDeletedOrFinalizerRemoved(t, r, key)
}

func TestFinalizeTestRunRetainsFinalizerWhenCloudFails(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "temporary failure", http.StatusServiceUnavailable)
	}))
	defer server.Close()

	ctx := context.Background()
	k6, secret := deletingPLZTestRun(server.URL+"/v1", "started")
	r := finalizerTestReconciler(t, k6, secret)

	result, err := r.finalizeTestRun(ctx, k6, logr.Discard())
	if err == nil {
		t.Fatal("finalizeTestRun() error = nil, want Cloud API error")
	}
	if result.RequeueAfter == 0 {
		t.Error("finalizeTestRun() did not schedule a retry")
	}
	if !controllerutil.ContainsFinalizer(k6, testRunFinalizer) {
		t.Error("finalizer was removed even though Cloud notification failed")
	}
}

func TestFinalizeTestRunRetainsFinalizerUntilTokenIsAvailable(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	k6, _ := deletingPLZTestRun("http://cloud.invalid/v1", "started")
	r := finalizerTestReconciler(t, k6)

	result, err := r.finalizeTestRun(ctx, k6, logr.Discard())
	if err != nil {
		t.Fatalf("finalizeTestRun() error = %v", err)
	}
	if result.RequeueAfter == 0 {
		t.Error("finalizeTestRun() did not schedule a retry while the token was unavailable")
	}
	if !controllerutil.ContainsFinalizer(k6, testRunFinalizer) {
		t.Error("finalizer was removed before a Cloud client could be created")
	}
}

func TestFinalizeFinishedTestRunSkipsAbort(t *testing.T) {
	t.Parallel()

	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	ctx := context.Background()
	k6, secret := deletingPLZTestRun(server.URL+"/v1", "finished")
	key := k6.NamespacedName()
	r := finalizerTestReconciler(t, k6, secret)

	if _, err := r.finalizeTestRun(ctx, k6, logr.Discard()); err != nil {
		t.Fatalf("finalizeTestRun() error = %v", err)
	}
	if requests.Load() != 0 {
		t.Errorf("cloud event requests = %d, want 0", requests.Load())
	}
	assertTestRunDeletedOrFinalizerRemoved(t, r, key)
}

func deletingPLZTestRun(host string, stage v1alpha1.Stage) (*v1alpha1.TestRun, *corev1.Secret) {
	deletionTimestamp := metav1.Now()
	k6 := &v1alpha1.TestRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "plz-test",
			Namespace:         "default",
			Finalizers:        []string{testRunFinalizer},
			DeletionTimestamp: &deletionTimestamp,
		},
		Spec: v1alpha1.TestRunSpec{
			Parallelism: 1,
			TestRunID:   "123",
			Token:       "cloud-token",
			Runner: v1alpha1.Pod{
				Env: []corev1.EnvVar{{Name: "K6_CLOUD_HOST", Value: host}},
			},
		},
		Status: v1alpha1.TestRunStatus{Stage: stage},
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "cloud-token", Namespace: "default"},
		Data:       map[string][]byte{"token": []byte("secret")},
	}
	return k6, secret
}

func finalizerTestReconciler(t *testing.T, objects ...runtime.Object) *TestRunReconciler {
	t.Helper()

	scheme := runtime.NewScheme()
	if err := v1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("adding TestRun to scheme: %v", err)
	}
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("adding core resources to scheme: %v", err)
	}

	builder := fake.NewClientBuilder().
		WithScheme(scheme).
		WithRuntimeObjects(objects...)

	return &TestRunReconciler{
		Client: builder.Build(),
		Log:    logr.Discard(),
		Scheme: scheme,
	}
}

func assertTestRunDeletedOrFinalizerRemoved(t *testing.T, r *TestRunReconciler, key types.NamespacedName) {
	t.Helper()
	persisted := &v1alpha1.TestRun{}
	if err := r.Get(context.Background(), key, persisted); err != nil {
		if !k8sErrors.IsNotFound(err) {
			t.Fatalf("fetching TestRun after finalization: %v", err)
		}
		return
	}
	if controllerutil.ContainsFinalizer(persisted, testRunFinalizer) {
		t.Errorf("persisted finalizers = %v, still contains %q", persisted.Finalizers, testRunFinalizer)
	}
}
