package controllers

import (
	"context"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	"github.com/grafana/k6-operator/api/v1alpha1"
	"github.com/grafana/k6-operator/pkg/cloud"
	k6types "github.com/grafana/k6-operator/pkg/types"
	k8sErrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const testRunDeletedReason = "TestRun custom resource was deleted"

func needsTestRunFinalizer(k6 *v1alpha1.TestRun) bool {
	if isCloudTestRun(k6) || k6.GetSpec().TestRunID != "" || k6.GetStatus().TestRunID != "" {
		return true
	}

	cli, err := k6types.ParseCLI(k6.GetSpec().Argv())
	return err == nil && cli.HasCloudOut
}

func (r *TestRunReconciler) ensureTestRunFinalizer(
	ctx context.Context,
	k6 *v1alpha1.TestRun,
) (bool, error) {
	if !needsTestRunFinalizer(k6) || controllerutil.ContainsFinalizer(k6, testRunFinalizer) {
		return false, nil
	}

	before := k6.DeepCopy()
	controllerutil.AddFinalizer(k6, testRunFinalizer)
	if err := r.Patch(ctx, k6, client.MergeFrom(before)); err != nil {
		return false, fmt.Errorf("adding TestRun finalizer: %w", err)
	}

	return true, nil
}

func (r *TestRunReconciler) finalizeTestRun(
	ctx context.Context,
	k6 *v1alpha1.TestRun,
	log logr.Logger,
) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(k6, testRunFinalizer) {
		return ctrl.Result{}, nil
	}

	if shouldNotifyCloudAboutDeletion(k6) {
		cloudClient, found, err := r.createClient(ctx, k6, log)
		if err != nil {
			return ctrl.Result{RequeueAfter: 5 * time.Second}, err
		}
		if !found {
			return ctrl.Result{RequeueAfter: time.Second}, nil
		}

		events := cloud.Events{cloud.AbortEvent(cloud.OriginUser)}
		(&events).WithDetail(testRunDeletedReason)
		if err := cloud.SendTestRunEventsWithError(cloudClient, k6.TestRunID(), log, &events); err != nil {
			return ctrl.Result{RequeueAfter: 5 * time.Second}, err
		}
	}

	before := k6.DeepCopy()
	controllerutil.RemoveFinalizer(k6, testRunFinalizer)
	if err := r.Patch(ctx, k6, client.MergeFrom(before)); err != nil {
		if k8sErrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{RequeueAfter: time.Second}, fmt.Errorf("removing TestRun finalizer: %w", err)
	}

	return ctrl.Result{}, nil
}

func shouldNotifyCloudAboutDeletion(k6 *v1alpha1.TestRun) bool {
	if !needsTestRunFinalizer(k6) || k6.TestRunID() == "" || k6.GetStatus().Stage == "finished" {
		return false
	}

	return !v1alpha1.IsTrue(k6, v1alpha1.CloudTestRunAborted) &&
		!v1alpha1.IsTrue(k6, v1alpha1.CloudTestRunFinalized)
}
