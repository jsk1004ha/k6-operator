package controllers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/go-logr/logr"

	"github.com/grafana/k6-operator/api/v1alpha1"
	"github.com/grafana/k6-operator/pkg/cloud"
	"github.com/grafana/k6-operator/pkg/testrun"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	errMessageTooLong = "Creation of %s takes too long: your configuration might be off. Check if %v were created successfully."
)

// It may take some time to retrieve inspect output so indicate with boolean if it's ready
// and use returnErr only for errors that require a change of behaviour. All other errors
// should just be logged.
func inspectTestRun(ctx context.Context, log logr.Logger, k6 *v1alpha1.TestRun, c client.Client) (
	inspectOutput cloud.InspectOutput, ready bool, returnErr error) {
	var (
		listOpts = &client.ListOptions{
			Namespace: k6.NamespacedName().Namespace,
			LabelSelector: labels.SelectorFromSet(map[string]string{
				"app":      "k6",
				"k6_cr":    k6.NamespacedName().Name,
				"job-name": fmt.Sprintf("%s-initializer", k6.NamespacedName().Name),
			}),
		}
		podList = &corev1.PodList{}
		err     error
	)
	if err = c.List(ctx, podList, listOpts); err != nil {
		returnErr = err
		log.Error(err, "Could not list pods")
		return
	}

	if len(podList.Items) < 1 {
		log.Info("No initializing pod found yet")
		return
	}

	// there should be only 1 initializer pod
	if podList.Items[0].Status.Phase == corev1.PodFailed {
		returnErr = errors.New("initalizer job has failed")
		log.Error(returnErr, "error:")
		return
	}
	if podList.Items[0].Status.Phase != corev1.PodSucceeded {
		log.Info("Waiting for initializing pod to finish")
		return
	}

	// Here we need to get the output of the pod
	// pods/log is not currently supported by controller-runtime client and it is officially
	// recommended to use REST client instead:
	// https://github.com/kubernetes-sigs/controller-runtime/issues/1229

	// TODO: if the below errors repeat several times, it'd be a real error case scenario.
	// How likely is it? Should we track frequency of these errors here?
	config, err := rest.InClusterConfig()
	if err != nil {
		log.Error(err, "unable to fetch in-cluster REST config")
		returnErr = err
		return
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Error(err, "unable to get access to clientset")
		returnErr = err
		return
	}
	req := clientset.CoreV1().Pods(k6.NamespacedName().Namespace).GetLogs(podList.Items[0].Name, &corev1.PodLogOptions{
		Container: "k6",
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()

	podLogs, err := req.Stream(ctx)
	if err != nil {
		log.Error(err, "unable to stream logs from the pod")
		returnErr = err
		return
	}
	defer podLogs.Close() //nolint:errcheck

	buf := new(bytes.Buffer)
	_, err = io.Copy(buf, podLogs)
	if err != nil {
		log.Error(err, "unable to copy logs from the pod")
		returnErr = err
		return
	}

	if returnErr = json.Unmarshal(buf.Bytes(), &inspectOutput); returnErr != nil {
		// this shouldn't normally happen but if it does, let's log output by default
		log.Error(returnErr, fmt.Sprintf("unable to marshal: `%s`", buf.String()))
	}

	ready = true
	return
}

func getEnvVar(vars []corev1.EnvVar, name string) string {
	for _, v := range vars {
		if v.Name == name {
			return v.Value
		}
	}
	return ""
}

func (r *TestRunReconciler) hostnames(ctx context.Context, log logr.Logger, abortOnUnready bool, opts *client.ListOptions) ([]string, error) {
	var (
		hostnames []string
		err       error
	)

	sl := &corev1.ServiceList{}

	if err = r.List(ctx, sl, opts); err != nil {
		log.Error(err, "Could not list services")
		return nil, err
	}

	for _, service := range sl.Items {
		log.Info(fmt.Sprintf("Checking service %s", service.Name))
		if isServiceReady(log, &service) {
			log.Info(fmt.Sprintf("%v service is ready", service.Name))
			hostnames = append(hostnames, service.Spec.ClusterIP)
		} else {
			err = fmt.Errorf("%v service is not ready", service.Name)
			log.Info(err.Error())
			if abortOnUnready {
				return nil, err
			}
		}
	}

	return hostnames, nil
}

type setupDataClient interface {
	RunSetup(context.Context, string) (json.RawMessage, error)
	GetSetupData(context.Context, string) (json.RawMessage, error)
	SetSetupData(context.Context, []string, json.RawMessage) error
}

type k6SetupDataClient struct{}

func (k6SetupDataClient) RunSetup(ctx context.Context, hostname string) (json.RawMessage, error) {
	return testrun.RunSetup(ctx, hostname)
}

func (k6SetupDataClient) GetSetupData(ctx context.Context, hostname string) (json.RawMessage, error) {
	return testrun.GetSetupData(ctx, hostname)
}

func (k6SetupDataClient) SetSetupData(
	ctx context.Context,
	hostnames []string,
	data json.RawMessage,
) error {
	return testrun.SetSetupData(ctx, hostnames, data)
}

const setupMarkerField = "__k6_operator_setup_claim"

type setupRunOutcome int

const (
	setupRunSucceeded setupRunOutcome = iota
	setupRunRetryable
	setupRunFailed
	setupRunPending
)

func setupMarkerData(claimID string) json.RawMessage {
	data, err := json.Marshal(map[string]string{setupMarkerField: claimID})
	if err != nil {
		panic(err)
	}
	return data
}

func isSetupMarkerData(data json.RawMessage, claimID string) bool {
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(data, &payload); err != nil || len(payload) != 1 {
		return false
	}

	rawClaimID, ok := payload[setupMarkerField]
	if !ok {
		return false
	}

	var storedClaimID string
	return json.Unmarshal(rawClaimID, &storedClaimID) == nil && storedClaimID == claimID
}

func prepareSetupRunner(
	ctx context.Context,
	hostnames []string,
	claimID string,
	setupClient setupDataClient,
) error {
	if len(hostnames) == 0 {
		return errors.New("no k6 Service is available to prepare setup")
	}

	marker := setupMarkerData(claimID)
	if err := setupClient.SetSetupData(ctx, hostnames[:1], marker); err != nil {
		return fmt.Errorf("storing setup execution marker: %w", err)
	}

	stored, err := setupClient.GetSetupData(ctx, hostnames[0])
	if err != nil {
		return fmt.Errorf("verifying setup execution marker: %w", err)
	}
	if !isSetupMarkerData(stored, claimID) {
		return errors.New("setup execution marker was not retained by the first runner")
	}
	return nil
}

func recoverSetupData(
	ctx context.Context,
	hostnames []string,
	claimID string,
	setupClient setupDataClient,
) (bool, error) {
	if len(hostnames) == 0 {
		return false, errors.New("no k6 Service is available to recover setup")
	}

	setupData, err := setupClient.GetSetupData(ctx, hostnames[0])
	if err != nil {
		return false, fmt.Errorf("reading setup recovery data: %w", err)
	}
	if isSetupMarkerData(setupData, claimID) {
		return false, nil
	}

	if err := setupClient.SetSetupData(ctx, hostnames, setupData); err != nil {
		return false, fmt.Errorf("redistributing recovered setup data: %w", err)
	}
	return true, nil
}

// runSetup executes a setup that has already been fenced by a persisted
// Prepared condition and a claim-specific marker on the first runner.
func runSetup(
	ctx context.Context,
	hostnames []string,
	claimID string,
	setupClient setupDataClient,
	log logr.Logger,
) (setupRunOutcome, error) {
	log.Info("Invoking setup() on the first runner")

	setupData, err := setupClient.RunSetup(ctx, hostnames[0])
	if err != nil {
		// A lost HTTP response can happen after setup completed. The first
		// runner's marker is overwritten only by a successful setup, so probe
		// it before deciding whether the operation is safe to retry.
		recovered, recoveryErr := recoverSetupData(ctx, hostnames, claimID, setupClient)
		if recoveryErr != nil {
			return setupRunPending, errors.Join(err, recoveryErr)
		}
		if recovered {
			return setupRunSucceeded, nil
		}

		if strings.Contains(err.Error(), "Error executing") {
			return setupRunFailed, err
		}
		return setupRunRetryable, err
	}

	// POST leaves the marker unchanged when the script has no setup export.
	// Clearing it preserves the original undefined setup-data semantics.
	if isSetupMarkerData(setupData, claimID) {
		setupData = nil
	}

	log.Info("Sending setup data to the runners")
	if err := setupClient.SetSetupData(ctx, hostnames, setupData); err != nil {
		// The first runner remains the durable source for a later reconcile.
		return setupRunPending, err
	}

	return setupRunSucceeded, nil
}

func runTeardown(ctx context.Context, hostnames []string, log logr.Logger) {
	log.Info("Invoking teardown() on the first responsive runner")

	if err := testrun.RunTeardown(ctx, hostnames); err != nil {
		log.Error(err, "Failed to invoke teardown()")
	}
}
