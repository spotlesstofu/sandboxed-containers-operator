package controllers

import (
	"context"
	"fmt"
	"os"
	"time"

	configv1 "github.com/openshift/api/config/v1"
	"github.com/openshift/oc/pkg/cli/admin/release"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// Create enum to represent the state of the deployment
type DeploymentMode int

const (
	MachineConfig DeploymentMode = iota
	DaemonSet
)

const (
	machineConfigCrdName = "machineconfigs.machineconfiguration.openshift.io"
	machineConfigGroup   = "machineconfiguration.openshift.io"
	machineConfigVersion = "v1"
	machineConfigKind    = "MachineConfig"
)

// Process the DaemonSet feature gate (FG).
// This method is invoked by the reconcile loop at its initiation.
// It examines the current state of the FeatureGate and adjusts the deployment mode (DaemonSet or MachineConfig)
// based on the availability of the MachineConfig Add-on and the FeatureGateState.
// If the MachineConfig Add-on is unavailable and the FeatureGateState is Enabled,
// the deployment mode is set to DaemonSet. Otherwise, it defaults to MachineConfig.
// If the FeatureGateState is Disabled, the deployment mode remains MachineConfig,
// regardless of the MachineConfig Add-on's availability.
func (r *KataConfigOpenShiftReconciler) handleDaemonSetFeature(state FeatureGateState) error {
	mcAvailable, err := r.isMCAvailable()
	if err != nil {
		r.Log.Info("Error checking if MC is available")
		return err
	}

	if state == Enabled && !mcAvailable {
		r.Log.Info("MC is not available, deployment mode will be set to DaemonSet")
		r.DeploymentMode = DaemonSet
	} else {
		r.Log.Info("Deployment mode will be set to MachineConfig")
		r.DeploymentMode = MachineConfig
	}

	return nil
}

// isMCAvailable checks if MachineConfig CRD is available.
//
// It returns a boolean indicating availability and an error if any.
func (r *KataConfigOpenShiftReconciler) isMCAvailable() (bool, error) {
	mc := &unstructured.Unstructured{}
	mc.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   machineConfigGroup,
		Version: machineConfigVersion,
		Kind:    machineConfigKind,
	})

	// Attempt to GET any MachineConfig to verify if the GVK is known.
	// If the CRD isn’t installed, it will return a NoMatchError.
	err := r.Client.Get(context.Background(), client.ObjectKey{Name: machineConfigCrdName}, mc)
	if err != nil {
		if meta.IsNoMatchError(err) {
			r.Log.Info("MachineConfig CRD not found")
			return false, nil
		}
		// CRD is installed but no resource exists under that name
		if k8serrors.IsNotFound(err) {
			r.Log.Info("MachineConfig CRD is present")
			return true, nil
		}

		return false, err
	}

	return true, nil
}

// This function attempts to replicate the behavior of the original reconcile function,
// with modifications only in the MachineConfig-related logic.
// It currently contains a lot of duplicated code copied from the original function.
// The goal is to deliver an MVP first; we can iterate later to simplify and reduce duplication
func (r *KataConfigOpenShiftReconciler) daemonSetDeployment() (ctrl.Result, error) {
	r.Log.Info("Reconcile DaemonSet")

	// Same as original reconcile.
	// The function uses MachineConfigPoolList to check if it runs on a converged cluster but we can ignore that for now.
	// k8s resource correctness checking on creation/modification
	// isn't fully reliable for matchExpressions.  Specifically,
	// it doesn't catch an invalid value of matchExpressions.operator.
	// With this work-around we check early if our kata node selector
	// is workable and bail out before making any changes to the
	// cluster if it turns out it isn't.
	_, err := r.getKataConfigNodeSelectorAsSelector()
	if err != nil {
		r.Log.Info("Invalid KataConfig.spec.kataConfigPoolSelector - please fix your KataConfig", "err", err)
		return ctrl.Result{}, nil
	}

	// Same as original reconcile
	// Check if the KataConfig instance is marked to be deleted, which is
	// indicated by the deletion timestamp being set.  However, don't let
	// uninstallation commence if another operation (installation, update)
	// is underway.
	if r.kataConfig.GetDeletionTimestamp() != nil && !r.isInstalling() && !r.isUpdating() {
		res, err := r.processKataConfigDeleteRequestDaemonSet()

		updateErr := r.Client.Status().Update(context.TODO(), r.kataConfig)
		// The finalizer test is to get rid of the
		// "Operation cannot be fulfilled [...] Precondition failed"
		// error which happens when returning from a reconciliation that
		// deleted our KataConfig by removing its finalizer.  So if the
		// finalizer is missing the actual KataConfig object is probably
		// already gone from the cluster, hence the error.
		if updateErr != nil && controllerutil.ContainsFinalizer(r.kataConfig, kataConfigFinalizer) {
			r.Log.Info("Updating KataConfig failed", "err", updateErr)
			return ctrl.Result{}, updateErr
		}
		return res, err
	}

	res, err := r.processDaemonSetKataConfigInstallRequest()
	if err != nil {
		return res, err
	}
	// Same as original reconcile
	updateErr := r.Client.Status().Update(context.TODO(), r.kataConfig)
	if updateErr != nil {
		return ctrl.Result{}, updateErr
	}

	// Same as original reconcile
	cMap := r.processDashboardConfigMap()
	if cMap == nil {
		r.Log.Info("failed to generate config map for metrics dashboard")
		return ctrl.Result{Requeue: true, RequeueAfter: 15 * time.Second}, nil
	}
	foundCm := &corev1.ConfigMap{}
	err = r.Client.Get(context.TODO(), types.NamespacedName{Name: cMap.Name, Namespace: cMap.Namespace}, foundCm)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			r.Log.Info("Installing metrics dashboard")
			err = r.Client.Create(context.TODO(), cMap)
			if err != nil {
				r.Log.Error(err, "Error when creating the dashboard configmap")
				res = ctrl.Result{Requeue: true, RequeueAfter: 15 * time.Second}
			}
		} else {
			r.Log.Error(err, "could not get dashboard info, try again")
			res = ctrl.Result{Requeue: true, RequeueAfter: 15 * time.Second}
		}
	}

	// Probably could be part of the other DaemonSets
	err = r.processLogLevelDaemonSet(r.kataConfig.Spec.LogLevel)
	if err != nil {
		res = ctrl.Result{Requeue: true, RequeueAfter: 15 * time.Second}
	}

	return res, err
}

func (r *KataConfigOpenShiftReconciler) processKataConfigDeleteRequestDaemonSet() (ctrl.Result, error) {
	// TODO: Remove packages, configs from nodes, remove labels, remove finalizer
	return ctrl.Result{}, nil
}

func (r *KataConfigOpenShiftReconciler) processLogLevelDaemonSet(level string) error {
	// TODO: Check level if it's not default to info,
	// put the runtime config on the worker nodes
	return nil
}

func (r *KataConfigOpenShiftReconciler) processDaemonSetKataConfigInstallRequest() (ctrl.Result, error) {
	r.Log.Info("Kata installation in progress")

	// Check Node Eligibility
	/*if r.kataConfig.Spec.CheckNodeEligibility {
		err := r.checkNodeEligibility()
		if err != nil {
			// If no nodes are found, requeue to check again for eligible nodes
			r.Log.Error(err, "Failed to check Node eligibility for running Kata containers")
			return ctrl.Result{Requeue: true, RequeueAfter: time.Second * 20}, err
		}
	}*/

	// Add finalizer for this CR
	/*if !contains(r.kataConfig.GetFinalizers(), kataConfigFinalizer) {
		if err := r.addFinalizer(); err != nil {
			return ctrl.Result{}, err
		}
	}*/

	// TODO:
	// - Retrieve the image reference and extension
	// - Run the DaemonSet and monitor its status until completion
	// - Add at least two labels:
	//     1. One to replace the MCP (MachineConfigPool), to identify which nodes should run the DaemonSet
	//     2. One to track the installation status
	// - Do we need any other label?
	// - Set the "InProgress" condition based on the installation status
	// - Wait for the installation to complete
	imageString, err := r.GetExtensionImage()
	if err != nil {
		r.Log.Info("couldn't get image", "err", err)
		return ctrl.Result{}, err
	}

	r.Log.Info("got image name", "imageName", imageString)

	// TODO: Check if Ds exists
	kataInstallDs := r.DaemonSetForKataInstall(imageString)
	if err := controllerutil.SetControllerReference(r.kataConfig, kataInstallDs, r.Scheme); err != nil {
		r.Log.Error(err, "Failed setting ControllerReference for cloud-api-adaptor DS")
		return ctrl.Result{}, err
	}
	err = r.Client.Update(context.TODO(), kataInstallDs)
	if err != nil && k8serrors.IsNotFound(err) {
		r.Log.Error(err, "cloud-api-adaptor daemonset doesn't exist. Creating")
		err = r.Client.Create(context.TODO(), kataInstallDs)
		if err != nil {
			r.Log.Error(err, "failed to create cloud-api-adaptor daemonset")
			return ctrl.Result{}, err
		}
	}

	// create confing, Pod VM image CRD and runtimeclass for peerpods
	// in case of an error wait a little bit and reconcile
	// TODO: Should we install CAA first and add the config after?
	if r.kataConfig.Spec.EnablePeerPods {
		err := r.addPeerPodsConfigDaemonSet()
		if err != nil {
			r.Log.Info("Adding peerpods configs failed", "err", err)
			return ctrl.Result{Requeue: true, RequeueAfter: 15 * time.Second}, err
		}

		err = r.enablePeerPods()
		if err != nil {
			r.Log.Info("Enabling peerpods failed", "err", err)
			return ctrl.Result{Requeue: true, RequeueAfter: 15 * time.Second}, err
		}
	}

	return ctrl.Result{}, nil
}

func (r *KataConfigOpenShiftReconciler) addPeerPodsConfigDaemonSet() error {
	// TODO: Add kata-remote CRIO config and config toml to hosts
	return nil
}

func (r *KataConfigOpenShiftReconciler) GetExtensionImage() (string, error) {
	//TODO: Don't use hard coded component name. Other OSes?
	imageString, err := r.GetImageForComponent("rhel-coreos-extensions")
	if err != nil {
		return "", err
	}
	if imageString == "" {
		return "", fmt.Errorf("empty result for image name")
	}

	return imageString, nil
}

func (r *KataConfigOpenShiftReconciler) GetImageForComponent(componentName string) (string, error) {
	clusterVersion := &configv1.ClusterVersion{}
	err := r.Client.Get(context.TODO(), types.NamespacedName{Name: "version"}, clusterVersion)
	if err != nil {
		return "", fmt.Errorf("failed to get cluster version: %w", err)
	}

	releaseImage := clusterVersion.Status.Desired.Image
	if releaseImage == "" {
		return "", fmt.Errorf("no release image found in cluster version")
	}

	// Create IOStreams
	streams := genericiooptions.IOStreams{
		Out:    os.Stdout,
		ErrOut: os.Stderr,
	}

	// Create InfoOptions
	infoOptions := release.NewInfoOptions(streams)

	// Load release info (set retrieveImages to false for faster loading)
	releaseInfo, err := infoOptions.LoadReleaseInfo(releaseImage, false)
	if err != nil {
		fmt.Printf("Error loading release info: %v\n", err)
		return "", err
	}

	// Search for the componentName and return
	for _, tag := range releaseInfo.References.Spec.Tags {
		if tag.Name == componentName {
			// we found the short name in ImageStream
			if tag.From != nil && tag.From.Kind == "DockerImage" {
				return tag.From.Name, nil
			}
		}
	}

	// Didn't find it
	return "", nil
}

func (r *KataConfigOpenShiftReconciler) DaemonSetForKataInstall(imageString string) *appsv1.DaemonSet {
	var (
		runPrivileged           = true
		runAsUser         int64 = 0
		_                       = r.getNodeSelectorAsMap() // TODO: Use kata-oc or another label?
		kataInstallDsName       = "osc-rpm-install"

		script = `
sleep infinity
set -xeuo pipefail
available_version=$(rpm -qp /usr/share/rpm-ostree/extensions/kata-containers-*.rpm)
installed_version=$(chroot /host rpm -q kata-containers) && \
  [ "$installed_version" = "$available_version" ] && exit
packages="capstone daxctl-libs edk2-ovmf ipxe-roms-qemu kata-containers libfdt libpmem libpng librdmacm ndctl-libs pixman qemu-img qemu-kvm-common qemu-kvm-core seabios-bin seavgabios-bin virtiofsd"
mkdir -p /host/tmp/extensions/
for package in $packages; do cp /usr/share/rpm-ostree/extensions/${package}-* /host/tmp/extensions/; done
chroot /host rpm-ostree install /tmp/extensions/${available_version}.rpm
rm -rf /host/tmp/extensions/
sleep infinity
`
	)

	dsLabelSelectors := map[string]string{
		"name": kataInstallDsName,
	}

	// TODO: Label nodes that need to run the DaemonSet and use that
	var nodeSelector map[string]string
	if r.kataConfig.Spec.KataConfigPoolSelector != nil {
		nodeSelector = r.kataConfig.Spec.KataConfigPoolSelector.MatchLabels
	} else {
		nodeSelector = map[string]string{
			"node-role.kubernetes.io/worker": "",
		}
	}

	// TODO: Add second container that change nodes' labels based on the installation status
	return &appsv1.DaemonSet{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "apps/v1",
			Kind:       "DaemonSet",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      kataInstallDsName,
			Namespace: OperatorNamespace,
		},
		Spec: appsv1.DaemonSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: dsLabelSelectors,
			},
			UpdateStrategy: appsv1.DaemonSetUpdateStrategy{
				Type: "RollingUpdate",
				RollingUpdate: &appsv1.RollingUpdateDaemonSet{
					MaxUnavailable: &intstr.IntOrString{
						Type:   intstr.Int,
						IntVal: 1,
					},
				},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: dsLabelSelectors,
				},
				Spec: corev1.PodSpec{
					ServiceAccountName: "default", // TODO: Which service account should be used?
					NodeSelector:       nodeSelector,
					HostPID:            true,
					Containers: []corev1.Container{
						{
							Name:            "rpm-install",
							Image:           imageString,
							ImagePullPolicy: corev1.PullIfNotPresent,
							SecurityContext: &corev1.SecurityContext{
								// TODO: do we really need to run as root?
								Privileged: &runPrivileged,
								RunAsUser:  &runAsUser,
							},
							Command: []string{"/bin/bash", "-c"},
							Args: []string{script},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "host-root",
									MountPath: "/host",
								},
								{
									Name:      "host-tmp",
									MountPath: "/host/tmp",
								},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "host-root",
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{
									Path: "/",
								},
							},
						},
						{
							Name: "host-tmp",
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{
									Path: "/tmp",
								},
							},
						},
					},
				},
			},
		},
	}
}

// Copied from processKataConfigInstallRequest
func (r *KataConfigOpenShiftReconciler) enablePeerPods() error {
	//Get pull-secret from openshift-config ns and save it as auth-json-secret in our ns
	//This will be used by the podvm image provider to pull the pause image for embedding
	err := r.createAuthJsonSecret()
	if err != nil {
		r.Log.Info("Error in creating auth-json-secret", "err", err)
		return err
	}

	// Create the podvm image
	// Since we want to declaratively reach the final state, we need to reconcile when there are errors
	// as we want the system to give a chance of fixing the error.
	// For cases we don't want to reconcile, ie for ImageCreatedSuccessfully and UnsupportedPodVMImageProvider
	// we should just log the message and let the code continue without explicitly returning from the method

	// Following are the returned statuses:
	// ImageCreatedSuccessfully
	// UnsupportedPodVMImageProvider
	// ImageCreationFailed
	// RequeueNeeded
	// ImageCreationStatusUnknown

	status, err := ImageCreate(r.Client)
	switch status {
	case ImageCreatedSuccessfully:
		r.setInProgressConditionToPodVMImageCreated()
		r.Log.Info("PodVM Image created successfully")

	case UnsupportedPodVMImageProvider:
		r.setInProgressConditionToPodVMImageUnsupportedProvider()
		r.Log.Info("unsupported cloud provider, skipping image creation")

	case RequeueNeeded:
		r.setInProgressConditionToPodVMImageCreating()
		return err

	case ImageCreationFailed:
		r.setInProgressConditionToPodVMImageCreationFailed()
		if err != nil {
			return err
		}
		// If there's no error, log and continue
		r.Log.Info("Image creation failed. Check logs for more details")

	case ImageCreationStatusUnknown:
		r.setInProgressConditionToPodVMImageCreationUnknown()
		return err
	default:
		// For all other statuses, just log and continue
		r.Log.Info("PodVM Image creation status and error", "status", status, "error", err)
	}

	err = r.enablePeerPodsMiscConfigs()
	if err != nil {
		r.Log.Info("Enabling peerpodconfig CR, runtimeclass etc", "err", err)
		return err
	}

	// Reset the in progress condition
	r.resetInProgressCondition()

	return nil
}
