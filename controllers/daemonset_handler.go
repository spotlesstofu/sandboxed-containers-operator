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

const (
	peerPodsConfigInstallDsName         = "osc-config-sync"

	kataInstallDsName       = "osc-rpm-install"
	kataDsInstallationLabel = "kataconfiguration.openshift.io/kata-ds-rpm-install"
)

type KataDsInstallationState string

// TODO: Do we need to add Failed and WaitToInstall states?
const (
	KataDsInstalled        KataDsInstallationState = "installed"
	KataDsInstalling       KataDsInstallationState = "installing"
	KataDsWaitingForReboot KataDsInstallationState = "waiting_for_reboot"
	KataDsUninstalling     KataDsInstallationState = "uninstalling"
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

	// TODO: Does labeling changed needed?
	_, err := r.updateNodeLabels()
	if err != nil {
		if k8serrors.IsConflict(err) {
			return ctrl.Result{Requeue: true, RequeueAfter: 10 * time.Second}, nil
		} else {
			return ctrl.Result{Requeue: true}, nil
		}
	}

	if r.kataConfig.Spec.EnablePeerPods {
		err := r.addPeerPodsConfigDaemonSet()
		if err != nil {
			r.Log.Info("Adding peerpods configs failed", "err", err)
			return ctrl.Result{Requeue: true, RequeueAfter: 15 * time.Second}, err
		}
	}

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
	extensionImageString, err := r.GetExtensionImage()
	if err != nil {
		r.Log.Error(err, "couldn't get extension image")
		return ctrl.Result{}, err
	}

	cliImageString, err := r.GetCliImage()
	if err != nil {
		r.Log.Error(err, "couldn't get cli image")
	}

	kataInstallDs := r.DaemonSetForKataInstall(extensionImageString, cliImageString)
	if err := controllerutil.SetControllerReference(r.kataConfig, kataInstallDs, r.Scheme); err != nil {
		r.Log.Error(err, "Failed setting ControllerReference for kata installation DS")
		return ctrl.Result{}, err
	}

	foundKataDs := &appsv1.DaemonSet{}
	err = r.Client.Get(context.TODO(), types.NamespacedName{Name: kataInstallDs.Name, Namespace: kataInstallDs.Namespace}, foundKataDs)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			r.Log.Info("Creating a new kata installation daemonset", "kataInstallDs.Namespace", kataInstallDs.Namespace, "kataInstallDs.Name", kataInstallDs.Name)
			err = r.Client.Create(context.TODO(), kataInstallDs)
			if err != nil {
				r.Log.Error(err, "error when creating kata installation daemonset")
				return ctrl.Result{Requeue: true, RequeueAfter: 15 * time.Second}, err
			}
		} else {
			r.Log.Error(err, "could not get kata installation daemonset, try again")
			return ctrl.Result{Requeue: true, RequeueAfter: 15 * time.Second}, err
		}
	} else {
		r.Log.Info("Updating kata installation daemonset", "kataInstallDs.Namespace", kataInstallDs.Namespace, "kataInstallDs.Name", kataInstallDs.Name)
	err = r.Client.Update(context.TODO(), kataInstallDs)
		if err != nil {
			r.Log.Error(err, "error when updating kata installation daemonset")
			return ctrl.Result{Requeue: true, RequeueAfter: 15 * time.Second}, err
		}
	}
	// create Pod VM image CRD and runtimeclass for peerpods
	// in case of an error wait a little bit and reconcile
	if r.kataConfig.Spec.EnablePeerPods {
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

func (r *KataConfigOpenShiftReconciler) DaemonSetForPeerPodsConfig(imageString string) *appsv1.DaemonSet {
	var (
		runPrivileged                       = true
		runAsUser                     int64 = 0
		nodeSelector                        = r.getNodeSelectorAsMap()
		hostPathTypeDirectory               = corev1.HostPathDirectory
		hostPathTypeDirectoryOrCreate       = corev1.HostPathDirectoryOrCreate
	)

	// TODO: configuration-remote.toml is set to 420 in MC
	// Add another paramter that defines the permissions

	script := `
set -xeuo pipefail

# Function to sync a file from configmap to host
sync_file() {
local src_file="$1"
local dest_file="$2"

if [ -f "$src_file" ]; then
	cp "$src_file" "$dest_file"
	chmod 644 "$dest_file"
	echo "Synced $(basename "$src_file") to $dest_file"
else
	echo "Warning: $(basename "$src_file") not found in configmap"
fi
}

echo "Starting configuration sync..."

# Sync configuration files
sync_file "/osc-configs/50-kata-remote" "/host/etc/crio/crio.conf.d/50-kata-remote"
sync_file "/osc-configs/configuration-remote.toml" "/host/opt/kata/configuration-remote.toml"

echo "Configuration sync completed at $(date)"

echo "Sending signal to reload  crio config"
pidof crio
kill -1 $(pidof crio)
sleep infinity
	`

	dsLabelSelectors := map[string]string{
		"name": peerPodsConfigInstallDsName,
	}

	return &appsv1.DaemonSet{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "apps/v1",
			Kind:       "DaemonSet",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      peerPodsConfigInstallDsName,
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
							Args:    []string{script},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "host-etc-crio",
									MountPath: "/host/etc/crio",
								},
								{
									Name:      "host-opt-kata",
									MountPath: "/host/opt/kata",
								},
								{
									Name:      "osc-configs",
									MountPath: "/osc-configs/50-kata-remote",
									SubPath:   "50-kata-remote",
									ReadOnly:  true,
								},
								{
									Name:      "osc-configs",
									MountPath: "/osc-configs/configuration-remote.toml",
									SubPath:   "configuration-remote.toml",
									ReadOnly:  true,
								},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "host-etc-crio",
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{
									Path: "/etc/crio",
									Type: &hostPathTypeDirectory,
								},
							},
						},
						{
							Name: "host-opt-kata",
							VolumeSource: corev1.VolumeSource{
								HostPath: &corev1.HostPathVolumeSource{
									Path: "/opt/kata",
									Type: &hostPathTypeDirectoryOrCreate,
								},
							},
						},
						{
							Name: "osc-configs",
							VolumeSource: corev1.VolumeSource{
								ConfigMap: &corev1.ConfigMapVolumeSource{
									LocalObjectReference: corev1.LocalObjectReference{
										Name: "osc-configs",
									},
								},
							},
						},
					},
				},
			},
		},
	}
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

func (r *KataConfigOpenShiftReconciler) GetCliImage() (string, error) {
	imageString, err := r.GetImageForComponent("cli")
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

func (r *KataConfigOpenShiftReconciler) DaemonSetForKataInstall(extensionImageString string, cliImageString string) *appsv1.DaemonSet {
	var (
		runPrivileged       = true
		runAsUser     int64 = 0
		nodeSelector        = r.getNodeSelectorAsMap()

		// TODO: Use Envars for path and code dependant variables in the script (like the labels' name)
		// TODO: Extract into a configmap and mount it
		installScript = `
set -xeuo pipefail

# Function to check if a reboot is required by looking for "Staged: yes"
is_reboot_required() {
  chroot /host /bin/bash -c '
    rpm-ostree status -v | grep -q "Staged: yes"
  '
}

# Loop until reboot is no longer required
wait_for_reboot_clear() {
  while is_reboot_required; do
    echo "Reboot required"
	set_status_waiting_for_reboot
    sleep 60
  done
}

clear_status() {
    rm -rf /tmp/shared/* || true
}

set_status() {
    local status_name="$1"
    touch /tmp/shared/$1
}

set_status_installed() {
    clear_status
    set_status "installed"
}

set_status_installing() {
    clear_status
    set_status "installing"
}

set_status_waiting_for_reboot() {
    clear_status
    set_status "waiting_for_reboot"
}

# Initial wait: avoid doing anything if a previous staged update is pending
wait_for_reboot_clear

# Compare installed and available versions of kata-containers
# If installation is complete and the installed version matches the available version we are done
# Create finished file to signal readiness
# Sleep infinity to prevent pod restart (DaemonSets always restart exited pods)
available_version=$(rpm -qp /usr/share/rpm-ostree/extensions/kata-containers-*.rpm)
if installed_version=$(chroot /host rpm -q kata-containers 2>/dev/null); then
  if [[ "$installed_version" == "$available_version" ]]; then
    echo "Package already installed and up-to-date: $installed_version"
    set_status_installed
    sleep infinity
  fi
fi

# Set installation status to installing
set_status_installing

# Prepare to install packages
packages="capstone daxctl-libs edk2-ovmf ipxe-roms-qemu kata-containers libfdt libpmem libpng librdmacm ndctl-libs pixman qemu-img qemu-kvm-common qemu-kvm-core seabios-bin seavgabios-bin virtiofsd"
mkdir -p /host/tmp/extensions/

for package in $packages; do
  cp /usr/share/rpm-ostree/extensions/${package}-* /host/tmp/extensions/
done

# Install extensions on the node
chroot /host /bin/bash -c "rpm-ostree install /tmp/extensions/*"

# Clean up temp dir
rm -rf /host/tmp/extensions/

# Wait again: rpm-ostree install stages changes, requiring a reboot
wait_for_reboot_clear
`

		statusCheckScript = `
set -xeuo pipefail

STATUS_DIR=/tmp/shared
CURRENT_STATE=""

echo "Watching directory $STATUS_DIR for status file changes..."

while true; do
	for state in installing installed waiting_for_reboot; do
		if [ -f "$STATUS_DIR/$state" ] && [ "$CURRENT_STATE" != "$state" ]; then
		CURRENT_STATE="$state"
		echo "Detected status: $state"
		kubectl label node "$NODE_NAME" "kataconfiguration.openshift.io/kata-ds-rpm-install=$state" --overwrite
		fi
	done
	sleep 5
done
`
	)

	dsLabelSelectors := map[string]string{
		"name": kataInstallDsName,
	}

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
							Image:           extensionImageString,
							ImagePullPolicy: corev1.PullIfNotPresent,
							SecurityContext: &corev1.SecurityContext{
								// TODO: do we really need to run as root?
								Privileged: &runPrivileged,
								RunAsUser:  &runAsUser,
							},
							Command: []string{"/bin/bash", "-c"},
							Args:    []string{installScript},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "host-root",
									MountPath: "/host",
								},
								{
									Name:      "host-tmp",
									MountPath: "/host/tmp",
								},
								{
									Name:      "shared",
									MountPath: "/tmp/shared",
								},
							},
						},
						{
							Name:            "rpm-install-status",
							Image:           cliImageString,
							ImagePullPolicy: corev1.PullIfNotPresent,
							Command:         []string{"/bin/bash", "-c"},
							Args:            []string{statusCheckScript},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "shared",
									MountPath: "/tmp/shared",
								},
							},
							Env: []corev1.EnvVar{
								{
									Name: "NODE_NAME",
									ValueFrom: &corev1.EnvVarSource{
										FieldRef: &corev1.ObjectFieldSelector{
											FieldPath: "spec.nodeName",
										},
									},
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
						{
							Name: "shared",
							VolumeSource: corev1.VolumeSource{
								EmptyDir: &corev1.EmptyDirVolumeSource{
									Medium: corev1.StorageMediumDefault,
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

func (r *KataConfigOpenShiftReconciler) isKataInstallDaemonSetInstalling() bool {
	nodes, err := r.getNodesWithLabels(r.getNodeSelectorAsMap())
	if err != nil {
		r.Log.Error(err, "Getting Node List failed")
		return false
	}

	for _, node := range nodes.Items {
		state, ok := node.Labels[kataDsInstallationLabel]
		if ok && state == string(KataDsInstalled) || state == string(KataDsWaitingForReboot) {
			return true
		}
	}
	return false
}

// TODO: Do we need to retrive all nodes as in the updateStatus function?
func (r *KataConfigOpenShiftReconciler) updateStatusDaemonSet() error {

	nodes, err := r.getNodesWithLabels(r.getNodeSelectorAsMap())
	if err != nil {
		return err
	}

	r.clearNodeStatusLists()

	r.kataConfig.Status.KataNodes.NodeCount = len(nodes.Items)

	for _, node := range nodes.Items {
		e := r.putNodeOnStatusListDaemonSet(&node)
		if e != nil {
			err = e
		}
	}

	r.kataConfig.Status.KataNodes.ReadyNodeCount = len(r.kataConfig.Status.KataNodes.Installed)

	return err
}

func (r *KataConfigOpenShiftReconciler) putNodeOnStatusListDaemonSet(node *corev1.Node) error {
	// TODO: Add FailedToInstall nodes
	kataInstallationState := node.Labels[kataDsInstallationLabel]

	switch kataInstallationState {
	case string(KataDsInstalling):
		r.kataConfig.Status.KataNodes.Installing = append(r.kataConfig.Status.KataNodes.Installing, node.GetName())
	case string(KataDsWaitingForReboot):
		r.kataConfig.Status.KataNodes.Installing = append(r.kataConfig.Status.KataNodes.Installing, node.GetName())
	case string(KataDsInstalled):
		r.kataConfig.Status.KataNodes.Installed = append(r.kataConfig.Status.KataNodes.Installed, node.GetName())
	default:
		r.kataConfig.Status.KataNodes.WaitingToInstall = append(r.kataConfig.Status.KataNodes.WaitingToInstall, node.GetName())
	}

	return nil
}
