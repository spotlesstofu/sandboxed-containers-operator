package controllers

import (
	"context"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrl "sigs.k8s.io/controller-runtime"
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


func (r *KataConfigOpenShiftReconciler) daemonSetDeployment() (ctrl.Result, error) {
	r.Log.Info("Reconcile DaemonSet")
	return ctrl.Result{}, nil
}