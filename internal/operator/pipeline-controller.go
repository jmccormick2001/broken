package operator

import (
	"context"
	"github.com/go-logr/logr"
	v1apps "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/api/policy/v1beta1"
	rbacv1 "k8s.io/api/rbac/v1"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"gitlab.com/churro-group/churro/api/v1alpha1"
	churrov1alpha1 "gitlab.com/churro-group/churro/api/v1alpha1"
)

var (
	ownerKey = ".metadata.controller"
	apiGVStr = v1alpha1.GroupVersion.String()
)

// PipelineReconciler reconciles a Pipeline object
type PipelineReconciler struct {
	client.Client
	Log                        logr.Logger
	Ctx                        context.Context
	Scheme                     *runtime.Scheme
	PVCTemplate                []byte
	StatefulSetTemplate        []byte
	WatchPodTemplate           []byte
	CockroachClientPodTemplate []byte
	CtlPodTemplate             []byte
	LoaderPodTemplate          []byte
}

// +kubebuilder:rbac:groups=churro.project.io,resources=pipelines,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=churro.project.io,resources=pipelines/status,verbs=get;update;patch

func (r *PipelineReconciler) Reconcile(req ctrl.Request) (ctrl.Result, error) {
	r.Ctx = context.Background()
	r.Log = r.Log.WithValues("pipeline", req.NamespacedName)

	// your logic here
	result, err := r.ProcessCR(req)
	if err != nil {
		return ctrl.Result{}, err
	}

	return result, nil
}

func (r *PipelineReconciler) SetupWithManager(mgr ctrl.Manager) error {
	r.Log.Info("jeff SetupWithManager...")
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &rbacv1.Role{}, ownerKey, func(rawObj runtime.Object) []string {
		// grab the role object, extract the owner...
		role := rawObj.(*rbacv1.Role)
		owner := metav1.GetControllerOf(role)
		if owner == nil {
			return nil
		}
		// ...make sure it's a pipeline...
		if owner.APIVersion != apiGVStr || owner.Kind != "Pipeline" {
			return nil
		}

		// ...and if so, return it
		return []string{owner.Name}
	}); err != nil {
		return err
	}
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &rbacv1.RoleBinding{}, ownerKey, func(rawObj runtime.Object) []string {
		// grab the rolebinding object, extract the owner...
		rolebinding := rawObj.(*rbacv1.RoleBinding)
		owner := metav1.GetControllerOf(rolebinding)
		if owner == nil {
			return nil
		}
		// ...make sure it's a pipeline...
		if owner.APIVersion != apiGVStr || owner.Kind != "Pipeline" {
			return nil
		}

		// ...and if so, return it
		return []string{owner.Name}
	}); err != nil {
		return err
	}
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &v1.ServiceAccount{}, ownerKey, func(rawObj runtime.Object) []string {
		// grab the ServiceAccount object, extract the owner...
		sa := rawObj.(*v1.ServiceAccount)
		owner := metav1.GetControllerOf(sa)
		if owner == nil {
			return nil
		}
		// ...make sure it's a pipeline...
		if owner.APIVersion != apiGVStr || owner.Kind != "Pipeline" {
			return nil
		}

		// ...and if so, return it
		return []string{owner.Name}
	}); err != nil {
		return err
	}
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &v1.Service{}, ownerKey, func(rawObj runtime.Object) []string {
		// grab the Service object, extract the owner...
		svc := rawObj.(*v1.Service)
		owner := metav1.GetControllerOf(svc)
		if owner == nil {
			return nil
		}
		// ...make sure it's a pipeline...
		if owner.APIVersion != apiGVStr || owner.Kind != "Pipeline" {
			return nil
		}

		// ...and if so, return it
		return []string{owner.Name}
	}); err != nil {
		return err
	}
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &v1.PersistentVolumeClaim{}, ownerKey, func(rawObj runtime.Object) []string {
		// grab the PVC object, extract the owner...
		pvc := rawObj.(*v1.PersistentVolumeClaim)
		owner := metav1.GetControllerOf(pvc)
		if owner == nil {
			return nil
		}
		// ...make sure it's a pipeline...
		if owner.APIVersion != apiGVStr || owner.Kind != "Pipeline" {
			return nil
		}

		// ...and if so, return it
		return []string{owner.Name}
	}); err != nil {
		return err
	}
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &v1.Secret{}, ownerKey, func(rawObj runtime.Object) []string {
		// grab the Secret object, extract the owner...
		secret := rawObj.(*v1.Secret)
		owner := metav1.GetControllerOf(secret)
		if owner == nil {
			return nil
		}
		// ...make sure it's a pipeline...
		if owner.APIVersion != apiGVStr || owner.Kind != "Pipeline" {
			return nil
		}

		// ...and if so, return it
		return []string{owner.Name}
	}); err != nil {
		return err
	}
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &v1beta1.PodDisruptionBudget{}, ownerKey, func(rawObj runtime.Object) []string {
		// grab the PodDisruptionBudget object, extract the owner...
		pdb := rawObj.(*v1beta1.PodDisruptionBudget)
		owner := metav1.GetControllerOf(pdb)
		if owner == nil {
			return nil
		}
		// ...make sure it's a pipeline...
		if owner.APIVersion != apiGVStr || owner.Kind != "Pipeline" {
			return nil
		}

		// ...and if so, return it
		return []string{owner.Name}
	}); err != nil {
		return err
	}
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &v1apps.StatefulSet{}, ownerKey, func(rawObj runtime.Object) []string {
		// grab the StatefulSet object, extract the owner...
		statefulset := rawObj.(*v1apps.StatefulSet)
		owner := metav1.GetControllerOf(statefulset)
		if owner == nil {
			return nil
		}
		// ...make sure it's a pipeline...
		if owner.APIVersion != apiGVStr || owner.Kind != "Pipeline" {
			return nil
		}

		// ...and if so, return it
		return []string{owner.Name}
	}); err != nil {
		return err
	}

	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &v1.Pod{}, ownerKey, func(rawObj runtime.Object) []string {
		// grab the Pod object, extract the owner...
		pod := rawObj.(*v1.Pod)
		owner := metav1.GetControllerOf(pod)
		if owner == nil {
			return nil
		}
		// ...make sure it's a pipeline...
		if owner.APIVersion != apiGVStr || owner.Kind != "Pipeline" {
			return nil
		}

		// ...and if so, return it
		return []string{owner.Name}
	}); err != nil {
		return err
	}

	// the For functions below are the equivalent of doing a
	// Watches(&source.Kind{Type: apiType}, &handler.EnqueueRequestForObject{})
	// the Owns functions below are the equivalent of doing a
	// Watches(&source.Kind{Type: <ForType-forInput>}, &handler.EnqueueRequestForOwner{OwnerType: apiType, IsController: true})
	return ctrl.NewControllerManagedBy(mgr).
		For(&churrov1alpha1.Pipeline{}).
		Owns(&rbacv1.Role{}).
		Owns(&rbacv1.RoleBinding{}).
		Owns(&v1.Pod{}).
		Owns(&v1.Service{}).
		Owns(&v1.ServiceAccount{}).
		Owns(&v1.PersistentVolumeClaim{}).
		Owns(&v1.Secret{}).
		Owns(&v1beta1.PodDisruptionBudget{}).
		Owns(&v1apps.StatefulSet{}).
		Complete(r)
}
