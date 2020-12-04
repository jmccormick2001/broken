package operator

import (
	"github.com/prometheus/common/log"
	"gitlab.com/churro-group/churro/api/v1alpha1"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	yaml "sigs.k8s.io/yaml"
)

const (
	ctlPodName = "churro-ctl"
)

func (r PipelineReconciler) processCtl(req ctrl.Request, pipeline v1alpha1.Pipeline) error {
	// get referenced churro-ctl Pod objects
	var pod v1.Pod
	var podFound bool
	thing := types.NamespacedName{
		Namespace: pipeline.ObjectMeta.Namespace,
		Name:      ctlPodName,
	}
	err := r.Get(r.Ctx, thing, &pod)
	if err != nil {
		log.Info(err, "unable to get churro-ctl Pod")
	} else {
		podFound = true
	}

	// create the churro-ctl Pod if necessary
	if !podFound {
		var pod v1.Pod
		err := yaml.Unmarshal(r.CtlPodTemplate, &pod)
		if err != nil {
			log.Error(err, "unable to unmarshal churro-ctl pod template")
			return err
		}

		pod.ObjectMeta.Labels = map[string]string{"app": "churro", "pipeline": pipeline.ObjectMeta.Name, "service": ctlPodName}
		pod.Namespace = pipeline.ObjectMeta.Namespace
		pod.Name = ctlPodName

		pEnv := v1.EnvVar{Name: "CHURRO_PIPELINE", Value: pipeline.ObjectMeta.Name}
		pod.Spec.Containers[0].Env = append(pod.Spec.Containers[0].Env, pEnv)

		if err := ctrl.SetControllerReference(&pipeline, &pod, r.Scheme); err != nil {
			log.Error(err, "error setting controller reference")
			return err
		}
		if err := r.Create(r.Ctx, &pod); err != nil {
			log.Error(err, "unable to create churro-ctl pod for pipeline", "pod", pod)
			return err
		}
		r.Log.V(1).Info("created churro-ctl pod for pipeline ")
	}

	return nil
}
