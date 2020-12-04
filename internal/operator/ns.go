// Package operator holds the churro operator logic
package operator

import (
	"github.com/prometheus/common/log"
	"gitlab.com/churro-group/churro/api/v1alpha1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ProcessCR handles a reconcile event
func (r PipelineReconciler) ProcessCR(req ctrl.Request) (result ctrl.Result, err error) {

	// get the CR
	var pipeline v1alpha1.Pipeline
	if err := r.Get(r.Ctx, req.NamespacedName, &pipeline); err != nil {
		log.Error(err, "unable to fetch pipeline")
		// we'll ignore not-found errors, since they can't be fixed by an immediate
		// requeue (we'll need to wait for a new notification), and we can get them
		// on deleted requests.
		return result, client.IgnoreNotFound(err)
	}
	log.Infof("got a pipeline %s\n", pipeline.Name)
	err = r.processPipeline(req, pipeline)
	if err != nil {
		log.Error(err, "error in processing pipeline")
	}
	return result, err
}

func (r PipelineReconciler) processPipeline(req ctrl.Request, pipeline v1alpha1.Pipeline) error {
	// insure rbac is defined for the pipeline
	err := r.processRoles(pipeline)
	if err != nil {
		return err
	}
	err = r.processRoleBindings(pipeline)
	if err != nil {
		return err
	}

	err = r.processServiceAccounts(pipeline)
	if err != nil {
		return err
	}
	err = r.processServices(pipeline)
	if err != nil {
		return err
	}
	err = r.processPVCs(pipeline)
	if err != nil {
		return err
	}
	err = r.processSecrets(pipeline)
	if err != nil {
		return err
	}
	err = r.processPDBs(pipeline)
	if err != nil {
		return err
	}
	err = r.processStatefulSet(pipeline)
	if err != nil {
		return err
	}
	err = r.initStatefulSet(req, pipeline)
	if err != nil {
		return err
	}
	err = r.processWatch(req, pipeline)
	if err != nil {
		return err
	}
	err = r.processCtl(req, pipeline)
	if err != nil {
		return err
	}
	err = r.processLoader(req, pipeline)
	if err != nil {
		return err
	}
	err = r.processCockroachClient(req, pipeline)
	if err != nil {
		return err
	}
	return err
}
