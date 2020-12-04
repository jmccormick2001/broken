package operator

import (
	"fmt"
	"github.com/prometheus/common/log"
	"gitlab.com/churro-group/churro/api/v1alpha1"
	v1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	yaml "sigs.k8s.io/yaml"
)

const (
	churroDataPVC = "churrodata"
)

func (r PipelineReconciler) processPVCs(pipeline v1alpha1.Pipeline) error {
	// get referenced PVC objects
	var childPVCs v1.PersistentVolumeClaimList
	err := r.List(r.Ctx, &childPVCs, client.InNamespace(pipeline.ObjectMeta.Namespace), client.MatchingFields{jobOwnerKey: pipeline.ObjectMeta.Name})
	if err != nil {
		log.Error(err, "unable to list child PVCs")
		return err
	}

	// compare referenced PVC objects with what we expect
	// make sure we have a pvc/churrodata
	needChurrodataPVC := true

	for i := 0; i < len(childPVCs.Items); i++ {
		r := childPVCs.Items[i]
		switch r.Name {
		case churroDataPVC:
			needChurrodataPVC = false
		}
	}

	// create any expected PVC objects, set owner reference to this pipeline
	pvcsToCreate := make([]v1.PersistentVolumeClaim, 0)
	if needChurrodataPVC {
		var pvc v1.PersistentVolumeClaim
		err := yaml.Unmarshal(r.PVCTemplate, &pvc)
		if err != nil {
			log.Error(err, "unable to unmarshal PVC template")
			return err
		}

		pvc.ObjectMeta.Labels = map[string]string{"app": "churro", "pipeline": pipeline.ObjectMeta.Name}
		pvc.Name = churroDataPVC
		pvc.Namespace = pipeline.ObjectMeta.Namespace
		/**
		apiVersion: v1
		kind: PersistentVolumeClaim
		metadata:
		  annotations:
		    volume.beta.kubernetes.io/storage-provisioner: rancher.io/local-path
		    volume.kubernetes.io/selected-node: roaster
		  labels:
		    app: churro
		    pipeline: pipeline1
		  name: churrodata
		spec:
		  accessModes:
		  - ReadWriteOnce
		  resources:
		    requests:
		      storage: 100M
		  storageClassName: local-path
		*/
		pvcsToCreate = append(pvcsToCreate, pvc)
	}

	for _, pvc := range pvcsToCreate {
		if err := ctrl.SetControllerReference(&pipeline, &pvc, r.Scheme); err != nil {
			return err
		}
		if err := r.Create(r.Ctx, &pvc); err != nil {
			log.Error(err, "unable to create PVC for pipeline", "pvc", pvc)
			return err
		}
		fmt.Println("created pvc for pipeline ")
	}

	return nil
}
