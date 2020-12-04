package operator

import (
	"fmt"

	"github.com/prometheus/common/log"
	"gitlab.com/churro-group/churro/api/v1alpha1"

	v1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	cockroachClientSecret = "cockroachdb.client.root"
	cockroachNodeSecret   = "cockroachdb.node"
	churroGRPCSecret      = "churro.client.root"
	churroConfigSecret    = "churro.config"
)

func (r PipelineReconciler) processSecrets(pipeline v1alpha1.Pipeline) error {
	// get referenced secrets objects
	var childSecrets v1.SecretList
	err := r.List(r.Ctx, &childSecrets, client.InNamespace(pipeline.ObjectMeta.Namespace), client.MatchingFields{jobOwnerKey: pipeline.ObjectMeta.Name})
	if err != nil {
		log.Error(err, "unable to list child Secret")
		return err
	}

	// compare referenced Secret objects with what we expect
	// make sure we have a secret/cockroachdb.client.root
	needCockroachClientSecret := true
	// make sure we have a secret/cockroachdb.node
	needCockroachNodeSecret := true
	// make sure we have a secret/churro.client.root
	needChurroGRPCSecret := true

	for i := 0; i < len(childSecrets.Items); i++ {
		r := childSecrets.Items[i]
		switch r.Name {
		case cockroachClientSecret:
			needCockroachClientSecret = false
		case cockroachNodeSecret:
			needCockroachNodeSecret = false
		case churroGRPCSecret:
			needChurroGRPCSecret = false
		}
	}

	// create any expected secrets , set owner reference to this pipeline
	secretsToCreate := make([]v1.Secret, 0)
	if needCockroachClientSecret {
		secret := v1.Secret{}
		secret.ObjectMeta.Labels = map[string]string{"app": "churro", "pipeline": pipeline.ObjectMeta.Name}
		secret.Name = cockroachClientSecret
		err := doDBCreds(&secret, pipeline.Name, pipeline.Spec.DatabaseCredentials)
		if err != nil {
		}
		secret.Namespace = pipeline.ObjectMeta.Namespace
		secretsToCreate = append(secretsToCreate, secret)
	}
	if needCockroachNodeSecret {
		secret := v1.Secret{}
		secret.ObjectMeta.Labels = map[string]string{"app": "churro", "pipeline": pipeline.ObjectMeta.Name}
		secret.Name = cockroachNodeSecret
		err := doDBNodeCreds(&secret, pipeline.Spec.DatabaseCredentials)
		if err != nil {
			return err
		}
		secret.Namespace = pipeline.ObjectMeta.Namespace
		secretsToCreate = append(secretsToCreate, secret)
	}
	if needChurroGRPCSecret {
		secret := v1.Secret{}
		secret.ObjectMeta.Labels = map[string]string{"app": "churro", "pipeline": pipeline.ObjectMeta.Name}
		secret.Name = churroGRPCSecret
		secret.Namespace = pipeline.ObjectMeta.Namespace
		err := doServiceCreds(&secret, pipeline.Spec.ServiceCredentials)
		if err != nil {
			return err
		}

		secretsToCreate = append(secretsToCreate, secret)
	}

	for _, secret := range secretsToCreate {
		if err := ctrl.SetControllerReference(&pipeline, &secret, r.Scheme); err != nil {
			return err
		}
		if err := r.Create(r.Ctx, &secret); err != nil {
			log.Error(err, "unable to create Secret for pipeline", "service", secret)
			return err
		}
		fmt.Println("created Secret for pipeline ")
	}

	return nil
}

func doDBNodeCreds(secret *v1.Secret, dbCreds v1alpha1.DBCreds) error {
	// key should be file name in dir
	// data should be base64 encoded should be file contents
	secret.Data = make(map[string][]byte, 0)

	// node.key
	if len(dbCreds.NodeKey) == 0 {
		return fmt.Errorf("node.key is empty")
	}
	secret.Data["node.key"] = []byte(dbCreds.NodeKey)
	fmt.Printf("node.key bytes %d\n", len(dbCreds.NodeKey))

	if len(dbCreds.NodeCrt) == 0 {
		return fmt.Errorf("node.crt is empty")
	}

	secret.Data["node.crt"] = []byte(dbCreds.NodeCrt)
	fmt.Printf("node.crt bytes %d\n", len(dbCreds.NodeCrt))

	// ca.crt
	if len(dbCreds.CACrt) == 0 {
		return fmt.Errorf("ca.crt is empty")
	}
	secret.Data["ca.crt"] = []byte(dbCreds.CACrt)
	fmt.Printf("ca.crt bytes %d\n", len(dbCreds.CACrt))
	// ca.key
	if len(dbCreds.CAKey) == 0 {
		return fmt.Errorf("ca.key is empty")
	}
	secret.Data["ca.key"] = []byte(dbCreds.CAKey)
	fmt.Printf("ca.key bytes %d\n", len(dbCreds.CAKey))

	secret.Data["client.root.key"] = []byte(dbCreds.ClientRootKey)
	fmt.Printf("client.root.key bytes %d\n", len(dbCreds.ClientRootKey))
	// client.root.crt
	if len(dbCreds.ClientRootCrt) == 0 {
		return fmt.Errorf("client.root.crt is empty")
	}
	secret.Data["client.root.crt"] = []byte(dbCreds.ClientRootCrt)
	fmt.Printf("client.root.crt bytes %d\n", len(dbCreds.ClientRootCrt))
	// client.somepipeline.crt
	if len(dbCreds.ClientRootCrt) == 0 {
		return fmt.Errorf("client.pipeline.crt is empty")
	}
	return nil
}

func doDBCreds(secret *v1.Secret, pipelineName string, dbCreds v1alpha1.DBCreds) error {
	// key should be file name in dir
	// data should be base64 encoded should be file contents
	secret.Data = make(map[string][]byte, 0)

	// ca.crt
	if len(dbCreds.CACrt) == 0 {
		return fmt.Errorf("ca.crt is empty")
	}
	secret.Data["ca.crt"] = []byte(dbCreds.CACrt)
	fmt.Printf("ca.crt bytes %d\n", len(dbCreds.CACrt))
	// ca.key
	if len(dbCreds.CAKey) == 0 {
		return fmt.Errorf("ca.key is empty")
	}
	secret.Data["ca.key"] = []byte(dbCreds.CAKey)
	fmt.Printf("ca.key bytes %d\n", len(dbCreds.CAKey))
	// client.root.key
	if len(dbCreds.ClientRootKey) == 0 {
		return fmt.Errorf("client.root.key is empty")
	}
	secret.Data["client.root.key"] = []byte(dbCreds.ClientRootKey)
	fmt.Printf("client.root.key bytes %d\n", len(dbCreds.ClientRootKey))
	// client.root.crt
	if len(dbCreds.ClientRootCrt) == 0 {
		return fmt.Errorf("client.root.crt is empty")
	}
	secret.Data["client.root.crt"] = []byte(dbCreds.ClientRootCrt)
	fmt.Printf("client.root.crt bytes %d\n", len(dbCreds.ClientRootCrt))
	// client.somepipeline.crt
	if len(dbCreds.ClientRootCrt) == 0 {
		return fmt.Errorf("client.pipeline.crt is empty")
	}
	secret.Data["client."+pipelineName+".crt"] = []byte(dbCreds.PipelineCrt)
	fmt.Printf("client."+pipelineName+".crt bytes %d\n", len(dbCreds.PipelineCrt))
	// client.somepipeline.key
	if len(dbCreds.ClientRootKey) == 0 {
		return fmt.Errorf("client.pipeline.key is empty")
	}
	secret.Data["client."+pipelineName+".key"] = []byte(dbCreds.PipelineKey)
	fmt.Printf("client."+pipelineName+".key bytes %d\n", len(dbCreds.PipelineKey))
	return nil
}

func doServiceCreds(secret *v1.Secret, serviceCreds v1alpha1.ServiceCreds) error {
	// key should be file name in dir
	// data should be base64 encoded should be file contents
	secret.Data = make(map[string][]byte, 0)

	if len(serviceCreds.ServiceCrt) == 0 {
		return fmt.Errorf("service.crt is empty")
	}
	if len(serviceCreds.ServiceKey) == 0 {
		return fmt.Errorf("service.key is empty")
	}

	secret.Data["service.crt"] = []byte(serviceCreds.ServiceCrt)
	fmt.Printf("service.crt bytes %d\n", len(serviceCreds.ServiceCrt))

	secret.Data["service.key"] = []byte(serviceCreds.ServiceKey)
	fmt.Printf("service.key bytes %d\n", len(serviceCreds.ServiceKey))

	return nil
}
