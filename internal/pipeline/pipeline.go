package pipeline

import (
	"context"
	"fmt"
	"time"

	"gitlab.com/churro-group/churro/api/v1alpha1"
	"gitlab.com/churro-group/churro/internal"
	"gitlab.com/churro-group/churro/internal/churroctl"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"go.uber.org/zap"
)

func GetPipeline(namespace string) (v1alpha1.Pipeline, error) {

	var p *v1alpha1.Pipeline

	// connect to the Kube API
	_, config, err := internal.GetKubeClient()
	if err != nil {
		return *p, err
	}

	pipelineClient, err := internal.NewClient(config, namespace)
	if err != nil {
		return *p, err
	}

	p, err = pipelineClient.Get(namespace)
	if err != nil {
		return *p, err
	}

	return *p, nil
}

func DeletePipeline(logger *zap.SugaredLogger, pipelineName string) error {
	// connect to the Kube API
	_, config, err := internal.GetKubeClient()
	if err != nil {
		return err
	}

	pipelineClient, err := internal.NewClient(config, pipelineName)
	if err != nil {
		return err
	}

	err = pipelineClient.Delete(pipelineName, &metav1.DeleteOptions{})
	if err != nil {
		return err
	}
	return nil

}

func CreatePipeline(logger *zap.SugaredLogger, cr v1alpha1.Pipeline) error {

	fmt.Printf("in pipeline.CreatePipeline\n")
	pipelineName := cr.ObjectMeta.Name

	// connect to the Kube API
	client, config, err := internal.GetKubeClient()
	if err != nil {
		logger.Error(err.Error())
		fmt.Println(err.Error())
		return err
	}

	// create the pipeline namespace if necessary

	ns, err := client.CoreV1().Namespaces().Get(context.TODO(), pipelineName, metav1.GetOptions{})
	if kerrors.IsNotFound(err) {
		logger.Info("namespace is not found, will create...", zap.String("pipeline", pipelineName))
		// create the namespace
		ns.ObjectMeta.Name = pipelineName
		_, err := client.CoreV1().Namespaces().Create(context.TODO(), ns, metav1.CreateOptions{})
		if err != nil {
			logger.Error("error creating namespace", zap.String("name", ns.Name), zap.String("error", err.Error()))
		}
		fmt.Printf("created namespace [%s]\n", pipelineName)

	} else if err != nil {
		logger.Error(err.Error())
		fmt.Println(err.Error())
		return err
	}

	// generate the pipeline credentials

	rsaBits := 4096
	dur, err := time.ParseDuration("8760h")
	if err != nil {
		logger.Error(err.Error())
		fmt.Println(err.Error())
		return err
	}
	serviceHosts := fmt.Sprintf("churro-ctl.%s.svc.cluster.local,localhost,churro-watch,churro-loader,churro-ctl,127.0.0.1", pipelineName)
	dbCreds, err := churroctl.GenerateChurroCreds(pipelineName, serviceHosts, rsaBits, dur)
	if err != nil {
		logger.Error(err.Error())
		fmt.Println(err.Error())
		return err
	}

	fillCRDefaults(&cr, pipelineName)

	// add the credentials to the CR

	d := v1alpha1.DBCreds{}
	d.CAKey = string(dbCreds.Cakey)
	d.CACrt = string(dbCreds.Cacrt)
	d.NodeKey = string(dbCreds.Nodekey)
	d.NodeCrt = string(dbCreds.Nodecrt)
	d.ClientRootCrt = string(dbCreds.Clientrootcrt)
	d.ClientRootKey = string(dbCreds.Clientrootkey)
	d.PipelineCrt = string(dbCreds.Clientcrt)
	d.PipelineKey = string(dbCreds.Clientkey)

	cr.Spec.DatabaseCredentials = d

	s := v1alpha1.ServiceCreds{}
	s.ServiceCrt = string(dbCreds.Servicecrt)
	s.ServiceKey = string(dbCreds.Servicekey)

	cr.Spec.ServiceCredentials = s

	// create the pipeline CR in k8s
	pipelineClient, err := internal.NewClient(config, pipelineName)
	if err != nil {
		logger.Error(err.Error())
		fmt.Println(err.Error())
		return err
	}

	fmt.Printf("jeff about to create CR %+v\n", cr)

	result, err := pipelineClient.Create(&cr)
	if err != nil {
		logger.Error(err.Error())
		fmt.Println(err.Error())
		return err
	}

	//fmt.Printf("created CR %s\n", result.Name)
	fmt.Printf("jeff created CR %+v\n", result)

	return nil
}

func fillCRDefaults(cr *v1alpha1.Pipeline, pipelineName string) {

	cr.APIVersion = "churro.project.io/v1alpha1"
	cr.Kind = "Pipeline"
	cr.ObjectMeta.Name = pipelineName
	cr.Labels = make(map[string]string)
	cr.Labels["name"] = pipelineName
	cr.Status.Active = "true"
	cr.Status.Standby = []string{"one", "two"}
	cr.Spec.AdminDataSource.Name = "churrodatastore"
	cr.Spec.AdminDataSource.Host = "cockroachdb-public"
	cr.Spec.AdminDataSource.Path = ""
	cr.Spec.AdminDataSource.Port = 26257
	cr.Spec.AdminDataSource.Scheme = ""
	cr.Spec.AdminDataSource.Username = "root"
	cr.Spec.AdminDataSource.Database = "churro"
	cr.Spec.DataSource.Name = "pipelinedatastore"
	cr.Spec.DataSource.Host = "cockroachdb-public"
	cr.Spec.DataSource.Path = ""
	cr.Spec.DataSource.Port = 26257
	cr.Spec.DataSource.Scheme = ""
	cr.Spec.DataSource.Username = pipelineName
	cr.Spec.DataSource.Database = pipelineName
	cr.Spec.LoaderConfig.Location.Scheme = "http"
	cr.Spec.LoaderConfig.Location.Host = "churro-loader"
	cr.Spec.LoaderConfig.Location.Port = 8083
	cr.Spec.LoaderConfig.QueueSize = 30
	cr.Spec.LoaderConfig.PctHeadRoom = 50
	cr.Spec.LoaderConfig.DataSource = v1alpha1.Source{}
	cr.Spec.WatchConfig.Location.Scheme = "http"
	cr.Spec.WatchConfig.Location.Host = "churro-watch"
	cr.Spec.WatchConfig.Location.Port = 8087
}
