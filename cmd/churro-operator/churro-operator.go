package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"

	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	ctrl "sigs.k8s.io/controller-runtime"

	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	churrov1alpha1 "gitlab.com/churro-group/churro/api/v1alpha1"
	"gitlab.com/churro-group/churro/internal/operator"
	// +kubebuilder:scaffold:imports
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

type ChurroOperatorFlags struct {
	metricsAddr                string
	watchPodTemplate           []byte
	pvcTemplate                []byte
	statefulsetTemplate        []byte
	cockroachClientPodTemplate []byte
	loaderPodTemplate          []byte
	ctlPodTemplate             []byte
	enableLeaderElection       bool
}

const (
	templatePath            = "/templates"
	cockroachClientFileName = "client.yaml"
	pvcFileName             = "churrodata-pvc.yaml"
	statefulSetFileName     = "cockroachdb-statefulset.yaml"
	watchFileName           = "churro-watch.yaml"
	ctlFileName             = "churro-ctl.yaml"
	loaderFileName          = "churro-loader.yaml"
)

func init() {
	_ = clientgoscheme.AddToScheme(scheme)

	_ = churrov1alpha1.AddToScheme(scheme)
	// +kubebuilder:scaffold:scheme
}

func main() {

	flags := processFlags()

	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:             scheme,
		MetricsBindAddress: flags.metricsAddr,
		Port:               9443,
		LeaderElection:     flags.enableLeaderElection,
		LeaderElectionID:   "d296171c.project.io",
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	if err = (&operator.PipelineReconciler{
		Client:                     mgr.GetClient(),
		Log:                        ctrl.Log.WithName("controllers").WithName("Pipeline"),
		Scheme:                     mgr.GetScheme(),
		PVCTemplate:                flags.pvcTemplate,
		WatchPodTemplate:           flags.watchPodTemplate,
		CockroachClientPodTemplate: flags.cockroachClientPodTemplate,
		CtlPodTemplate:             flags.ctlPodTemplate,
		LoaderPodTemplate:          flags.loaderPodTemplate,
		StatefulSetTemplate:        flags.statefulsetTemplate,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "Pipeline")
		os.Exit(1)
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}

func processFlags() ChurroOperatorFlags {
	flags := ChurroOperatorFlags{}

	flag.StringVar(&flags.metricsAddr, "metrics-addr", ":8080", "The address the metric endpoint binds to.")
	flag.BoolVar(&flags.enableLeaderElection, "enable-leader-election", false,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	flag.Parse()
	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))

	setupLog := ctrl.Log.WithName("setup")
	var err error
	path := fmt.Sprintf("%s%s%s", templatePath, "/", pvcFileName)
	flags.pvcTemplate, err = ioutil.ReadFile(path)
	if err != nil {
		setupLog.Error(err, "unable to read "+path)
		os.Exit(1)
	}
	path = fmt.Sprintf("%s%s%s", templatePath, "/", cockroachClientFileName)
	flags.cockroachClientPodTemplate, err = ioutil.ReadFile(path)
	if err != nil {
		setupLog.Error(err, "unable to read "+path)
		os.Exit(1)
	}
	path = fmt.Sprintf("%s%s%s", templatePath, "/", watchFileName)
	flags.watchPodTemplate, err = ioutil.ReadFile(path)
	if err != nil {
		setupLog.Error(err, "unable to read "+path)
		os.Exit(1)
	}
	path = fmt.Sprintf("%s%s%s", templatePath, "/", loaderFileName)
	flags.loaderPodTemplate, err = ioutil.ReadFile(path)
	if err != nil {
		setupLog.Error(err, "unable to read "+path)
		os.Exit(1)
	}
	path = fmt.Sprintf("%s%s%s", templatePath, "/", ctlFileName)
	flags.ctlPodTemplate, err = ioutil.ReadFile(path)
	if err != nil {
		setupLog.Error(err, "unable to read "+path)
		os.Exit(1)
	}
	path = fmt.Sprintf("%s%s%s", templatePath, "/", statefulSetFileName)
	flags.statefulsetTemplate, err = ioutil.ReadFile(path)
	if err != nil {
		setupLog.Error(err, "unable to read "+path)
		os.Exit(1)
	}

	return flags
}
