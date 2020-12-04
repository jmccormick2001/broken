package watch

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"

	_ "github.com/lib/pq"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	cruntime "sigs.k8s.io/controller-runtime/pkg/client/config"
	"sigs.k8s.io/yaml"

	"github.com/fsnotify/fsnotify"
	"gitlab.com/churro-group/churro/api/v1alpha1"
	"gitlab.com/churro-group/churro/internal/config"
	pb "gitlab.com/churro-group/churro/rpc/watch"

	"os"

	"go.uber.org/zap"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/client-go/kubernetes"
)

const (
	DEFAULT_PORT = ":8087"
)

// Server implements the watch service
type Server struct {
	logger           *zap.SugaredLogger
	Pi               v1alpha1.Pipeline
	ServiceCreds     config.ServiceCredentials
	DBCreds          config.DBCredentials
	UserDBCreds      config.DBCredentials
	WatchDirectories []WatchDirectory
}

func (s *Server) Ping(ctx context.Context, size *pb.PingRequest) (hat *pb.PingResponse, err error) {
	return &pb.PingResponse{}, nil
}

// NewWatchServer creates a watch server and returns
// a pointer to it.  The server is built based on the configuration
// passed to it.
func NewWatchServer(debug bool, svcCreds config.ServiceCredentials, pipeline v1alpha1.Pipeline, l *zap.SugaredLogger, userDBCreds config.DBCredentials, dbCreds config.DBCredentials) *Server {

	s := &Server{
		logger:       l,
		ServiceCreds: svcCreds,
		UserDBCreds:  userDBCreds,
		DBCreds:      dbCreds,
		Pi:           pipeline,
	}

	go s.startSocketProcessing()

	go s.startWatching()

	s.logger.Infof("watch service started %s\n", DEFAULT_PORT)
	return s
}

func (s *Server) startSocketProcessing() {
	sockets := s.Pi.Spec.WatchSockets

	s.logger.Info("watching the following sockets...")
	for _, socket := range sockets {
		s.logger.Infof("socket %v\n", socket)
		err := s.createExtractPod(socket.Scheme, socket.Path, s.Pi, socket.Tablename, "")
		if err != nil {
			s.logger.Errorf("error createExtractPod %s\n", err.Error())
		}
	}

}

func (s *Server) startWatching() {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		s.logger.Errorf("error in watcher %s\n", err.Error())
		os.Exit(1)
	}
	defer watcher.Close()

	done := make(chan bool)

	// TODO totally refactor this select to use the regex that is
	// in the CR watch configurations instead of them being
	// hard-coded here
	go func() {
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				//log.Infof("event: %v", event)
				if event.Op&fsnotify.Write == fsnotify.Write {
					s.logger.Debugf("modified file: %s\n", event.Name)
				}
				if event.Op == fsnotify.Create {
					filePath := event.Name
					err := s.createExtractPodForNewFile(filePath)
					if err != nil {
						s.logger.Error(err.Error())
					}
				}
			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				if err != nil {
					s.logger.Error(err.Error())
				}
			}
		}
	}()

	s.createWatchedDirectories(watcher)

	<-done

}

func getTable(scheme string, dirs []WatchDirectory) (string, error) {
	for _, dir := range dirs {
		if dir.Scheme == scheme {
			return dir.Tablename, nil
		}
	}
	return "", fmt.Errorf("could not find right scheme %s in churro config", scheme)
}

func (s *Server) createExtractPod(scheme string, filePath string, cfg v1alpha1.Pipeline, tableName, watchDirName string) error {
	ns := os.Getenv("CHURRO_NAMESPACE")
	pipelineName := os.Getenv("CHURRO_PIPELINE")
	imageName := "registry.gitlab.com/churro-group/churro/churro-extract"
	ctx := context.TODO()

	// TODO cleanup this selection logic as its redundant

	switch scheme {
	case config.FinnHubScheme:
	case config.XMLScheme:
	case config.CSVScheme:
	case config.JSONScheme:
	case config.JSONPathScheme:
	case config.XLSXScheme:
		s.logger.Debugf("scheme used for extract job %s\n", scheme)
	default:
		return fmt.Errorf("%s scheme is not recognized", scheme)
	}

	client, err := GetKubeClient("")
	if err != nil {
		return err
	}

	pod := getPodDefinition(filePath, tableName, scheme, rand.String(4), ns, imageName, pipelineName, watchDirName)
	s.logger.Debugf("creating pod %s\n", pod.Name)

	pod, err = client.CoreV1().Pods(ns).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		return err
	}

	return nil
}

// getPodDefinition fills out a Pod definition
func getPodDefinition(filePath, tableName, scheme, suffix, namespace, imageName, pipelineName, watchDirName string) *v1.Pod {
	entrypoint := []string{
		"/usr/local/bin/churro-extract",
		"-servicecert",
		"/servicecerts",
		"-dbcert",
		"/dbcerts",
		"-debug",
		"true",
	}

	var mode int32
	//mode = 0620
	mode = 256

	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("churro-extract-%s", suffix),
			Namespace: namespace,
			Labels: map[string]string{
				"app":     "churro",
				"service": "churro-extract",
			},
		},
		Spec: v1.PodSpec{
			ServiceAccountName: "churro",
			RestartPolicy:      v1.RestartPolicyNever,
			Containers: []v1.Container{
				{
					Name:            "churro-extract",
					Image:           imageName,
					ImagePullPolicy: v1.PullIfNotPresent,
					Command:         entrypoint,
					VolumeMounts: []v1.VolumeMount{
						{
							MountPath: "/dbcerts",
							Name:      "db-certs",
							ReadOnly:  true,
						},
						{
							MountPath: "/servicecerts",
							Name:      "service-certs",
							ReadOnly:  true,
						},
						{
							MountPath: "/churro",
							Name:      "churrodata",
							ReadOnly:  false,
						},
					},
					Env: []v1.EnvVar{
						{
							Name: "CHURRO_NAMESPACE",
							ValueFrom: &v1.EnvVarSource{
								FieldRef: &v1.ObjectFieldSelector{
									FieldPath: "metadata.namespace",
								},
							},
						},
						{
							Name:  "CHURRO_PIPELINE",
							Value: pipelineName,
						},
						{
							Name:  "CHURRO_FILENAME",
							Value: filePath,
						},
						{
							Name:  "CHURRO_SCHEME",
							Value: scheme,
						},
						{
							Name:  "CHURRO_WATCHDIR_NAME",
							Value: watchDirName,
						},
						{
							Name:  "CHURRO_TABLENAME",
							Value: tableName,
						},
					},
				},
			},
			Volumes: []v1.Volume{
				{
					Name: "db-certs",
					VolumeSource: v1.VolumeSource{
						Secret: &v1.SecretVolumeSource{
							SecretName:  "cockroachdb.client.root",
							DefaultMode: &mode,
						},
					},
				},
				{
					Name: "service-certs",
					VolumeSource: v1.VolumeSource{
						Secret: &v1.SecretVolumeSource{
							SecretName: "churro.client.root",
						},
					},
				},
				{
					Name: "churrodata",
					VolumeSource: v1.VolumeSource{
						PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
							ClaimName: "churrodata",
						},
					},
				},
			},
		},
	}
}

func GetKubeClient(kubeconfig string) (client kubernetes.Interface, err error) {

	/**
	if kubeconfig != "" {
		os.Setenv(k8sutil.KubeConfigEnvVar, kubeconfig)
	}
	*/

	config, err := cruntime.GetConfig()
	if err != nil {
		return client, err
	}

	// create the clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return client, err
	}

	return clientset, err
}

func (s *Server) createWatchedDirectories(watcher *fsnotify.Watcher) {

	// TODO get the list of watched directories from the database to
	// bootstrap the watching process

	pgConnectString := s.DBCreds.GetDBConnectString(s.Pi.Spec.AdminDataSource)
	s.logger.Info("db creds", zap.String("pgConnectString", pgConnectString))
	db, err := sql.Open("postgres", pgConnectString)
	if err != nil {
		s.logger.Errorf("error opening pipeline db %s\n", err.Error())
		return
	}

	s.WatchDirectories, err = GetWatchDirectories(db)
	if err != nil {
		s.logger.Errorf("error getting watch directories %s\n", err.Error())
	}
	db.Close()
	s.logger.Infof("watch directories found %d\n", len(s.WatchDirectories))

	for _, dir := range s.WatchDirectories {
		s.logger.Debugf("watch service: watching %s\n", dir.Path)
		_, err := os.Stat(dir.Path)
		if os.IsNotExist(err) {
			s.logger.Errorf("dir path not exist, will create %s %s\n", dir.Path, err.Error())
			err = os.Mkdir(dir.Path, os.ModePerm)
			if err != nil {
				s.logger.Errorf("could not create directory %s %s\n", dir.Path, err.Error())
			} else {
				s.logger.Infof("created directory %s\n", dir.Path)
			}
		}
		_, err = os.Stat(dir.Path)
		if err == nil {
			err = watcher.Add(dir.Path)
			if err != nil {
				s.logger.Errorf("error adding watch path %s %s", dir.Path, err.Error())
			} else {
				s.logger.Infof("added watched path %s\n", dir.Path)
			}
		}
	}
}

func (s *Server) createExtractPodForNewFile(filePath string) error {

	dirs := s.WatchDirectories

	for i := 0; i < len(dirs); i++ {
		regex := dirs[i].Regex
		scheme := dirs[i].Scheme
		watchDirName := dirs[i].Name
		s.logger.Infof("scheme %s %s\n", scheme, regex)
		match, err := regexp.Match(regex, []byte(filePath))
		if err != nil {
			s.logger.Errorf("error in regexp match %s %s\n", regex, err.Error())
			return err
		}

		if match {
			tableName, err := getTable(scheme, s.WatchDirectories)
			if err != nil {
				s.logger.Errorf("error getting table %s\n", err.Error())
				return err
			}

			err = s.createExtractPod(scheme, filePath, s.Pi, tableName, watchDirName)
			if err != nil {
				s.logger.Errorf("error in createExtractPod %s\n", err.Error())
				return err
			}
		}
	}
	return nil
}

func (s *Server) CreateWatchDirectory(ctx context.Context, req *pb.CreateWatchDirectoryRequest) (response *pb.CreateWatchDirectoryResponse, err error) {

	resp := &pb.CreateWatchDirectoryResponse{}
	var wdir WatchDirectory

	// this is the watchdirectory that was inserted into
	// the database by churro-ctl, churro-ctl then calls
	// this service to make the change dynamically
	err = yaml.Unmarshal([]byte(req.ConfigString), &wdir)
	if err != nil {
		s.logger.Errorf("error CreateWatchDirectory %s\n", err.Error())
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	// TODO update the current set of watched directories here

	return resp, nil
}

func (s *Server) DeleteWatchDirectory(ctx context.Context, req *pb.DeleteWatchDirectoryRequest) (response *pb.DeleteWatchDirectoryResponse, err error) {

	resp := &pb.DeleteWatchDirectoryResponse{}

	// TODO update the current in-memory configuration by
	// removing the passed WatchName req.WatchName
	if req.WatchName == "" {
		s.logger.Error("error watchname is empty")
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	return resp, nil
}
