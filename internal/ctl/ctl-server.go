package ctl

import (
	"context"
	"fmt"
	"os"

	"gitlab.com/churro-group/churro/api/v1alpha1"
	"gitlab.com/churro-group/churro/internal/config"
	pb "gitlab.com/churro-group/churro/rpc/ctl"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var DBCertPath string
var ServiceCertPath string

var svcCreds config.ServiceCredentials
var dbCreds config.DBCredentials
var userDBCreds config.DBCredentials

const (
	DEFAULT_PORT = ":8088"
)

type Server struct {
	logger       *zap.SugaredLogger
	Pi           v1alpha1.Pipeline
	ServiceCreds config.ServiceCredentials
	DBCreds      config.DBCredentials
	UserDBCreds  config.DBCredentials
}

func init() {
	fmt.Println("initializing Pipelines")
}

// NewCtlServer constructs a ctl server based on the passed
// configuration, a pointer to the server is returned
func NewCtlServer(ns string, debug bool, serviceCertPath string, dbCertPath string, pipeline v1alpha1.Pipeline, l *zap.SugaredLogger) *Server {
	s := Server{
		logger: l,
		Pi:     pipeline,
	}

	err := s.SetupCredentials(ns, serviceCertPath, dbCertPath)
	if err != nil {
		l.Error("error getting credentials", zap.Error(err))
		os.Exit(1)
	}

	s.DBCreds = dbCreds
	s.UserDBCreds = userDBCreds
	s.ServiceCreds = svcCreds

	err = s.verify()
	if err != nil {
		l.Error("error seeding pipeline admin database", zap.Error(err))
		os.Exit(1)
	}

	return &s
}

func (s *Server) Ping(ctx context.Context, request *pb.PingRequest) (response *pb.PingResponse, err error) {
	if false {
		return nil, status.Errorf(codes.InvalidArgument,
			"something is not right")
	}

	return &pb.PingResponse{}, nil
}

func UnimplementedCtlServer() {
}

func (s *Server) SetupCredentials(ns, serviceCertPath, dbCertPath string) (err error) {
	svcCreds = config.ServiceCredentials{
		ServiceCrt: serviceCertPath + "/service.crt",
		ServiceKey: serviceCertPath + "/service.key",
	}

	// churro-ctl uses the database root credentials
	dbCreds = config.DBCredentials{
		SSLRootCertPath: dbCertPath + "/ca.crt",
		SSLKeyPath:      dbCertPath + "/client.root.key",
		SSLCertPath:     dbCertPath + "/client.root.crt",
	}

	userDBCreds = config.DBCredentials{
		SSLRootCertPath: dbCertPath + "/ca.crt",
		SSLKeyPath:      dbCertPath + "/client." + ns + ".key",
		SSLCertPath:     dbCertPath + "/client." + ns + ".crt",
	}

	err = svcCreds.Validate()
	if err != nil {
		s.logger.Error("error in svccreds validate", zap.Error(err))
		return err
	}

	err = dbCreds.Validate()
	if err != nil {
		s.logger.Error("error in dbcreds validte", zap.Error(err))
		return err
	}

	return nil
}

/**
func StartCtlServer(pi v1alpha1.Pipeline, debug bool, ns, serviceCertPath, dbCertPath string) {
	fmt.Printf("starting %s ctl-server on port %d\n", pi.ObjectMeta.Name, pi.Spec.Port)

	logger, _ := zap.NewDevelopment()

	server := NewCtlServer(ns, debug, serviceCertPath, dbCertPath, pi, logger)

	lis, err := net.Listen("tcp", ":"+strconv.Itoa(pi.Spec.Port))
	if err != nil {
		fmt.Printf("failed to listen %s\n", err.Error())
		return
	}

	//s := grpc.NewServer(grpc.Creds(creds))
	s := grpc.NewServer()

	pb.RegisterCtlServer(s, server)
	if err := s.Serve(lis); err != nil {
		fmt.Printf("failed to serve %s\n", err.Error())
		return
	}

}
*/
