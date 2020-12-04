package main

import (
	"flag"
	"net"
	"os"

	"gitlab.com/churro-group/churro/internal"
	cfg "gitlab.com/churro-group/churro/internal/config"
	"gitlab.com/churro-group/churro/internal/loader"
	pb "gitlab.com/churro-group/churro/rpc/loader"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

var logger *zap.SugaredLogger

func main() {
	l, _ := zap.NewDevelopment()
	logger = l.Sugar()
	logger.Info("churro-loader")

	debug := flag.Bool("debug", false, "debug flag")
	serviceCertPath := flag.String("servicecert", "", "path to service creds")
	dbCertPath := flag.String("dbcert", "", "path to database cert files")

	flag.Parse()

	_, err := os.Stat(*dbCertPath)
	if err != nil {
		logger.Errorf("can not find dbCertPath %s\n", err.Error())
		os.Exit(1)
	}
	pipeline := os.Getenv("CHURRO_PIPELINE")
	if pipeline == "" {
		logger.Errorf("can not get CHURRO_PIPELINE env var CHURRO_PIPELINE env var is required\n")
		os.Exit(1)
	}

	svcCreds := cfg.ServiceCredentials{
		ServiceCrt: *serviceCertPath + "/service.crt",
		ServiceKey: *serviceCertPath + "/service.key",
	}

	dbCreds := cfg.DBCredentials{
		SSLRootCertPath: *dbCertPath + "/ca.crt",
		SSLKeyPath:      *dbCertPath + "/client." + pipeline + ".key",
		SSLCertPath:     *dbCertPath + "/client." + pipeline + ".crt",
	}

	err = svcCreds.Validate()
	if err != nil {
		logger.Errorf("can not validate svc creds %s", err.Error())
		os.Exit(1)
	}
	err = dbCreds.Validate()
	if err != nil {
		logger.Errorf("can not validate db creds %s", err.Error())
		os.Exit(1)
	}

	creds, err := credentials.NewServerTLSFromFile(svcCreds.ServiceCrt, svcCreds.ServiceKey)
	if err != nil {
		logger.Errorf("failed to setup TLS %s\n", err.Error())
		os.Exit(1)
	}

	pi, err := internal.GetPipeline(logger)
	if err != nil {
		logger.Errorf("failed to get pipeline %s\n", pi.Name)
		os.Exit(1)
	}

	server := loader.NewLoaderServer(*debug, svcCreds, dbCreds, pi, logger)

	lis, err := net.Listen("tcp", loader.DEFAULT_PORT)
	if err != nil {
		logger.Errorf("failed to listen: %s\n", err.Error())
		os.Exit(1)
	}

	s := grpc.NewServer(grpc.Creds(creds))

	pb.RegisterLoaderServer(s, server)

	if err := s.Serve(lis); err != nil {
		logger.Errorf("failed to serve %s\n", err.Error())
		os.Exit(1)
	}

}
