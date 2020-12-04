package main

import (
	"flag"
	"net"
	"os"

	"gitlab.com/churro-group/churro/internal"
	"gitlab.com/churro-group/churro/internal/config"
	cfg "gitlab.com/churro-group/churro/internal/config"
	"gitlab.com/churro-group/churro/internal/watch"
	pb "gitlab.com/churro-group/churro/rpc/watch"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

var logger *zap.SugaredLogger

func main() {
	l, _ := zap.NewDevelopment()
	defer l.Sync()

	logger = l.Sugar()

	debugFlag := flag.Bool("debug", false, "debug logging")

	serviceCertPath := flag.String("servicecert", "", "path to service creds")
	dbCertPath := flag.String("dbcert", "", "path to database cert files (e.g. ca.crt)")

	flag.Parse()

	svcCreds := cfg.ServiceCredentials{
		ServiceCrt: *serviceCertPath + "/service.crt",
		ServiceKey: *serviceCertPath + "/service.key",
	}
	err := svcCreds.Validate()
	if err != nil {
		logger.Errorf("error in Validate %s\n", err.Error())
		os.Exit(1)
	}

	ns := os.Getenv("CHURRO_NAMESPACE")
	if ns == "" {
		logger.Error("CHURRO_NAMESPACE env var is required")
		os.Exit(1)
	}

	_, err = os.Stat(*dbCertPath)
	if err != nil {
		logger.Errorf("error in dbCertPath %s\n", err.Error())
		os.Exit(1)
	}

	dbCreds := config.DBCredentials{
		SSLRootCertPath: *dbCertPath + "/ca.crt",
		SSLKeyPath:      *dbCertPath + "/client.root.key",
		SSLCertPath:     *dbCertPath + "/client.root.crt",
	}

	userDBCreds := cfg.DBCredentials{
		SSLRootCertPath: *dbCertPath + "/ca.crt",
		SSLKeyPath:      *dbCertPath + "/client." + ns + ".key",
		SSLCertPath:     *dbCertPath + "/client." + ns + ".crt",
	}

	creds, err := credentials.NewServerTLSFromFile(svcCreds.ServiceCrt, svcCreds.ServiceKey)
	if err != nil {
		logger.Errorf("failed to setup TLS %s\n", err.Error())
		os.Exit(1)
	}

	pi, err := internal.GetPipeline(logger)
	if err != nil {
		logger.Errorf("pipeline not found %s %s\n", err.Error(), pi.Name)
		os.Exit(1)
	}

	server := watch.NewWatchServer(*debugFlag, svcCreds, pi, logger, userDBCreds, dbCreds)

	lis, err := net.Listen("tcp", watch.DEFAULT_PORT)
	if err != nil {
		logger.Errorf("failed to listen %s\n", err.Error())
		os.Exit(1)
	}

	s := grpc.NewServer(grpc.Creds(creds))

	pb.RegisterWatchServer(s, server)
	if err := s.Serve(lis); err != nil {
		logger.Errorf("failed to serve %s\n", err.Error())
		os.Exit(1)
	}

}
