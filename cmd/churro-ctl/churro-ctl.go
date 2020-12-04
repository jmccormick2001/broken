package main

import (
	"flag"
	"net"
	"os"

	"gitlab.com/churro-group/churro/internal"
	"gitlab.com/churro-group/churro/internal/ctl"
	pb "gitlab.com/churro-group/churro/rpc/ctl"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

var logger *zap.SugaredLogger

func main() {
	l, _ := zap.NewDevelopment()
	logger = l.Sugar()
	logger.Info("churro-ctl")

	debug := flag.Bool("debug", false, "debug flag")
	logger.Infof("debug set to %t\n", debug)
	serviceCertPath := flag.String("servicecert", "", "path to service cert files e.g. service.crt")
	dbCertPath := flag.String("dbcert", "", "path to database cert files (e.g. ca.crt)")

	flag.Parse()

	pipeline := os.Getenv("CHURRO_PIPELINE")
	if pipeline == "" {
		logger.Error("CHURRO_PIPELINE env var is required")
		os.Exit(1)
	}
	ns := os.Getenv("CHURRO_NAMESPACE")
	if ns == "" {
		logger.Error("CHURRO_NAMESPACE env var is required")
		os.Exit(1)
	}

	_, err := os.Stat(*dbCertPath)
	if err != nil {
		logger.Error("error in dbCertPath", zap.Error(err))
		os.Exit(1)
	}

	pi, err := internal.GetPipeline(logger)
	if err != nil {
		panic(err)
	}

	server := ctl.NewCtlServer(ns, true, *serviceCertPath, *dbCertPath, pi, logger)
	creds, err := credentials.NewServerTLSFromFile(server.ServiceCreds.ServiceCrt, server.ServiceCreds.ServiceKey)
	if err != nil {
		panic(err)
	}

	lis, err := net.Listen("tcp", ctl.DEFAULT_PORT)
	if err != nil {
		panic(err)
	}

	s := grpc.NewServer(grpc.Creds(creds))

	pb.RegisterCtlServer(s, server)
	if err := s.Serve(lis); err != nil {
		panic(err)
	}

}
