package main

import (
	"flag"
	"os"

	"go.uber.org/zap"

	"gitlab.com/churro-group/churro/internal"
	cfg "gitlab.com/churro-group/churro/internal/config"
	"gitlab.com/churro-group/churro/internal/extract"
)

//var queueMax = 100

var logger *zap.SugaredLogger

func main() {
	l, _ := zap.NewDevelopment()
	logger = l.Sugar()
	logger.Info("churro-extract")
	serviceCertPath := flag.String("servicecert", "", "path to service cert file e.g. service.crt")

	dbCertPath := flag.String("dbcert", "", "path to database cert files (e.g. ca.crt)")

	debug := flag.Bool("debug", false, "debug flag")

	flag.Parse()

	_, err := os.Stat(*dbCertPath)
	if err != nil {
		logger.Error("error dbCertPath", zap.Error(err))
		os.Exit(1)
	}
	pipeline := os.Getenv("CHURRO_NAMESPACE")
	if pipeline == "" {
		logger.Error("error CHURRO_NAMESPACE env var not set")
		os.Exit(1)
	}
	fileName := os.Getenv("CHURRO_FILENAME")
	if fileName == "" {
		logger.Error("error CHURRO_FILENAME not set")
		os.Exit(1)
	}
	schemeValue := os.Getenv("CHURRO_SCHEME")
	if schemeValue == "" {
		logger.Error("CHURRO_SCHEME env var is required")
		os.Exit(1)
	}
	tableName := os.Getenv("CHURRO_TABLENAME")
	if tableName == "" {
		logger.Error("CHURRO_TABLENAME env var is required")
		os.Exit(1)
	}
	watchDirName := os.Getenv("CHURRO_WATCHDIR_NAME")
	if watchDirName == "" {
		logger.Error("CHURRO_WATCHDIR_NAME env var is required")
		os.Exit(1)
	}

	logger.Infof("CHURRO_TABLENAME %s\n", tableName)
	logger.Infof("CHURRO_SCHEME %s\n", schemeValue)
	logger.Infof("CHURRO_FILENAME %s\n", fileName)
	logger.Infof("CHURRO_NAMESPACE %s\n", pipeline)
	logger.Infof("CHURRO_WATCHDIR_NAME %s\n", watchDirName)

	dbCreds := cfg.DBCredentials{
		SSLRootCertPath: *dbCertPath + "/ca.crt",
		SSLKeyPath:      *dbCertPath + "/client." + pipeline + ".key",
		SSLCertPath:     *dbCertPath + "/client." + pipeline + ".crt",
	}
	svcCreds := cfg.ServiceCredentials{
		ServiceCrt: *serviceCertPath + "/service.crt",
		ServiceKey: *serviceCertPath + "/service.key",
	}

	err = svcCreds.Validate()
	if err != nil {
		logger.Errorf("error svccreds validate %s\n", err.Error())
		os.Exit(1)
	}
	err = dbCreds.Validate()
	if err != nil {
		logger.Errorf("error dbcreds validate %s\n", err.Error())
		os.Exit(1)
	}

	pi, err := internal.GetPipeline(logger)
	if err != nil {
		logger.Errorf("could not get pipeline %s %s\n", pi.Name, err.Error())
		os.Exit(1)
	}

	extract.NewExtractServer(fileName, schemeValue, tableName, *debug, svcCreds, dbCreds, pi, logger)
	logger.Info("extract ending...")

}
