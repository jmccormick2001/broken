// Package extract holds the churro extract service implementation
package extract

import (
	"context"
	"fmt"
	"os"

	"go.uber.org/zap"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"database/sql"

	_ "github.com/lib/pq"
	"gitlab.com/churro-group/churro/api/v1alpha1"
	"gitlab.com/churro-group/churro/internal/config"
	"gitlab.com/churro-group/churro/internal/loader"
	"gitlab.com/churro-group/churro/internal/transform"
	"gitlab.com/churro-group/churro/internal/watch"
	pb "gitlab.com/churro-group/churro/rpc/extract"
	pbloader "gitlab.com/churro-group/churro/rpc/loader"
)

const (
	DEFAULT_PORT = ":8081"
)

var sleepTime = 3 //seconds to sleep when backpressure
var backPressure int32

type Server struct {
	Pi                 v1alpha1.Pipeline
	Queue              chan loader.LoaderMessage
	ServiceCreds       config.ServiceCredentials
	DBCreds            config.DBCredentials
	TableName          string
	SchemeValue        string
	FileName           string
	TransformFunctions []transform.TransformFunction
	TransformRules     []transform.TransformRule
	WatchDirectory     []watch.WatchDirectory
	logger             *zap.SugaredLogger
}

// NewExtractServer creates an extract server based on the configPath
// and returns a pointer to the extract server
func NewExtractServer(fileName, schemeValue, tableName string, debug bool, svcCreds config.ServiceCredentials, dbCreds config.DBCredentials, pipeline v1alpha1.Pipeline, l *zap.SugaredLogger) *Server {
	s := &Server{
		logger:       l,
		Queue:        make(chan loader.LoaderMessage, 32),
		ServiceCreds: svcCreds,
		DBCreds:      dbCreds,
		Pi:           pipeline,
		FileName:     fileName,
		SchemeValue:  schemeValue,
		TableName:    tableName,
	}

	var err error

	pgConnectString := dbCreds.GetDBConnectString(pipeline.Spec.DataSource)
	s.logger.Info("extract db creds", zap.String("pgConnectString", pgConnectString))
	db, err := sql.Open("postgres", pgConnectString)
	if err != nil {
		s.logger.Error("could not open the database: ", zap.Error(err))
		os.Exit(1)
	}

	//s.TransformFunctions, err = transform.GetTransformFunctions(db, s.Pi.Name)
	s.TransformFunctions, err = transform.GetTransformFunctions(db)
	//s.TransformRules, err = transform.GetTransformRules(db, s.Pi.Name)
	s.TransformRules, err = transform.GetTransformRules(db)
	if err != nil {
		s.logger.Errorf("could not get transform rules: %s\n", err.Error())
		os.Exit(1)
	}

	s.logger.Infof("transform functions %d\n", len(s.TransformFunctions))
	s.logger.Infof("transform rules %d\n", len(s.TransformRules))
	s.WatchDirectory, err = watch.GetWatchDirectories(db)
	if err != nil {
		s.logger.Errorf("could not get watch directories: %s\n", err.Error())
		os.Exit(1)
	}

	db.Close()

	s.logger.Debugf("NewExtractServer with scheme %s\n", schemeValue)

	// metric created
	url := fmt.Sprintf("%s:%d", s.Pi.Spec.LoaderConfig.Location.Host, s.Pi.Spec.LoaderConfig.Location.Port)

	creds, err := credentials.NewClientTLSFromFile(svcCreds.ServiceCrt, "")
	if err != nil {
		s.logger.Errorf("could not process the credentials: %s\n", err.Error())
		os.Exit(1)
	}

	conn, err := grpc.Dial(url, grpc.WithTransportCredentials(creds))
	if err != nil {
		s.logger.Errorf("did not connect: %s\n", err.Error())
		return s
	}
	defer conn.Close()
	loaderclient := pbloader.NewLoaderClient(conn)

	ctx := context.Background()

	_, err2 := loaderclient.FileProcessed(ctx, &pbloader.FileProcessedRequest{Filename: fileName})
	if err2 != nil {
		s.logger.Errorf("error in FileProcessed %s\n", err2.Error())
		return s
	}

	s.logger.Debug("NewExtractServer called processing started...")
	switch schemeValue {
	case config.FinnHubScheme:
		s.logger.Info("Info: extract is processing a finnhub-stocks config")
		err = s.ExtractFinnhubStocks(ctx)
		if err != nil {
			s.logger.Errorf("error in finnhub processing %s\n", err.Error())
		}
	case config.XMLScheme:
		s.logger.Info("Info: extract is processing a xml file")
		err = s.ExtractXML(ctx)
		if err != nil {
			s.logger.Errorf("error in xml processing %s\n", err.Error())
		}
	case config.CSVScheme:
		s.logger.Info("Info: extract is processing a CSV file")
		err = s.ExtractCSV(ctx)
		if err != nil {
			s.logger.Errorf("error in csv processing %s\n", err.Error())
		}
	case config.XLSXScheme:
		s.logger.Info("Info: extract is processing a xlsx file")
		err = s.ExtractXLS(ctx)
		if err != nil {
			s.logger.Errorf("error in xlsx processing %s\n", err.Error())
		}
	case config.JSONScheme:
		s.logger.Info("extract is processing a json file")
		err = s.ExtractJSON(ctx)
		if err != nil {
			s.logger.Errorf("error in json processing %s\n", err.Error())
		}
	case config.JSONPathScheme:
		s.logger.Info("extract is processing a jsonpath file")
		err = s.ExtractJSONPath(ctx)
		if err != nil {
			s.logger.Errorf("error in jsonpath processing %s\n", err.Error())
		}
	default:
		s.logger.Errorf("invalid datasource scheme value %s\n", schemeValue)
		os.Exit(1)
	}

	switch schemeValue {
	case config.XLSXScheme:
	case config.CSVScheme:
	case config.JSONPathScheme:
	case config.JSONScheme:
	case config.XMLScheme:
		s.renameFile(fileName)
	}

	return s
}

// Ping implements the Ping interface and simply responds by returning
// a response that also holds the current backpressure status
func (s *Server) Ping(ctx context.Context, size *pb.PingRequest) (hat *pb.PingResponse, err error) {
	return &pb.PingResponse{
		Backpressure: size.Backpressure,
	}, nil
}

func (s *Server) renameFile(path string) {
	newPath := path + ".churro-processed"
	err := os.Rename(path, newPath)
	if err != nil {
		s.logger.Errorf("error in renaming file %s\n", err.Error())
	}
	s.logger.Infof("extract is renaming processed file %s\n", newPath)
}
