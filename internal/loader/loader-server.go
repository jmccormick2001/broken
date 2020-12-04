// Package loader holds the churro loader service implementation
package loader

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/golang/snappy"
	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"gitlab.com/churro-group/churro/api/v1alpha1"
	"gitlab.com/churro-group/churro/internal/backpressure"
	"gitlab.com/churro-group/churro/internal/churrodata"
	"gitlab.com/churro-group/churro/internal/config"
	"gitlab.com/churro-group/churro/internal/stats"
	pb "gitlab.com/churro-group/churro/rpc/loader"
	"go.uber.org/zap"
)

const (
	DEFAULT_PORT = ":8083"
)

var filesProcessedMetric prometheus.Counter

var recordsInput int32
var backPressure int32

type LoaderMessage struct {
	Metadata   []byte
	DataFormat string
}

// Server implements the Loader service
type Server struct {
	logger       *zap.SugaredLogger
	Config       config.ChurroConfig
	Pi           v1alpha1.Pipeline
	Queue        chan LoaderMessage
	ServiceCreds config.ServiceCredentials
	DBCreds      config.DBCredentials
}

// NewLoaderServer constructs a loader server based on the passed
// configuration, a pointer to the server is returned
func NewLoaderServer(debug bool, svcCreds config.ServiceCredentials, dbCreds config.DBCredentials, pipeline v1alpha1.Pipeline, l *zap.SugaredLogger) *Server {
	s := &Server{
		logger:       l,
		ServiceCreds: svcCreds,
		DBCreds:      dbCreds,
		Pi:           pipeline,
	}

	s.Queue = make(chan LoaderMessage, 32)

	filesProcessedMetric = promauto.NewCounter(prometheus.CounterOpts{
		Name:        "churro_processed_files_totals",
		Help:        "the total number of processed files for this pipeline",
		ConstLabels: prometheus.Labels{"pipeline": s.Pi.Name},
	})

	go s.pushToDataStore()

	go s.startMetrics()

	return s
}

// Ping is the ping implementation that satisfies the rpc interface, a simple
// response is returned that holds the current backpressure status
func (s *Server) Ping(ctx context.Context, request *pb.PingRequest) (response *pb.PingResponse, err error) {
	return &pb.PingResponse{
		Backpressure: backPressure,
	}, nil
}

// FileProcessed
func (s *Server) FileProcessed(ctx context.Context, request *pb.FileProcessedRequest) (response *pb.FileProcessedResponse, err error) {

	filesProcessedMetric.Add(1)

	return &pb.FileProcessedResponse{
		Backpressure: backPressure,
	}, nil
}

// Push is the push implementation that satisfies the rpc interface, a simple
// response is returned that holds the current backpressure status, as
// push is called, messages in the request are added to an internal queue
// for processing (loading into a data store).
func (s *Server) Push(ctx context.Context, msg *pb.PushRequest) (response *pb.PushResponse, err error) {
	s.logger.Info("Loader Push received")

	recordsInput++
	decoded, err := snappy.Decode(nil, msg.MessageCompressed)
	if err != nil {
		return nil, err
	}
	s.Queue <- LoaderMessage{Metadata: decoded, DataFormat: msg.DataFormat}

	backPressure = backpressure.CheckBackpressure(len(s.Queue), s.Config.LoaderConfig.QueueSize, s.Config.LoaderConfig.PctHeadRoom, s.logger)

	return &pb.PushResponse{
		Backpressure: backPressure,
	}, nil
}

func (s *Server) pushToDataStore() {
	//TODO cache the client globally
	//TODO build the URL from the config

	dbname := s.Pi.Spec.DataSource.Database

	url := s.DBCreds.GetDBConnectString(s.Pi.Spec.DataSource)
	s.logger.Infof("url %s\n", url)

	db, err := sql.Open("postgres", url)
	if err != nil {
		s.logger.Errorf("error connecting to the database: %s\n", err.Error())
		return
	}
	defer db.Close()

	for elem := range s.Queue {

		s.logger.Infof("loader has dataformat in the queue %s\n", elem.DataFormat)
		switch elem.DataFormat {
		case config.CSVScheme:
			s.processCSV(db, dbname, elem)
		case config.XLSXScheme:
			s.processXLS(db, dbname, elem)
		case config.JSONScheme:
			s.processJSON(db, s.Pi.Name, elem)
		case config.JSONPathScheme:
			s.processJSONPath(db, s.Pi.Name, elem)
		case config.XMLScheme:
			s.processXML(db, dbname, elem)
		case config.FinnHubScheme:
			s.processFinnhubStocks(db, dbname, elem)
		default:
			s.logger.Errorf("scheme not recoginized %s", elem.DataFormat)
		}
	}
}

func (s *Server) processCSV(db *sql.DB, database string, elem LoaderMessage) {

	//unmarshal elem metadata into CSV message
	var csvMsg churrodata.CSVFormat
	err := json.Unmarshal(elem.Metadata, &csvMsg)
	if err != nil {
		s.logger.Errorf("error in unmarshal %s", err.Error())
		return
	}

	for _, r := range csvMsg.Records {
		csvsql := getInsertStatement(config.CSVScheme, database, csvMsg.Tablename, csvMsg.ColumnNames, r.Cols)
		s.logger.Infof("csvsql %s", csvsql)

		_, err := db.Query(csvsql)
		if err != nil {
			s.logger.Errorf("error in query %s %s\n", csvsql, err.Error())
			return
		}
	}

	t := stats.PipelineStats{
		DataprovId: csvMsg.Dataprov,
		Pipeline:   csvMsg.PipelineName,
		FileName:   csvMsg.Path,
		RecordsIn:  int64(len(csvMsg.Records)),
	}

	err = stats.Update(db, t, s.logger)
	if err != nil {
		s.logger.Errorf("error in stats update %s\n", err.Error())
		return
	}
}

func (s *Server) processJSON(db *sql.DB, pipelineName string, elem LoaderMessage) {
	sql := fmt.Sprintf("INSERT into %s.churroformat (dataformat, metadata, createdtime) values ($1, $2, now())", pipelineName)
	insertStmt, err := db.Prepare(sql)
	if err != nil {
		s.logger.Errorf("error in sql prepare %s %s\n", sql, err.Error())
	}
	defer insertStmt.Close()
	s.logger.Infof("loader sql %s\n", sql)
	if _, err := insertStmt.Exec(elem.DataFormat, elem.Metadata); err != nil {
		s.logger.Errorf("error in insert %s\n", err.Error())
	}

}

// GetStats implements the GetStats rpc interface, and simply returns
// the number of records processed as a status
func (s *Server) GetStats(ctx context.Context, msg *pb.StatsRequest) (response *pb.StatsResponse, err error) {
	s.logger.Info("Loader GetStats received")
	return &pb.StatsResponse{Recordsin: recordsInput}, nil
}

func getInsertStatement(scheme, database, tablename string, cols []string, vals []string) string {

	var b string
	for _, v := range cols {
		b = b + fmt.Sprintf("%s,", v)
	}
	var c string
	for _, v := range vals {
		c = c + fmt.Sprintf("'%s',", v)
	}

	csvsql := fmt.Sprintf("insert into %s.%s (dataformat, %s createdtime) values ('%s', %s now())", database, tablename, b, scheme, c)

	return csvsql
}

func (s *Server) startMetrics() {
	http.Handle("/metrics", promhttp.Handler())
	err := http.ListenAndServe(":2112", nil)
	if err != nil {
		s.logger.Errorf("error in startMetrics %s\n", err.Error())
	}
}

func (s *Server) processXML(db *sql.DB, database string, elem LoaderMessage) {

	//unmarshal elem metadata into XML message
	var xmlMsg churrodata.XMLFormat
	err := json.Unmarshal(elem.Metadata, &xmlMsg)
	if err != nil {
		s.logger.Errorf("error in processXML %s\n", err.Error())
		return
	}

	s.logger.Infof("loader is processing XML records %d\n", len(xmlMsg.Records))
	s.logger.Infof("loader is processing XML columns %s\n", xmlMsg.ColumnNames)
	for _, r := range xmlMsg.Records {
		xmlsql := getInsertStatement(config.XMLScheme, database, xmlMsg.Tablename, xmlMsg.ColumnNames, r.Cols)
		s.logger.Infof("xmlsql %s", xmlsql)

		_, err := db.Query(xmlsql)
		if err != nil {
			s.logger.Errorf("error in query %s\n", err.Error())
			return
		}
	}

	t := stats.PipelineStats{
		DataprovId: xmlMsg.Dataprov,
		Pipeline:   xmlMsg.PipelineName,
		FileName:   xmlMsg.Path,
		RecordsIn:  int64(len(xmlMsg.Records)),
	}

	err = stats.Update(db, t, s.logger)
	if err != nil {
		s.logger.Errorf("error on stats update %s\n", err.Error())
	}
}

func (s *Server) processFinnhubStocks(db *sql.DB, database string, elem LoaderMessage) {

	//unmarshal elem metadata into CSV message
	var csvMsg churrodata.CSVFormat
	err := json.Unmarshal(elem.Metadata, &csvMsg)
	if err != nil {
		s.logger.Errorf("error on csv unmarshal %s\n", err.Error())
		return
	}

	for _, r := range csvMsg.Records {
		csvsql := getInsertStatement(config.FinnHubScheme, database, csvMsg.Tablename, csvMsg.ColumnNames, r.Cols)
		s.logger.Infof("finnhub-stocks sql %s", csvsql)

		_, err := db.Query(csvsql)
		if err != nil {
			s.logger.Errorf("erro in db query %s %s", csvsql, err.Error())
			return
		}
	}

	t := stats.PipelineStats{
		DataprovId: csvMsg.Dataprov,
		Pipeline:   csvMsg.PipelineName,
		FileName:   csvMsg.Path,
		RecordsIn:  int64(len(csvMsg.Records)),
	}

	err = stats.Update(db, t, s.logger)
	if err != nil {
		s.logger.Errorf("error in stats update %s\n", err.Error())
	}
}

func (s *Server) processXLS(db *sql.DB, database string, elem LoaderMessage) {

	//unmarshal elem metadata into XLS message
	var xlsMsg churrodata.XLSFormat
	err := json.Unmarshal(elem.Metadata, &xlsMsg)
	if err != nil {
		s.logger.Errorf("error in xls unmarshal %s\n", err.Error())
		return
	}

	for _, r := range xlsMsg.Records {
		// use the CSV insert statement for the XLS scheme
		xlssql := getInsertStatement(config.XLSXScheme, database, xlsMsg.Tablename, xlsMsg.ColumnNames, r.Cols)
		s.logger.Infof("xlssql %s\n", xlssql)

		_, err := db.Query(xlssql)
		if err != nil {
			s.logger.Errorf("error in xls query %s\n", err.Error())
			return
		}
	}

	t := stats.PipelineStats{
		DataprovId: xlsMsg.Dataprov,
		Pipeline:   xlsMsg.PipelineName,
		FileName:   xlsMsg.Path,
		RecordsIn:  int64(len(xlsMsg.Records)),
	}

	err = stats.Update(db, t, s.logger)
	if err != nil {
		s.logger.Errorf("error in stats update %s\n", err.Error())
		return
	}
}

func (s *Server) processJSONPath(db *sql.DB, database string, elem LoaderMessage) {

	//unmarshal into JsonPathMessage
	var jsonPathMsg churrodata.JsonPathFormat
	err := json.Unmarshal(elem.Metadata, &jsonPathMsg)
	if err != nil {
		s.logger.Errorf("error in jsonpath unmarshal %s\n", err.Error())
		return
	}

	s.logger.Infof("jsonPathMsg %+v\n", jsonPathMsg)

	var recordsProcessed int64

	for r := 0; r < len(jsonPathMsg.Records); r++ {
		record := jsonPathMsg.Records[r]
		s.logger.Infof("r.Cols %v\n", record.Cols)
		if len(record.Cols) > 0 {
			jsonpathsql := getInsertStatement(config.JSONPathScheme, database, jsonPathMsg.Tablename, jsonPathMsg.ColumnNames, record.Cols)
			s.logger.Info("jsonpathsql %s\n", jsonpathsql)

			_, err := db.Query(jsonpathsql)
			if err != nil {
				s.logger.Errorf("error in jsonpath query %s\n", err.Error())
				return
			}
			recordsProcessed++
		}
	}

	t := stats.PipelineStats{
		DataprovId: jsonPathMsg.Dataprov,
		Pipeline:   jsonPathMsg.PipelineName,
		FileName:   jsonPathMsg.Path,
		RecordsIn:  recordsProcessed,
	}

	err = stats.Update(db, t, s.logger)
	if err != nil {
		s.logger.Errorf("error in jsonpath stats update %s\n", err.Error())
		return
	}

}
