package extract

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"go.uber.org/zap"

	"gitlab.com/churro-group/churro/internal/churrodata"
	"gitlab.com/churro-group/churro/internal/config"
	"gitlab.com/churro-group/churro/internal/dataprov"
	"gitlab.com/churro-group/churro/internal/loader"
	"gitlab.com/churro-group/churro/internal/watch"

	"github.com/ohler55/ojg/jp"
	"github.com/ohler55/ojg/oj"
)

// Extract a JSON file contents using jsonpath rules and exit
// this file is expected to be a single JSON document
func (s *Server) ExtractJSONPath(ctx context.Context) (err error) {

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	s.logger.Debug("ExtractJSONPath starting...")

	byteValue, err := ioutil.ReadFile(s.FileName)
	if err != nil {
		return fmt.Errorf("could not open JSONPath file: %s %v\n", s.FileName, err)
	}

	obj, parseError := oj.ParseString(string(byteValue))
	if parseError != nil {
		return fmt.Errorf("error parsing rule: %s %v\n", string(byteValue), err)
	}
	dp := dataprov.DataProvenance{}
	dp.Name = s.FileName
	dp.Path = s.FileName
	err = dataprov.Register(&dp, s.Pi, s.DBCreds, s.logger)
	if err != nil {
		return fmt.Errorf("can not register data prov %v %v", dp, err)
	}
	s.logger.Debug("dp info", zap.String("dp", fmt.Sprintf("%v", dp)))

	go s.pushToLoader(ctx, config.JSONPathScheme)

	time.Sleep(time.Second * time.Duration(sleepTime))

	jsonStruct := churrodata.JsonPathFormat{}
	jsonStruct.Path = dp.Path
	jsonStruct.Dataprov = dp.Id
	jsonStruct.PipelineName = s.Pi.Name
	jsonStruct.ColumnNames = make([]string, 0)
	jsonStruct.ColumnTypes = make([]string, 0)

	// since we assume a single json message, we extract that
	// into a single record with multiple columns
	jsonStruct.Records = make([]churrodata.JsonPathRow, 1)

	watchDirName := os.Getenv("CHURRO_WATCHDIR_NAME")
	if watchDirName == "" {
		return fmt.Errorf("CHURRO_WATCHDIR_NAME is not set")
	}

	// get the watch directories
	pgConnectString := s.DBCreds.GetDBConnectString(s.Pi.Spec.DataSource)
	s.logger.Info("extract db creds", zap.String("pgConnectString", pgConnectString))
	db, err := sql.Open("postgres", pgConnectString)
	if err != nil {
		s.logger.Error("could not open the database: ", zap.Error(err))
		os.Exit(1)
	}

	var wdirs []watch.WatchDirectory
	wdirs, err = watch.GetWatchDirectories(db)
	if err != nil {
		return fmt.Errorf(err.Error())
	}
	db.Close()
	s.logger.Info("watch directories", zap.Int("count", len(wdirs)))

	var rules []watch.ExtractRule
	jsonStruct.ColumnNames, jsonStruct.ColumnTypes, rules = getRules(watchDirName, wdirs)
	s.logger.Info("found rules ", zap.Int("count", len(rules)))

	allCols := make([][]string, 0)

	jsonStruct.Tablename = s.TableName
	err = s.tableCheck(jsonStruct.ColumnNames, jsonStruct.ColumnTypes)
	if err != nil {
		return err
	}

	var rows int
	for r := 0; r < len(rules); r++ {
		cols, err := getColumns(obj, rules[r].RuleSource)
		if err != nil {
			panic(err)
		}
		allCols = append(allCols, cols)
		rows = len(cols)
	}

	for row := 0; row < rows; row++ {
		r := churrodata.JsonPathRow{}
		r.Cols = make([]string, 0)
		for cell := 0; cell < len(allCols); cell++ {
			r.Cols = append(r.Cols, allCols[cell][row])
		}
		jsonStruct.Records = append(jsonStruct.Records, r)
	}

	someBytes, _ := json.Marshal(jsonStruct)

	fmt.Println("jeff pushing a message to the queue")
	s.Queue <- loader.LoaderMessage{Metadata: someBytes, DataFormat: config.JSONPathScheme}

	s.logger.Info("end of jsonpath file reached, cancelling pushes...")
	time.Sleep(time.Second * 10)

	return err
}

func getRules(watchDirName string, p []watch.WatchDirectory) ([]string, []string, []watch.ExtractRule) {
	cols := make([]string, 0)
	types := make([]string, 0)
	rules := make([]watch.ExtractRule, 0)

	for i := 0; i < len(p); i++ {
		if p[i].Name == watchDirName {
			for _, v := range p[i].ExtractRules {
				cols = append(cols, v.ColumnName)
				types = append(types, "TEXT")
				rules = append(rules, v)
			}
			return cols, types, rules
		}
	}

	return cols, types, rules
}

func getColumns(parsedFileBytes interface{}, jsonpath string) (columns []string, err error) {
	var x jp.Expr
	x, err = jp.ParseString(jsonpath)
	if err != nil {
		return columns, err
	}
	var cols []interface{}
	cols = x.Get(parsedFileBytes)

	for i := 0; i < len(cols); i++ {
		s := cols[i].(string)
		columns = append(columns, s)
	}
	return columns, nil
}
