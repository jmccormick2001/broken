package extract

import (
	"context"
	"encoding/json"
	"fmt"
	"go.uber.org/zap"
	"io/ioutil"
	"os"
	"time"

	"gitlab.com/churro-group/churro/internal/churrodata"
	"gitlab.com/churro-group/churro/internal/config"
	"gitlab.com/churro-group/churro/internal/dataprov"
	"gitlab.com/churro-group/churro/internal/loader"
)

// Extract a JSON file contents and exit
// this file is expected to be a single JSON document
func (s *Server) ExtractJSON(ctx context.Context) (err error) {

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	fmt.Printf("ExtractJSON starting...\n")

	jsonfile, err := os.Open(s.FileName)
	if err != nil {
		return fmt.Errorf("could not open JSON file: %s %v\n", s.FileName, err)
	}
	defer jsonfile.Close()

	dp := dataprov.DataProvenance{}
	dp.Name = s.FileName
	dp.Path = s.FileName
	err = dataprov.Register(&dp, s.Pi, s.DBCreds, s.logger)
	if err != nil {
		return fmt.Errorf("can not register data prov %v %v", dp, err)
	}
	s.logger.Debug("dp info", zap.String("dp", fmt.Sprintf("%v", dp)))

	var byteValue []byte
	byteValue, err = ioutil.ReadAll(jsonfile)
	if err != nil {
		return fmt.Errorf("can not read json input file %v", err)
	}
	var result map[string]interface{}
	err = json.Unmarshal([]byte(byteValue), &result)
	if err != nil {
		return fmt.Errorf("can not unmarshal json input file %v", err)
	}

	go s.pushToLoader(ctx, config.JSONScheme)

	time.Sleep(time.Second * time.Duration(sleepTime))

	jsonStruct := churrodata.IntermediateFormat{}
	jsonStruct.Path = dp.Path
	jsonStruct.Dataprov = dp.Id
	jsonStruct.ColumnNames = make([]string, 0)
	jsonStruct.ColumnTypes = make([]string, 0)
	jsonStruct.Messages = make([]map[string]interface{}, 0)
	jsonStruct.Messages = append(jsonStruct.Messages, result)

	someBytes, _ := json.Marshal(jsonStruct)

	s.Queue <- loader.LoaderMessage{Metadata: someBytes, DataFormat: config.JSONScheme}

	for {

		// back-pressure check
		if backPressure == 1 {
			s.logger.Debug("sleeping due to backpressure...")
			time.Sleep(time.Second * time.Duration(sleepTime))
			continue
		} else {
			break
		}

	}

	return err
}
