package extract

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"gitlab.com/churro-group/churro/internal/churrodata"
	"gitlab.com/churro-group/churro/internal/config"
	"gitlab.com/churro-group/churro/internal/dataprov"
	"gitlab.com/churro-group/churro/internal/loader"
	"gitlab.com/churro-group/churro/internal/transform"
)

// Extract a CSV file contents and exit
func (s *Server) ExtractCSV(ctx context.Context) (err error) {

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	s.logger.Info("ExtractCSV starting...")

	csvfile, err := os.Open(s.FileName)
	if err != nil {
		s.logger.Errorf("could not open csv file %s %s\n", s.FileName, err.Error())
		return err
	}

	dp := dataprov.DataProvenance{Name: s.FileName, Path: s.FileName}
	err = dataprov.Register(&dp, s.Pi, s.DBCreds, s.logger)
	if err != nil {
		s.logger.Errorf("can not register data prov %s\n", err.Error())
		os.Exit(1)
	}
	s.logger.Infof("dp info %s\n", fmt.Sprintf("%v", dp))

	r := csv.NewReader(csvfile)

	go s.pushToLoader(ctx, config.CSVScheme)

	time.Sleep(time.Second * time.Duration(sleepTime))

	csvStruct := churrodata.CSVFormat{}
	csvStruct.Path = s.FileName
	csvStruct.Dataprov = dp.Id
	csvStruct.PipelineName = s.Pi.Name
	csvStruct.ColumnNames = make([]string, 0)
	csvStruct.ColumnTypes = make([]string, 0)

	firstRow := true
	// for now, only a single row per Queue message
	csvStruct.Records = make([]churrodata.CSVRow, 0)

	for {
		// back-pressure check
		if backPressure == 1 {
			s.logger.Info("sleeping due to backpressure...")
			time.Sleep(time.Second * time.Duration(sleepTime))
			continue
		}

		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		// process the csv header which we expect to be there
		if firstRow {
			firstRow = false
			for i := 0; i < len(record); i++ {
				csvStruct.ColumnNames = append(csvStruct.ColumnNames, strings.Trim(record[i], "\t \n"))
				csvStruct.ColumnTypes = append(csvStruct.ColumnTypes, "TEXT")
			}
			err := s.tableCheck(csvStruct.ColumnNames, csvStruct.ColumnTypes)
			if err != nil {
				return err
			}
			csvStruct.Tablename = s.TableName
		} else {
			fmt.Printf("transform rules %v\n", s.TransformRules)

			err := transform.RunRules(config.CSVScheme, csvStruct.ColumnNames, record, s.TransformRules, s.TransformFunctions, s.logger)
			if err != nil {
				s.logger.Errorf("error in runRules %s\n", err.Error())
			}

			r := getCSVRow(record)
			csvStruct.Records = append(csvStruct.Records, r)
			s.logger.Debugf("csv record read %s\n", fmt.Sprintf("%v", record))
			if s.Queue == nil {
				s.logger.Debug("Info: queue is nil")
			}
			s.logger.Debugf("queue len %d\n", len(s.Queue))

			if len(csvStruct.Records) >= RecordsPerPush {
				s.logger.Info("pushing to Queue")
				//convert csvStruct into []byte
				csvBytes, _ := json.Marshal(csvStruct)
				msg := loader.LoaderMessage{
					Metadata:   csvBytes,
					DataFormat: config.CSVScheme,
				}
				s.Queue <- msg
				csvStruct.Records = make([]churrodata.CSVRow, 0)
			}
		}

	}

	if len(csvStruct.Records) > 0 {
		csvBytes, _ := json.Marshal(csvStruct)
		msg := loader.LoaderMessage{
			Metadata:   csvBytes,
			DataFormat: config.CSVScheme,
		}
		s.Queue <- msg
	}

	s.logger.Info("end of CSV file reached, cancelling pushes...")
	time.Sleep(time.Second * 10)

	return err
}

func getCSVRow(record []string) churrodata.CSVRow {
	csvRow := churrodata.CSVRow{Cols: make([]string, len(record))}
	copy(csvRow.Cols, record)

	return csvRow
}
