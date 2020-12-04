package extract

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/360EntSecGroup-Skylar/excelize/v2"
	"go.uber.org/zap"
	"os"
	"time"

	"gitlab.com/churro-group/churro/internal/churrodata"
	"gitlab.com/churro-group/churro/internal/config"
	"gitlab.com/churro-group/churro/internal/dataprov"
	"gitlab.com/churro-group/churro/internal/loader"
)

// Extract an Excel file contents and exit
func (s *Server) ExtractXLS(ctx context.Context) (err error) {

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	fmt.Printf("ExtractXLS starting...\n")

	xlsxFile, err := excelize.OpenFile(s.FileName)
	if err != nil {
		s.logger.Error("could not open xlsx file", zap.String("file", s.FileName))
		return err
	}

	// TODO make sheetName configurable
	sheetName := "Sheet1"

	dp := dataprov.DataProvenance{}
	dp.Name = s.FileName
	dp.Path = s.FileName
	err = dataprov.Register(&dp, s.Pi, s.DBCreds, s.logger)
	if err != nil {
		s.logger.Error("can not register data prov")
		os.Exit(1)
	}
	s.logger.Info("dp info", zap.String("name", dp.Name), zap.String("path", dp.Path))

	var rows [][]string
	rows, err = xlsxFile.GetRows(sheetName)
	if err != nil {
		s.logger.Error("could not GetRows xlsx file", zap.Error(err), zap.String("sheetName", sheetName))
		return err
	}

	go s.pushToLoader(ctx, config.XLSXScheme)

	time.Sleep(time.Second * time.Duration(sleepTime))

	xlsStruct := churrodata.XLSFormat{}
	xlsStruct.Path = s.FileName
	xlsStruct.Dataprov = dp.Id
	xlsStruct.PipelineName = s.Pi.Name
	xlsStruct.ColumnNames = make([]string, 0)
	xlsStruct.ColumnTypes = make([]string, 0)

	firstRow := true
	// for now, only a single row per Queue message
	xlsStruct.Records = make([]churrodata.XLSRow, 0)

	for r := 0; r < len(rows); r++ {
		// back-pressure check
		if backPressure == 1 {
			fmt.Printf("sleeping due to backpressure...")
			time.Sleep(time.Second * time.Duration(sleepTime))
			continue
		}

		record := rows[r]

		// process the xls header which we expect to be there
		if firstRow {
			firstRow = false
			cols, err := xlsxFile.GetCols(sheetName)
			if err != nil {
				return err
			}
			colLen := len(cols)
			fmt.Printf("xlsx file has %d columns\n", colLen)
			xlsStruct.ColumnNames = genColumnNames(colLen)
			for i := 0; i < colLen; i++ {
				xlsStruct.ColumnTypes = append(xlsStruct.ColumnTypes, "TEXT")
			}
			err = s.tableCheck(xlsStruct.ColumnNames, xlsStruct.ColumnTypes)
			if err != nil {
				return err
			}
			xlsStruct.Tablename = s.TableName
		} else {
			// TODO apply transforms to XLS data
			/**
			fmt.Printf("transform rules %v\n", s.Pi.Spec.TransformConfig.Rules)

			err := transform.RunRules(scheme, xlsStruct.ColumnNames, record, s.Pi.Spec.TransformConfig.Rules, s.Pi.Spec.TransformConfig.Functions)
			if err != nil {
				log.Error(err.Error())
			}
			*/

			r := getXLSRow(record)
			xlsStruct.Records = append(xlsStruct.Records, r)
			s.logger.Debug("xls record read")
			if s.Queue == nil {
				s.logger.Debug("Info: queue is nil")
			}
			s.logger.Debug("queue len", zap.Int("queue len", len(s.Queue)))

			if len(xlsStruct.Records) >= RecordsPerPush {
				s.logger.Info("pushing to Queue")
				//convert xlsStruct into []byte
				xlsBytes, _ := json.Marshal(xlsStruct)
				msg := loader.LoaderMessage{}
				msg.Metadata = xlsBytes
				msg.DataFormat = config.XLSXScheme
				s.Queue <- msg
				xlsStruct.Records = make([]churrodata.XLSRow, 0)
			}
		}

	}

	if len(xlsStruct.Records) > 0 {
		xlsBytes, _ := json.Marshal(xlsStruct)
		msg := loader.LoaderMessage{}
		msg.Metadata = xlsBytes
		msg.DataFormat = config.XLSXScheme
		s.Queue <- msg
	}

	s.logger.Info("end of xlsx file reached, cancelling pushes...")
	time.Sleep(time.Second * 10)

	return err
}

func getXLSRow(record []string) churrodata.XLSRow {
	xlsRow := churrodata.XLSRow{}
	xlsRow.Cols = make([]string, len(record))
	copy(xlsRow.Cols, record)

	return xlsRow
}

func genColumnNames(colCount int) []string {
	cols := make([]string, 0)
	charsetCount := 24
	startingChar := 'A'
	currentChar := startingChar
	prefixChar := startingChar
	currentprefix := ""
	charsDone := 0

	for i := 0; i < colCount; i++ {
		if charsDone >= charsetCount {
			currentprefix = string(prefixChar)
			prefixChar = prefixChar + 1
			startingChar = startingChar
			charsDone = 0
			currentChar = rune(int(startingChar))
		}
		asInt := int(currentChar)
		cols = append(cols, currentprefix+string(asInt))
		currentChar = rune(int(currentChar) + 1)
		charsDone++
	}
	return cols
}
