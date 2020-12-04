package extract

import (
	"context"
	"encoding/json"
	"fmt"
	"go.uber.org/zap"
	"os"
	"time"

	"github.com/gorilla/websocket"
	"gitlab.com/churro-group/churro/internal/churrodata"
	"gitlab.com/churro-group/churro/internal/config"
	"gitlab.com/churro-group/churro/internal/dataprov"
	"gitlab.com/churro-group/churro/internal/loader"
	"gitlab.com/churro-group/churro/internal/transform"
)

type WebSocketData struct {
	Data []struct {
		LastPrice float64 `json:"p"`
		Symbol    string  `json:"s"`
		Timestamp int64   `json:"t"`
		Volume    float64 `json:"v"`
	} `json:"data"`
	Type string `json:"type"`
}

// Extract from a finnhub.io stocks wss feed forever
func (s *Server) ExtractFinnhubStocks(ctx context.Context) (err error) {

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	fmt.Printf("ExtractFinnhubStocks dialing...%s\n", s.FileName)

	// connect to finnhub
	w, _, err := websocket.DefaultDialer.Dial(s.FileName, nil)
	if err != nil {
		return err
	}
	defer w.Close()

	// subscribe to the wss feed
	// TODO get the stocks from the WatchSocket used in this config
	symbols := []string{"AAPL", "AMZN"}
	for _, s := range symbols {
		msg, _ := json.Marshal(map[string]interface{}{"type": "subscribe", "symbol": s})
		w.WriteMessage(websocket.TextMessage, msg)
	}

	// register to dataprov
	dp := dataprov.DataProvenance{}
	dp.Name = s.FileName
	dp.Path = s.FileName
	err = dataprov.Register(&dp, s.Pi, s.DBCreds, s.logger)
	if err != nil {
		s.logger.Error("can not register data prov", zap.Error(err))
		os.Exit(1)
	}
	s.logger.Info("dp info ", zap.String("dp", fmt.Sprintf("%+v", dp)))

	go s.pushToLoader(ctx, config.FinnHubScheme)

	time.Sleep(time.Second * time.Duration(sleepTime))

	csvStruct := churrodata.CSVFormat{}
	csvStruct.Path = s.FileName
	csvStruct.Dataprov = dp.Id
	csvStruct.Tablename = s.TableName
	csvStruct.PipelineName = s.Pi.Name
	csvStruct.ColumnNames = make([]string, 0)
	csvStruct.ColumnTypes = make([]string, 0)

	csvStruct.ColumnNames = append(csvStruct.ColumnNames, "LastPrice")
	csvStruct.ColumnTypes = append(csvStruct.ColumnTypes, "text")
	csvStruct.ColumnNames = append(csvStruct.ColumnNames, "Symbol")
	csvStruct.ColumnTypes = append(csvStruct.ColumnTypes, "text")
	csvStruct.ColumnNames = append(csvStruct.ColumnNames, "Timestamp")
	csvStruct.ColumnTypes = append(csvStruct.ColumnTypes, "text")
	csvStruct.ColumnNames = append(csvStruct.ColumnNames, "Volume")
	csvStruct.ColumnTypes = append(csvStruct.ColumnTypes, "text")

	err = s.tableCheck(csvStruct.ColumnNames, csvStruct.ColumnTypes)
	if err != nil {
		return err
	}

	// for now, only a single row per Queue message
	csvStruct.Records = make([]churrodata.CSVRow, 0)

	var wsMsg WebSocketData

	for {
		// back-pressure check
		if backPressure == 1 {
			fmt.Printf("sleeping due to backpressure...")
			time.Sleep(time.Second * time.Duration(sleepTime))
			continue
		}

		// read from the stream
		err := w.ReadJSON(&wsMsg)
		if err != nil {
			return err
		}

		records := wsMsg.GetRecords()
		fmt.Printf("transform rules %v\n", s.TransformRules)

		for i := 0; i < len(records); i++ {
			err = transform.RunRules(config.FinnHubScheme, csvStruct.ColumnNames, records[i], s.TransformRules, s.TransformFunctions, s.logger)
			if err != nil {
				s.logger.Error("error in runrules", zap.Error(err))
			}

			r := getCSVRow(records[i])
			csvStruct.Records = append(csvStruct.Records, r)
			s.logger.Debug("csv record read ", zap.String("csvrec", fmt.Sprintf("%v", records[i])))
			if s.Queue == nil {
				s.logger.Debug("Info: queue is nil")
			}
			s.logger.Debug("queue len", zap.Int("queuelen", len(s.Queue)))

			if len(csvStruct.Records) >= RecordsPerPush {
				s.logger.Info("pushing to Queue")
				//convert csvStruct into []byte
				csvBytes, _ := json.Marshal(csvStruct)
				msg := loader.LoaderMessage{}
				msg.Metadata = csvBytes
				msg.DataFormat = config.FinnHubScheme
				s.Queue <- msg
				csvStruct.Records = make([]churrodata.CSVRow, 0)
			}
		}

	}

	return nil
}

func (w WebSocketData) Print() {
	dataLen := len(w.Data)
	fmt.Printf("Data: \n")
	for i := 0; i < dataLen; i++ {
		fmt.Printf("LastPrice: %f\n", w.Data[i].LastPrice)
		fmt.Printf("Symbol: %s\n", w.Data[i].Symbol)
		fmt.Printf("Timestamp: %d\n", w.Data[i].Timestamp)
		fmt.Printf("Volume: %f\n", w.Data[i].Volume)
	}
	fmt.Printf("Type: %s\n", w.Type)
}
func (w WebSocketData) GetRecords() [][]string {
	records := [][]string{}
	dataLen := len(w.Data)
	fmt.Printf("Data: \n")
	for i := 0; i < dataLen; i++ {
		rec := make([]string, 4)
		rec[0] = fmt.Sprintf("%f", w.Data[i].LastPrice)
		rec[1] = w.Data[i].Symbol
		rec[2] = fmt.Sprintf("%d", w.Data[i].Timestamp)
		rec[3] = fmt.Sprintf("%f", w.Data[i].Volume)
		records = append(records, rec)
	}
	fmt.Printf("Type: %s\n", w.Type)
	return records
}
