package extract

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"gitlab.com/churro-group/churro/internal/churrodata"
	"gitlab.com/churro-group/churro/internal/config"
	"gitlab.com/churro-group/churro/internal/dataprov"
	"gitlab.com/churro-group/churro/internal/loader"
	"gitlab.com/churro-group/churro/internal/transform"
	"gitlab.com/churro-group/churro/internal/watch"
	"gopkg.in/xmlpath.v2"
)

type compiledXMLRule struct {
	rule         watch.ExtractRule
	compiledRule *xmlpath.Path
}

// Extract a XML file contents and exit
func (s *Server) ExtractXML(ctx context.Context) (err error) {

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	s.logger.Info("ExtractXML starting...")

	// read the XML file to be processed and parse it
	reader, err := os.Open(s.FileName)
	if err != nil {
		return err
	}
	root, err := xmlpath.Parse(reader)
	if err != nil {
		return err
	}

	// register data provenance
	dp := dataprov.DataProvenance{}
	dp.Name = s.FileName
	dp.Path = s.FileName
	err = dataprov.Register(&dp, s.Pi, s.DBCreds, s.logger)
	if err != nil {
		s.logger.Error("can not register data prov")
		os.Exit(1)
	}
	s.logger.Infof("dp info %s %s\n", dp.Name, dp.Path)

	go s.pushToLoader(ctx, config.XMLScheme)

	time.Sleep(time.Second * time.Duration(sleepTime))

	rules := getXMLRules(s.WatchDirectory)

	xmlStruct := getXMLFormat(rules, root)

	xmlStruct.Path = s.FileName
	xmlStruct.Dataprov = dp.Id
	xmlStruct.PipelineName = s.Pi.Name
	xmlStruct.Tablename = s.TableName

	recLen := len(xmlStruct.Records)
	s.logger.Infof("xml records to process %d\n", recLen)

	err = s.tableCheck(xmlStruct.ColumnNames, xmlStruct.ColumnTypes)
	if err != nil {
		return err
	}

	s.logger.Infof("transform rules %s\n", fmt.Sprintf("%+v", s.TransformRules))

	// partStruct holds a portion of the overall xmlStruct records
	// since we push portions of the overall set of records
	partStruct := xmlStruct
	partStruct.Records = make([]churrodata.XMLRow, 0)

	s.logger.Infof("partStruct col len %d\n", len(partStruct.ColumnNames))

	recordsProcessed := 0
	for i := 0; i < recLen; i++ {
		if backPressure == 1 {
			fmt.Printf("sleeping due to backpressure...")
			time.Sleep(time.Second * time.Duration(sleepTime))
		}

		s.logger.Infof("before transform %s\n", fmt.Sprintf("%v", xmlStruct.Records[i].Cols))
		err := transform.RunRules(config.XMLScheme, xmlStruct.ColumnNames, xmlStruct.Records[i].Cols, s.TransformRules, s.TransformFunctions, s.logger)
		if err != nil {
			s.logger.Errorf("error in RunRules %s\n", err.Error())
		}
		s.logger.Info("after transform %s\n", fmt.Sprintf("%+v", xmlStruct.Records[i].Cols))

		if s.Queue == nil {
			s.logger.Debug("Info: queue is nil")
		}
		s.logger.Debug("queue len %d\n", len(s.Queue))

		recordsProcessed++

		partStruct.Records = append(partStruct.Records, xmlStruct.Records[i])
		if recordsProcessed >= RecordsPerPush {
			s.logger.Info("pushing to Queue")
			//convert partStruct into []byte
			xmlBytes, _ := json.Marshal(partStruct)
			msg := loader.LoaderMessage{}
			msg.Metadata = xmlBytes
			msg.DataFormat = "xml"
			s.Queue <- msg
			recordsProcessed = 0
			partStruct.Records = make([]churrodata.XMLRow, 0)
		}

	}

	if recordsProcessed > 0 {
		xmlBytes, _ := json.Marshal(partStruct)
		msg := loader.LoaderMessage{}
		msg.Metadata = xmlBytes
		msg.DataFormat = "xml"
		s.Queue <- msg
	}

	s.logger.Info("end of XML file reached, cancelling pushes...")
	time.Sleep(time.Second * 5)

	return err
}

func getColumn(rule compiledXMLRule, root *xmlpath.Node) (cols []string) {
	iter := rule.compiledRule.Iter(root)
	for iter.Next() {
		cols = append(cols, iter.Node().String())
	}
	return cols
}

func getXMLRules(watchDirs []watch.WatchDirectory) (rules []compiledXMLRule) {
	for i := 0; i < len(watchDirs); i++ {
		if watchDirs[i].Scheme == config.XMLScheme {
			for _, v := range watchDirs[i].ExtractRules {
				fromRule := v
				path := xmlpath.MustCompile(fromRule.RuleSource)
				rules = append(rules, compiledXMLRule{rule: fromRule, compiledRule: path})
			}
		}
	}
	return rules
}

func getXMLFormat(rules []compiledXMLRule, root *xmlpath.Node) (format churrodata.XMLFormat) {

	cols := make([][]string, 0)

	for i := 0; i < len(rules); i++ {
		format.ColumnNames = append(format.ColumnNames, rules[i].rule.ColumnName)
		format.ColumnTypes = append(format.ColumnTypes, "TEXT")

		cols = append(cols, getColumn(rules[i], root))
	}

	var records int
	fmt.Printf("cols %+v\n", cols)
	if len(cols) > 0 {
		records = len(cols[0])
	} else {
		fmt.Println("no columns found in queries")
		return format
	}
	columns := len(cols)
	for rec := 0; rec < records; rec++ {
		xmlrow := churrodata.XMLRow{}
		for c := 0; c < columns; c++ {
			xmlrow.Cols = append(xmlrow.Cols, cols[c][rec])
		}
		format.Records = append(format.Records, xmlrow)
	}

	return format
}
