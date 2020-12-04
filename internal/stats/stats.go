// Package stats holds the pipeline stats logic
// for each pipeline as it executes, stats are written about
// each data source being processed, stats are per-pipeline, and
// the pipeline_stats table is created per-pipeline
// the intention of these stats is to give users insights into
// their pipeline processing
package stats

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/lib/pq"
	"go.uber.org/zap"
)

type PipelineStats struct {
	Id          int64
	DataprovId  string
	Pipeline    string
	FileName    string
	RecordsIn   int64
	LastUpdated time.Time
}

// Update inserts or updates pipeline stats
func Update(db *sql.DB, data PipelineStats, logger *zap.SugaredLogger) (err error) {

	var recordsIn int64
	// get existing records count if a row exists
	sqlstr := fmt.Sprintf("SELECT records_in from %s.pipeline_stats where file_name = '%s'", data.Pipeline, data.FileName)
	logger.Infof("stats sql query %s\n", sqlstr)
	row := db.QueryRow(sqlstr)
	err = row.Scan(&recordsIn)
	if err != nil {
		if err == sql.ErrNoRows {
			logger.Info("no rows in pipeline_stats yet")
		} else {
			return err
		}
	}

	recordsIn += data.RecordsIn

	sqlstr = fmt.Sprintf("UPSERT into %s.pipeline_stats (dataprov_id, file_name, records_in, lastupdated ) values ($1, $2, $3, 'now()')", data.Pipeline)
	logger.Infof("stats upsert %s\n", sqlstr)
	upsertStmt, err := db.Prepare(sqlstr)
	if err != nil {
		return err
	}
	defer upsertStmt.Close()
	if _, err := upsertStmt.Exec(data.DataprovId, data.FileName, recordsIn); err != nil {
		return err
	}

	return nil
}
