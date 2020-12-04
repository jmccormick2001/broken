// Package dataprov holds the data provenance logic which is essentially
// used to identify each data source uniquely with a generated ID
// that is passed with all processed data so users can track data back
// to its source
package dataprov

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/lib/pq"
	"github.com/rs/xid"
	"gitlab.com/churro-group/churro/api/v1alpha1"
	"gitlab.com/churro-group/churro/internal/config"
	"go.uber.org/zap"
)

type DataProvenance struct {
	Id          string
	Name        string
	Path        string
	CreatedTime time.Time
}

// Register a new data provenance instance, return an error
// if it can not be registered with churro
func Register(dp *DataProvenance, pipeline v1alpha1.Pipeline, dbCreds config.DBCredentials, logger *zap.SugaredLogger) (err error) {

	dp.CreatedTime = time.Now()
	dp.Id = xid.New().String()
	// register the id with the churro data store

	err = insertDataprov(*dp, pipeline, dbCreds, logger)
	if err != nil {
		logger.Errorf("error in insertDataprov %s\n", err.Error())
	}

	return err
}

func (s DataProvenance) String() string {
	return fmt.Sprintf("Name: %s Path: %s CreatedTime %s\n", s.Name, s.Path, s.CreatedTime)
}

func insertDataprov(dp DataProvenance, cfg v1alpha1.Pipeline, dbCreds config.DBCredentials, logger *zap.SugaredLogger) (err error) {
	// Connect to the "churro" database.

	pgConnectString := dbCreds.GetDBConnectString(cfg.Spec.DataSource)
	logger.Infof("insertDataprov %s\n", pgConnectString)
	db, err := sql.Open("postgres", pgConnectString)
	if err != nil {
		return err
	}
	defer db.Close()

	insertStmt, err := db.Prepare("INSERT into DATAPROV (id, name, path, createdtime) values ($1, $2, $3, $4)")
	if err != nil {
		return err
	}
	defer insertStmt.Close()
	if _, err := insertStmt.Exec(dp.Id, dp.Name, dp.Path, dp.CreatedTime); err != nil {
		return err
	}

	return nil
}
