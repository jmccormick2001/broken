package ctl

import (
	"database/sql"
	"os"

	_ "github.com/mattn/go-sqlite3"
)

type PipelineAdminDatabase struct {
	DBPath  string
	Created bool
}

// this is the single instance of the db
var PipelineAdminDB PipelineAdminDatabase

func Seed(dbPath string) error {

	PipelineAdminDB = PipelineAdminDatabase{}
	PipelineAdminDB.DBPath = dbPath

	_, err := os.Stat(dbPath)
	if os.IsNotExist(err) {
		os.Create(dbPath)
	}

	var db *sql.DB
	db, err = sql.Open("sqlite3", dbPath)
	if err != nil {
		return err
	}

	err = PipelineAdminDB.CreateObjects(db)
	if err != nil {
		return err
	}

	db.Close()
	PipelineAdminDB.Created = true

	return nil
}

func (a *PipelineAdminDatabase) CreateObjects(db *sql.DB) (err error) {

	// create WatchDirectory
	_, err = db.Exec("CREATE TABLE if not exists `watchdirectory` (`id` VARCHAR(255) PRIMARY KEY, `name` VARCHAR(64) NOT NULL, `path` VARCHAR(64) NOT NULL, `scheme` VARCHAR(10) NOT NULL, `regex` VARCHAR(64) NOT NULL, `tablename` VARCHAR(40) NOT NULL, `lastupdated` DATETIME NULL)")
	if err != nil {
		panic(err)
	}

	// create ExtractRule
	_, err = db.Exec("CREATE TABLE if not exists `extractrule` (`id` VARCHAR(255) PRIMARY KEY, `watchdirectoryid` VARCHAR(64) NOT NULL, `columnname` VARCHAR(64) NOT NULL, `rulesource` VARCHAR(64) NOT NULL, `matchvalues` VARCHAR(64), `lastupdated` DATETIME NULL)")
	if err != nil {
		panic(err)
	}

	// create TransformFunction
	_, err = db.Exec("CREATE TABLE if not exists `transformfunction` (`id` VARCHAR(255) PRIMARY KEY, `name` VARCHAR(64) NOT NULL, `source` TEXT NOT NULL, `lastupdated` DATETIME NULL)")
	if err != nil {
		panic(err)
	}
	// create TransformRule
	_, err = db.Exec("CREATE TABLE if not exists `transformrule` (`id` VARCHAR(255) PRIMARY KEY, `name` VARCHAR(64) NOT NULL, `path` VARCHAR(25) NOT NULL, `scheme` VARCHAR(10) NOT NULL, `transformfunctionname` VARCHAR(64) NOT NULL, `lastupdated` DATETIME NULL)")
	if err != nil {
		panic(err)
	}

	return err
}

func (a *PipelineAdminDatabase) Bootstrap() error {

	return nil
}
