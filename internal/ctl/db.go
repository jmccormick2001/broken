package ctl

import (
	"database/sql"
	"fmt"

	"go.uber.org/zap"

	_ "github.com/lib/pq"
)

func (s *Server) verify() error {

	cfg := s.Pi.Spec.AdminDataSource
	dbCreds := s.DBCreds

	// get db connection
	pgConnectString := dbCreds.GetDBConnectString(cfg)
	s.logger.Debug("pgConnectString", zap.String("connectstring", pgConnectString))
	db, err := sql.Open("postgres", pgConnectString)
	if err != nil {
		return err
	}
	defer db.Close()
	s.logger.Debug("logged on as db churro admin user", zap.String("user", cfg.Username))

	// make sure churro admin database is created
	sqlStr := fmt.Sprintf("CREATE DATABASE if not exists %s", cfg.Database)
	_, err = db.Exec(sqlStr)
	s.logger.Info("create database", zap.String("sql", sqlStr))
	if err != nil {
		return err
	}
	s.logger.Info("Successfully created database", zap.String("database", cfg.Database))

	sqlStr = fmt.Sprintf("CREATE TABLE if not exists %s.watchdirectory ( id STRING PRIMARY KEY, name STRING NOT NULL, path STRING NOT NULL, scheme STRING NOT NULL, regex STRING NOT NULL, tablename STRING NOT NULL, lastupdated TIMESTAMP);", cfg.Database)
	s.logger.Info("create table", zap.String("sql", sqlStr))
	var stmt *sql.Stmt
	stmt, err = db.Prepare(sqlStr)
	if err != nil {
		return err
	}
	_, err = stmt.Exec()
	if err != nil {
		return err
	}
	s.logger.Info("watchdirectory Table created successfully..")

	sqlStr = fmt.Sprintf("CREATE TABLE if not exists %s.extractrule ( id STRING PRIMARY KEY, watchdirectoryid STRING NOT NULL, columnname STRING NOT NULL, rulesource STRING NOT NULL, matchvalues STRING, lastupdated TIMESTAMP);", cfg.Database)
	s.logger.Info("create table", zap.String("sql", sqlStr))
	stmt, err = db.Prepare(sqlStr)
	if err != nil {
		return err
	}
	_, err = stmt.Exec()
	if err != nil {
		return err
	}
	s.logger.Info("extractrule Table created successfully..")

	sqlStr = fmt.Sprintf("CREATE TABLE if not exists %s.transformfunction ( id STRING PRIMARY KEY, name STRING NOT NULL, source STRING NOT NULL, lastupdated TIMESTAMP);", cfg.Database)
	s.logger.Info("create table", zap.String("sql", sqlStr))
	stmt, err = db.Prepare(sqlStr)
	if err != nil {
		return err
	}
	_, err = stmt.Exec()
	if err != nil {
		return err
	}
	s.logger.Info("transformfunction Table created successfully..")

	sqlStr = fmt.Sprintf("CREATE TABLE if not exists %s.transformrule ( id STRING PRIMARY KEY, name STRING NOT NULL, path STRING NOT NULL, scheme STRING NOT NULL, transformfunctionname STRING NOT NULL, lastupdated TIMESTAMP);", cfg.Database)
	s.logger.Info("create table", zap.String("sql", sqlStr))
	stmt, err = db.Prepare(sqlStr)
	if err != nil {
		return err
	}
	_, err = stmt.Exec()
	if err != nil {
		return err
	}
	s.logger.Info("transformrule Table created successfully..")

	return nil
}
