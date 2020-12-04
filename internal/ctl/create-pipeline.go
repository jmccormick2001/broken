package ctl

import (
	"database/sql"
	"fmt"
	"go.uber.org/zap"

	_ "github.com/lib/pq"
)

// createPipeline creates the pipeline database
func (s *Server) createPipeline() error {

	pi := s.Pi
	dbCreds := s.DBCreds

	adminUser := pi.Spec.AdminDataSource.Username

	// get db connection
	pgConnectString := dbCreds.GetDBConnectString(pi.Spec.AdminDataSource)
	db, err := sql.Open("postgres", pgConnectString)
	if err != nil {
		return err
	}
	defer db.Close()
	s.logger.Debug("logged on as db admin user", zap.String("user", adminUser))

	// create the pipeline user
	// create user if not exists pipeline1user;
	sqlStr := fmt.Sprintf("create user if not exists %s;", pi.Spec.DataSource.Username)
	var stmt *sql.Stmt
	stmt, err = db.Prepare(sqlStr)
	if err != nil {
		return err
	}
	_, err = stmt.Exec()
	if err != nil {
		return err
	} else {
		s.logger.Info("User created successfully", zap.String("user", pi.Spec.DataSource.Username), zap.String("sql", sqlStr))
	}

	// create the pipeline database
	sqlStr = fmt.Sprintf("CREATE DATABASE if not exists %s", pi.Spec.DataSource.Database)
	_, err = db.Exec(sqlStr)
	if err != nil {
		return err
	} else {
		s.logger.Info("Successfully created database..", zap.String("sql", sqlStr), zap.String("database", pi.Spec.DataSource.Database))
	}

	sqlStr = fmt.Sprintf("CREATE TABLE if not exists %s.dataprov ( id STRING PRIMARY KEY, name STRING, path STRING, createdtime TIMESTAMP);", pi.Spec.DataSource.Database)
	stmt, err = db.Prepare(sqlStr)
	if err != nil {
		return err
	}
	_, err = stmt.Exec()
	if err != nil {
		return err
	}

	s.logger.Info("Table created successfully..", zap.String("sql", sqlStr))

	/**
	CREATE TABLE if not exists pipeline1.churroformat (
	        id serial PRIMARY KEY,
	        dataformat text,
	        metadata JSONB,
	        createdtime TIMESTAMP);
	*/
	sqlStr = fmt.Sprintf("CREATE TABLE if not exists %s.churroformat ( id serial PRIMARY KEY, dataformat text, metadata JSONB, createdtime TIMESTAMP);", pi.Spec.DataSource.Database)
	stmt, err = db.Prepare(sqlStr)
	if err != nil {
		return err
	}
	_, err = stmt.Exec()
	if err != nil {
		return err
	}
	s.logger.Info("Table created successfully..", zap.String("sql", sqlStr))

	// grant privs to pipeline database user
	// grant insert,select on foo.churro,foo.dataprov to foo
	sqlStr = fmt.Sprintf("grant insert,select on %s.churroformat,%s.dataprov to %s;", pi.Spec.DataSource.Database, pi.Spec.DataSource.Database, pi.Spec.DataSource.Username)
	stmt, err = db.Prepare(sqlStr)
	if err != nil {
		return err
	}
	_, err = stmt.Exec()
	if err != nil {
		return err
	}
	s.logger.Info("Privs granted successfully..", zap.String("sql", sqlStr))

	// grant privs to pipeline database user
	// grant create on pipelinedatabase to someuser
	sqlStr = fmt.Sprintf("grant all on database %s to %s;", pi.Spec.DataSource.Database, pi.Spec.DataSource.Username)
	stmt, err = db.Prepare(sqlStr)
	if err != nil {
		return err
	}
	_, err = stmt.Exec()
	if err != nil {
		return err
	}
	s.logger.Info("Privs granted successfully..", zap.String("sql", sqlStr))

	/**
	CREATE TABLE if not exists pipeline1.pipeline_stats (
	        id serial PRIMARY KEY,
	        dataprov_id bigint,
	        file_name text,
	        records_in bigint,
	        lastUpdated TIMESTAMP);
	*/
	sqlStr = fmt.Sprintf("CREATE TABLE if not exists %s.pipeline_stats ( id serial PRIMARY KEY, dataprov_id text, file_name text, records_in bigint, lastupdated TIMESTAMP);", pi.Spec.DataSource.Database)
	stmt, err = db.Prepare(sqlStr)
	if err != nil {
		return err
	}
	_, err = stmt.Exec()
	if err != nil {
		return err
	}
	s.logger.Info("Table created successfully..", zap.String("sql", sqlStr))

	// grant privs to pipeline database user
	// grant insert,update,select on pipeline1.pipeline_stats to someuser
	sqlStr = fmt.Sprintf("grant insert,select on %s.pipeline_stats to %s;", pi.Spec.DataSource.Database, pi.Spec.DataSource.Username)
	stmt, err = db.Prepare(sqlStr)
	if err != nil {
		return err
	}
	_, err = stmt.Exec()
	if err != nil {
		return err
	}
	s.logger.Info("Privs granted successfully..", zap.String("sql", sqlStr))

	// TODO create an index on the stats table

	// update the churro.pipeline admin table for this new pipeline
	/**
	insertStmt := "insert into churro.pipeline (name, config, createdtime) values ($1, $2, now())"
	if err != nil {
		return err
	}
	_, err = db.Exec(insertStmt, cfg.PipelineName, cfg.String())
	if err != nil {
		return err
	}
	*/
	/**
	CREATE TABLE if not exists pipeline1.transform_function (
	        id string PRIMARY KEY,
	        transform_name string unique,
	        transform_source text,
	        lastUpdated TIMESTAMP);
	*/
	sqlStr = fmt.Sprintf("CREATE TABLE if not exists %s.transform_function ( id string PRIMARY KEY, transform_name string unique, transform_source text, lastupdated TIMESTAMP);", pi.Spec.DataSource.Database)
	stmt, err = db.Prepare(sqlStr)
	if err != nil {
		return err
	}
	_, err = stmt.Exec()
	if err != nil {
		return err
	}
	s.logger.Info("Table created successfully..", zap.String("sql", sqlStr))
	/**
	CREATE TABLE if not exists pipeline1.transform_rule (
	        id string PRIMARY KEY,
	        transform_rule_name string,
	        transform_rule_path string,
	        transform_rule_scheme string,
	        transform_function_name string references doozer.transform_function (transform_name),
	        lastUpdated TIMESTAMP);
	*/
	sqlStr = fmt.Sprintf("CREATE TABLE if not exists %s.transform_rule ( id string PRIMARY KEY, transform_rule_name string, transform_rule_path string, transform_rule_scheme string, transform_function_name string references %s.transform_function (transform_name), lastupdated TIMESTAMP);", pi.Spec.DataSource.Database, pi.Spec.DataSource.Database)
	stmt, err = db.Prepare(sqlStr)
	if err != nil {
		return err
	}
	_, err = stmt.Exec()
	if err != nil {
		return err
	}
	s.logger.Info("Table created successfully..", zap.String("sql", sqlStr))

	/**
	CREATE TABLE if not exists doozer.watch_directory (
	        id string PRIMARY KEY,
	        watch_name string unique,
	        watch_path string,
	        watch_scheme string,
	        watch_regex string,
	        watch_tablename string,
	        lastUpdated TIMESTAMP);
	*/
	sqlStr = fmt.Sprintf("CREATE TABLE if not exists %s.watch_directory ( id string PRIMARY KEY, watch_name string unique, watch_path string, watch_scheme string, watch_regex string, watch_tablename string, lastupdated TIMESTAMP);", pi.Spec.DataSource.Database)
	stmt, err = db.Prepare(sqlStr)
	if err != nil {
		return err
	}
	_, err = stmt.Exec()
	if err != nil {
		return err
	}
	s.logger.Info("Table created successfully..", zap.String("sql", sqlStr))

	s.logger.Info("pipeline database successfully created", zap.String("database", pi.Spec.DataSource.Database))

	/**
	CREATE TABLE if not exists doozer.watch_rule (
	        id string PRIMARY KEY,
	        watch_directory_id string references doozer.watch_directory (id),
	        column_name string,
	        rule_source text,
	        lastUpdated TIMESTAMP);
	*/
	sqlStr = fmt.Sprintf("CREATE TABLE if not exists %s.watch_rule ( id string PRIMARY KEY, watch_directory_id string references %s.watch_directory (id), column_name string, rule_source text, lastupdated TIMESTAMP);", pi.Spec.DataSource.Database, pi.Spec.DataSource.Database)
	stmt, err = db.Prepare(sqlStr)
	if err != nil {
		return err
	}
	_, err = stmt.Exec()
	if err != nil {
		return err
	}
	s.logger.Info("Table created successfully..", zap.String("sql", sqlStr))

	return nil

}
