package extract

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
	"go.uber.org/zap"
)

// create the table based on column names and column types
func (s Server) tableCheck(columnNames, columnTypes []string) (err error) {

	userid := s.Pi.Spec.DataSource.Username
	dbname := s.Pi.Spec.DataSource.Database

	tableName := s.TableName

	url := s.DBCreds.GetDBConnectString(s.Pi.Spec.DataSource)

	db, err := sql.Open("postgres", url)
	if err != nil {
		return fmt.Errorf("error connecting to the database: %v", err)
	}
	defer db.Close()

	sqlStr := fmt.Sprintf("CREATE TABLE if not exists %s.%s ( id serial PRIMARY KEY, dataformat text, %s createdtime TIMESTAMP);", dbname, tableName, getTableColumns(columnNames, columnTypes))
	s.logger.Info(sqlStr)

	var stmt *sql.Stmt
	stmt, err = db.Prepare(sqlStr)
	if err != nil {
		return err
	}
	_, err = stmt.Exec()
	if err != nil {
		return err
	}

	s.logger.Debug("Table created successfully..", zap.String("table", tableName))

	// grant privs to pipeline database user
	// grant insert,select on foo.churro,foo.dataprov to foo
	sqlStr = fmt.Sprintf("grant insert,select on %s.%s to %s;", dbname, tableName, userid)
	stmt, err = db.Prepare(sqlStr)
	if err != nil {
		return err
	}
	_, err = stmt.Exec()
	if err != nil {
		return err
	}
	s.logger.Debug(sqlStr)

	return nil
}

func getTableColumns(columnNames, columnTypes []string) string {
	var result string
	for i, v := range columnNames {
		col := fmt.Sprintf("%s %s,", v, columnTypes[i])
		result = result + col
	}
	return result
}
