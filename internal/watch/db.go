package watch

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/rs/xid"
)

type ExtractRule struct {
	Id               string    `json:"id"`
	WatchDirectoryId string    `json:"watchdirectoryid"`
	ColumnName       string    `json:"columnname"`
	RuleSource       string    `json:"rulesource"`
	MatchValues      string    `json:"matchvalues"`
	LastUpdated      time.Time `json:"lastupdated"`
}

type WatchDirectory struct {
	Id           string                 `json:"id"`
	Name         string                 `json:"watchname"`
	Path         string                 `json:"watchpath"`
	Scheme       string                 `json:"watchscheme"`
	Regex        string                 `json:"watchregex"`
	Tablename    string                 `json:"watchtablename"`
	ExtractRules map[string]ExtractRule `json:"watchrules"`
	LastUpdated  time.Time              `json:"lastupdated"`
}

func (a *WatchDirectory) Create(db *sql.DB) error {
	a.Id = xid.New().String()
	var INSERT = fmt.Sprintf("INSERT INTO watchdirectory(id, name, path, scheme, regex, tablename, lastupdated) values('%s',$1,$2,$3,$4,$5,now())", a.Id)
	stmt, err := db.Prepare(INSERT)
	if err != nil {
		fmt.Println(err)
		return err
	}

	_, err = stmt.Exec(a.Name, a.Path, a.Scheme, a.Regex, a.Tablename)
	if err != nil {
		fmt.Println(err)
		return err
	}

	return nil
}

func (a *WatchDirectory) Delete(db *sql.DB) error {
	var DELETE = fmt.Sprintf("DELETE FROM watchdirectory where id=$1")
	stmt, err := db.Prepare(DELETE)
	if err != nil {
		fmt.Println(err)
		return err
	}

	_, err = stmt.Exec(a.Id)
	if err != nil {
		fmt.Println(err)
		return err
	}

	return nil
}

func (a *WatchDirectory) Update(db *sql.DB) error {
	var UPDATE = fmt.Sprintf("UPDATE watchdirectory set (tablename, name, path, scheme, regex, lastupdated) = ($1,$2,$3,$4,$5,now()) where id = $6")
	stmt, err := db.Prepare(UPDATE)
	if err != nil {
		fmt.Println(err)
		return err
	}

	_, err = stmt.Exec(a.Tablename, a.Name, a.Path, a.Scheme, a.Regex, a.Id)
	if err != nil {
		fmt.Println(err)
		return err
	}

	return nil
}

func GetWatchDirectory(id string, db *sql.DB) (a WatchDirectory, err error) {

	rules, err := GetExtractRulesForWatchDir(id, db)
	if err != nil {
		return a, err
	}

	a.ExtractRules = make(map[string]ExtractRule)
	for i := 0; i < len(rules); i++ {
		a.ExtractRules[rules[i].Id] = rules[i]
	}

	a.Id = id
	row := db.QueryRow("SELECT tablename, name, path, scheme, regex, lastupdated FROM watchdirectory where id=$1", id)
	switch err := row.Scan(&a.Tablename, &a.Name, &a.Path, &a.Scheme, &a.Regex, &a.LastUpdated); err {
	case sql.ErrNoRows:
		fmt.Printf("watchdir id was not found\n")
		return a, err
	case nil:
		fmt.Println("watchdir id was found")
		return a, nil
	default:
		return a, err
	}
	return a, nil
}
func GetWatchDirectories(db *sql.DB) (a []WatchDirectory, err error) {

	var rows *sql.Rows
	rows, err = db.Query("SELECT tablename, id, name, path, scheme, regex, lastupdated FROM watchdirectory")
	if err != nil {
		fmt.Printf("watchdir id was not found\n")
		return a, err
	}

	for rows.Next() {
		r := WatchDirectory{}
		err := rows.Scan(&r.Tablename, &r.Id, &r.Name, &r.Path, &r.Scheme, &r.Regex, &r.LastUpdated)
		if err != nil {
			return a, err
		}
		a = append(a, r)
	}

	return a, nil
}

func (a *ExtractRule) Create(db *sql.DB) error {
	a.Id = xid.New().String()
	var INSERT = fmt.Sprintf("INSERT INTO extractrule(id, watchdirectoryid, columnname, rulesource, matchvalues, lastupdated) values('%s',$1,$2,$3,$4,now())", a.Id)
	stmt, err := db.Prepare(INSERT)
	if err != nil {
		fmt.Println(err)
		return err
	}

	_, err = stmt.Exec(a.WatchDirectoryId, a.ColumnName, a.RuleSource, a.MatchValues)
	if err != nil {
		fmt.Println(err)
		return err
	}

	return nil
}

func (a *ExtractRule) Delete(db *sql.DB) error {
	var DELETE = fmt.Sprintf("DELETE FROM extractrule where id=$1")
	stmt, err := db.Prepare(DELETE)
	if err != nil {
		fmt.Println(err)
		return err
	}

	_, err = stmt.Exec(a.Id)
	if err != nil {
		fmt.Println(err)
		return err
	}

	return nil
}

func (a *ExtractRule) Update(db *sql.DB) error {
	var UPDATE = fmt.Sprintf("UPDATE extractrule set (columnname, rulesource, matchvalues, lastupdated) = ($1,$2,$3,now()) where id = $4")
	stmt, err := db.Prepare(UPDATE)
	if err != nil {
		fmt.Println(err)
		return err
	}

	_, err = stmt.Exec(a.ColumnName, a.RuleSource, a.MatchValues, a.Id)
	if err != nil {
		fmt.Println(err)
		return err
	}

	return nil
}

func GetExtractRule(id string, db *sql.DB) (a ExtractRule, err error) {

	a.Id = id
	row := db.QueryRow("SELECT watchdirectoryid, columnname, rulesource, matchvalues, lastupdated FROM extractrule where id=$1", id)
	switch err := row.Scan(&a.WatchDirectoryId, &a.ColumnName, &a.RuleSource, &a.MatchValues, &a.LastUpdated); err {
	case sql.ErrNoRows:
		fmt.Printf("extractrule id was not found\n")
		return a, err
	case nil:
		fmt.Println("extractrule id was found")
		return a, nil
	default:
		return a, err
	}
	return a, nil
}

func GetExtractRulesForWatchDir(watchDirId string, db *sql.DB) (a []ExtractRule, err error) {

	var rows *sql.Rows
	rows, err = db.Query("SELECT id, columnname, rulesource, matchvalues, lastupdated FROM extractrule where watchdirectoryid=$1", watchDirId)
	if err != nil {
		return a, err
	}

	for rows.Next() {
		r := ExtractRule{}
		r.WatchDirectoryId = watchDirId
		err := rows.Scan(&r.Id, &r.ColumnName, &r.RuleSource, &r.MatchValues, &r.LastUpdated)
		if err != nil {
			return a, err
		}
		a = append(a, r)
	}
	return a, nil
}
