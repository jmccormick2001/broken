package user

import (
	"database/sql"
	"os"

	_ "github.com/mattn/go-sqlite3"
)

type AdminDatabase struct {
	DBPath  string
	Created bool
}

// this is the single instance of the db
var AdminDB AdminDatabase

func Seed(dbPath string) error {

	AdminDB = AdminDatabase{}
	AdminDB.DBPath = dbPath

	_, err := os.Stat(dbPath)
	if os.IsNotExist(err) {
		os.Create(dbPath)
	}

	var db *sql.DB
	db, err = sql.Open("sqlite3", dbPath)
	if err != nil {
		return err
	}

	err = AdminDB.CreateObjects(db)
	if err != nil {
		return err
	}

	db.Close()
	AdminDB.Created = true

	return nil
}

func (a *AdminDatabase) CreateObjects(db *sql.DB) (err error) {

	// create AuthenticatedUser
	_, err = db.Exec("CREATE TABLE if not exists `authenticateduser` (`id` VARCHAR(255) PRIMARY KEY, `token` VARCHAR(64) NOT NULL, `locked` boolean NOT NULL, `lastupdated` DATETIME NULL)")
	if err != nil {
		panic(err)
	}

	// create UserProfile
	_, err = db.Exec("CREATE TABLE if not exists `userprofile` (`id` VARCHAR(255) PRIMARY KEY, `firstname` VARCHAR(64) NOT NULL, `lastname` VARCHAR(64) NOT NULL, `password` VARCHAR(64) NOT NULL, `access` VARCHAR(25) NOT NULL, `email` VARCHAR(64) NOT NULL, `lastupdated` DATETIME NULL)")
	if err != nil {
		panic(err)
	}
	// create UserPipelineAccess
	_, err = db.Exec("CREATE TABLE if not exists `userpipelineaccess` (`userprofileid` VARCHAR(255) NOT NULL, `pipelineid` VARCHAR(64) NOT NULL, `access` VARCHAR(25) NOT NULL, `lastupdated` DATETIME NULL)")
	if err != nil {
		panic(err)
	}
	// create Pipeline
	_, err = db.Exec("CREATE TABLE if not exists `pipeline` (`id` VARCHAR(255) PRIMARY KEY, `name` VARCHAR(64) NOT NULL, `port` INT NOT NULL, `servicecrt` TEXT NOT NULL, `cr` TEXT NOT NULL, `lastupdated` DATETIME NULL)")
	if err != nil {
		panic(err)
	}

	return err
}

func (a *AdminDatabase) Bootstrap() error {

	db, err := sql.Open("sqlite3", a.DBPath)
	if err != nil {
		return err
	}

	var id string
	bootstrapID := "0000"
	row := db.QueryRow("SELECT id FROM userprofile where id=$1", bootstrapID)
	switch err := row.Scan(&id); err {
	case sql.ErrNoRows:
	case nil:
		return nil
	default:
		return err
	}

	// if bootstrap ID was not found, then....
	// insert bootstrap user into UserProfile
	stmt, err := db.Prepare("INSERT INTO userprofile(id, firstname, lastname, password, access, email, lastupdated) values(?,?,?,?,?,?,?)")
	if err != nil {
		return err
	}

	_, err = stmt.Exec(bootstrapID, "admin", "admin", "admin", "Admin", "admin@admin.org", "2012-12-09")
	if err != nil {
		return err
	}

	db.Close()
	return nil
}
