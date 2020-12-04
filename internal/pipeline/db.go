package pipeline

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/xid"
)

type Pipeline struct {
	Id          string
	Name        string
	Port        int
	Cr          string
	ServiceCrt  string
	LastUpdated string
}

func (a *Pipeline) Create(db *sql.DB) (string, error) {
	a.Id = xid.New().String()
	datetime := time.Now()
	var INSERT = fmt.Sprintf("INSERT INTO pipeline(servicecrt, id, name, port, cr, lastupdated) values(?,?,?,?,?,'%v')", datetime.Format("2006-01-02T15:04:05.999999999"))
	stmt, err := db.Prepare(INSERT)
	if err != nil {
		fmt.Println(err)
		return "", err
	}

	_, err = stmt.Exec(a.ServiceCrt, a.Id, a.Name, a.Port, a.Cr)
	if err != nil {
		fmt.Println(err)
		return "", err
	}

	return a.Id, nil
}

func (a *Pipeline) Delete(db *sql.DB) error {
	var DELETE = fmt.Sprintf("DELETE FROM pipeline where id=?")
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

func Get(id string, db *sql.DB) (a Pipeline, err error) {

	row := db.QueryRow("SELECT servicecrt, id,name, port, cr, lastupdated FROM pipeline where id=$1", id)
	switch err := row.Scan(&a.ServiceCrt, &a.Id, &a.Name, &a.Port, &a.Cr, &a.LastUpdated); err {
	case sql.ErrNoRows:
		fmt.Printf("pipeline id was not found\n")
		return a, err
	case nil:
		fmt.Println("pipeline id was found")
		return a, nil
	default:
		return a, err
	}
	return a, nil
}

func GetAll(db *sql.DB) (a []Pipeline, err error) {

	a = make([]Pipeline, 0)

	rows, err := db.Query("SELECT servicecrt, id, name, port, cr, lastupdated FROM pipeline")
	if err != nil {
		fmt.Println(err.Error())
		return a, err
	}

	for rows.Next() {
		p := Pipeline{}
		err = rows.Scan(&p.ServiceCrt, &p.Id, &p.Name, &p.Port, &p.Cr, &p.LastUpdated)
		if err != nil {
			fmt.Println(err.Error())
			return a, err
		}
		a = append(a, p)
	}

	return a, nil
}

func (a *Pipeline) Update(db *sql.DB) error {
	datetime := time.Now()
	var UPDATE = fmt.Sprintf("UPDATE pipeline set (servicecrt, cr, lastupdated) = (?, ?,'%v') where id = ?", datetime.Format("2006-01-02T15:04:05.999999999"))
	stmt, err := db.Prepare(UPDATE)
	if err != nil {
		fmt.Println(err)
		return err
	}

	_, err = stmt.Exec(a.ServiceCrt, a.Cr, a.Id)
	if err != nil {
		fmt.Println(err)
		return err
	}

	return nil
}
