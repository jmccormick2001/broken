package transform

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/rs/xid"
)

type TransformFunction struct {
	Id          string    `json:"id"`
	Name        string    `json:"transformname"`
	Source      string    `json:"transformsource"`
	LastUpdated time.Time `json:"lastupdated"`
}

type TransformRule struct {
	Id                    string    `json:"id"`
	Name                  string    `json:"transformrulename"`
	Path                  string    `json:"transformrulepath"`
	Scheme                string    `json:"transformrulescheme"`
	TransformFunctionName string    `json:"transformfunctionname"`
	LastUpdated           time.Time `json:"lastupdated"`
}

func (a *TransformFunction) Create(db *sql.DB) error {
	a.Id = xid.New().String()
	var INSERT = fmt.Sprintf("INSERT INTO transformfunction(id, name, source, lastupdated) values('%s',$1, $2, now())", a.Id)
	stmt, err := db.Prepare(INSERT)
	if err != nil {
		fmt.Println(err)
		return err
	}

	_, err = stmt.Exec(a.Name, a.Source)
	if err != nil {
		fmt.Println(err)
		return err
	}

	return nil
}

func (a *TransformFunction) Delete(db *sql.DB) error {
	var DELETE = fmt.Sprintf("DELETE FROM transformfunction where id=$1")
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

func (a *TransformFunction) Update(db *sql.DB) error {
	var UPDATE = fmt.Sprintf("UPDATE transformfunction set (name, source, lastupdated) = ($1,$2,now()) where id = $3")
	stmt, err := db.Prepare(UPDATE)
	if err != nil {
		fmt.Println(err)
		return err
	}

	_, err = stmt.Exec(a.Name, a.Source, a.Id)
	if err != nil {
		fmt.Println(err)
		return err
	}

	return nil
}

func GetTransformFunction(id string, db *sql.DB) (a TransformFunction, err error) {

	a.Id = id
	row := db.QueryRow("SELECT name, source, lastupdated FROM transformfunction where id=$1", id)
	switch err := row.Scan(&a.Name, &a.Source, &a.LastUpdated); err {
	case sql.ErrNoRows:
		fmt.Printf("transformfunction id was not found\n")
		return a, err
	case nil:
		fmt.Println("transformfunction id was found")
		return a, nil
	default:
		return a, err
	}
	return a, nil
}

func GetTransformFunctions(db *sql.DB) (a []TransformFunction, err error) {

	var rows *sql.Rows
	rows, err = db.Query("SELECT id, name, source, lastupdated FROM transformfunction")
	if err != nil {
		return a, err
	}

	for rows.Next() {
		r := TransformFunction{}
		err := rows.Scan(&r.Id, &r.Name, &r.Source, &r.LastUpdated)
		if err != nil {
			return a, err
		}
		a = append(a, r)
	}
	return a, nil
}

func (a *TransformRule) Create(db *sql.DB) error {
	a.Id = xid.New().String()
	var INSERT = fmt.Sprintf("INSERT INTO transformrule(id, name, path, scheme, transformfunctionname, lastupdated) values('%s',$1,$2,$3,$4,now())", a.Id)
	stmt, err := db.Prepare(INSERT)
	if err != nil {
		fmt.Println(err)
		return err
	}

	_, err = stmt.Exec(a.Name, a.Path, a.Scheme, a.TransformFunctionName)
	if err != nil {
		fmt.Println(err)
		return err
	}

	return nil
}

func (a *TransformRule) Delete(db *sql.DB) error {
	var DELETE = fmt.Sprintf("DELETE FROM transformrule where id=$1")
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

func (a *TransformRule) Update(db *sql.DB) error {
	var UPDATE = fmt.Sprintf("UPDATE transformrule set (name, path,scheme, transformfunctionname, lastupdated) = ($1,$2,$3,$4,now()) where id = $5")
	stmt, err := db.Prepare(UPDATE)
	if err != nil {
		fmt.Println(err)
		return err
	}

	_, err = stmt.Exec(a.Name, a.Path, a.Scheme, a.TransformFunctionName, a.Id)
	if err != nil {
		fmt.Println(err)
		return err
	}

	return nil
}

func GetTransformRule(id string, db *sql.DB) (a TransformRule, err error) {

	a.Id = id
	row := db.QueryRow("SELECT name, path, scheme, transformfunctionname, lastupdated FROM transformrule where id=$1", a.Id)
	switch err := row.Scan(&a.Name, &a.Path, &a.Scheme, &a.TransformFunctionName, &a.LastUpdated); err {
	case sql.ErrNoRows:
		fmt.Printf("transformrule id was not found\n")
		return a, err
	case nil:
		fmt.Println("transformrule id was found")
		return a, nil
	default:
		return a, err
	}
	return a, nil
}

func GetTransformRules(db *sql.DB) (a []TransformRule, err error) {

	var rows *sql.Rows
	rows, err = db.Query("SELECT id, name, path, scheme, transformfunctionname, lastupdated FROM transformrule")
	if err != nil {
		return a, err
	}

	for rows.Next() {
		r := TransformRule{}
		err := rows.Scan(&r.Id, &r.Name, &r.Path, &r.Scheme, &r.TransformFunctionName, &r.LastUpdated)
		if err != nil {
			return a, err
		}
		a = append(a, r)
	}
	return a, nil
}
