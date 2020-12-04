package user

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/rs/xid"
)

// authenticated users
type AuthenticatedUser struct {
	Id          string    `json:"id"`
	Token       string    `json:"token"`
	Locked      bool      `json:"locked"`
	LastUpdated time.Time `json:"lastupdated"`
}

// users are global to all the pipelines
type UserProfile struct {
	Id          string    `json:"id"`
	FirstName   string    `json:"firstname"`
	LastName    string    `json:"lastname"`
	Password    string    `json:"password"`
	Access      string    `json:"access"`
	Email       string    `json:"email"`
	LastUpdated time.Time `json:"lastupdated"`
}

// users can have access granted to a pipeline
type UserPipelineAccess struct {
	UserProfileId string    `json:"userprofileid"`
	PipelineId    string    `json:"pipelineid"`
	Access        string    `json:"access"`
	LastUpdated   time.Time `json:"lastupdated"`
}

func (a *AuthenticatedUser) Create(db *sql.DB) error {
	a.Id = xid.New().String()
	datetime := time.Now()
	var INSERT = fmt.Sprintf("INSERT INTO authenticateduser(id, token, locked, lastupdated) values('%s',?,?,'%v')", a.Id, datetime.Format("2006-01-02T15:04:05.999999999"))
	stmt, err := db.Prepare(INSERT)
	if err != nil {
		fmt.Println(err)
		return err
	}

	_, err = stmt.Exec(a.Token, a.Locked)
	if err != nil {
		fmt.Println(err)
		return err
	}

	return nil
}

func (a *AuthenticatedUser) Delete(db *sql.DB) error {
	var DELETE = fmt.Sprintf("DELETE FROM authenticateduser where id=?")
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

func (a *UserPipelineAccess) Create(db *sql.DB) error {
	datetime := time.Now()
	var INSERT = fmt.Sprintf("INSERT INTO userpipelineaccess(userprofileid, pipelineid, access, lastupdated) values(?,?,?,'%v')", datetime.Format("2006-01-02T15:04:05.999999999"))
	stmt, err := db.Prepare(INSERT)
	if err != nil {
		fmt.Println(err)
		return err
	}

	_, err = stmt.Exec(a.UserProfileId, a.PipelineId, a.Access)
	if err != nil {
		fmt.Println(err)
		return err
	}

	return nil
}

func (a *UserPipelineAccess) Update(db *sql.DB) error {
	datetime := time.Now()
	var UPDATE = fmt.Sprintf("UPDATE userpipelineaccess set (access, lastupdated) = (?,'%v') where userprofileid = ? and pipelineid = ?", datetime.Format("2006-01-02T15:04:05.999999999"))
	stmt, err := db.Prepare(UPDATE)
	if err != nil {
		fmt.Println(err)
		return err
	}

	_, err = stmt.Exec(a.Access, a.UserProfileId, a.PipelineId)
	if err != nil {
		fmt.Println(err)
		return err
	}

	return nil
}

func (a *UserPipelineAccess) DeleteAll(db *sql.DB) error {
	var DELETE = fmt.Sprintf("DELETE FROM UserPipelineAccess where pipelineid = ?")
	stmt, err := db.Prepare(DELETE)
	if err != nil {
		fmt.Println(err)
		return err
	}

	_, err = stmt.Exec(a.PipelineId)
	if err != nil {
		fmt.Println(err)
		return err
	}

	return nil
}
func (a *UserPipelineAccess) Delete(db *sql.DB) error {
	var DELETE = fmt.Sprintf("DELETE FROM UserPipelineAccess where pipelineid = ? and userprofileid = ?")
	stmt, err := db.Prepare(DELETE)
	if err != nil {
		fmt.Println(err)
		return err
	}

	_, err = stmt.Exec(a.PipelineId, a.UserProfileId)
	if err != nil {
		fmt.Println(err)
		return err
	}

	return nil
}

func (a *UserProfile) Create(db *sql.DB) error {
	datetime := time.Now()
	id := xid.New().String()
	var INSERT = fmt.Sprintf("INSERT INTO userprofile(password, id, lastname, firstname, email, access, lastupdated) values(?,?,?,?,?,?,'%v')", datetime.Format("2006-01-02T15:04:05.999999999"))
	stmt, err := db.Prepare(INSERT)
	if err != nil {
		fmt.Println(err)
		return err
	}

	_, err = stmt.Exec(a.Password, id, a.LastName, a.FirstName, a.Email, a.Access)
	if err != nil {
		fmt.Println(err)
		return err
	}

	return nil
}
func (a *UserProfile) Update(db *sql.DB) error {
	datetime := time.Now()
	var UPDATE = fmt.Sprintf("UPDATE userprofile set (password, lastname, firstname, email, access, lastupdated) = (?,?,?,?,?,'%v') where id = ?", datetime.Format("2006-01-02T15:04:05.999999999"))
	stmt, err := db.Prepare(UPDATE)
	if err != nil {
		fmt.Println(err)
		return err
	}

	_, err = stmt.Exec(a.Password, a.LastName, a.FirstName, a.Email, a.Access, a.Id)
	if err != nil {
		fmt.Println(err)
		return err
	}

	return nil
}

func (a *UserProfile) Delete(db *sql.DB) error {
	var DELETE = fmt.Sprintf("DELETE FROM userprofile where id=?")
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

func GetAll(db *sql.DB) (a []UserProfile, err error) {

	a = make([]UserProfile, 0)

	rows, err := db.Query("SELECT id, firstname, lastname, password, access, email, lastupdated FROM userprofile")
	if err != nil {
		fmt.Println(err.Error())
		return a, err
	}

	for rows.Next() {
		p := UserProfile{}
		err = rows.Scan(&p.Id, &p.FirstName, &p.LastName, &p.Password, &p.Access, &p.Email, &p.LastUpdated)
		if err != nil {
			fmt.Println(err.Error())
			return a, err
		}
		a = append(a, p)
	}

	return a, nil
}

func Get(id string, db *sql.DB) (a UserProfile, err error) {

	row := db.QueryRow("SELECT id, firstname, lastname, password, access, email, lastupdated FROM userprofile where id=$1", id)
	switch err := row.Scan(&a.Id, &a.FirstName, &a.LastName, &a.Password, &a.Access, &a.Email, &a.LastUpdated); err {
	case sql.ErrNoRows:
		fmt.Printf("userprofile id was not found\n")
		return a, err
	case nil:
		fmt.Println("userprofile id was found")
		return a, nil
	default:
		return a, err
	}
	return a, nil
}

func GetUserPipelineAccess(pipelineId, userProfileId string, db *sql.DB) (a UserPipelineAccess, err error) {

	a.PipelineId = pipelineId
	a.UserProfileId = userProfileId

	row := db.QueryRow("SELECT access, lastupdated FROM userpipelineaccess where pipelineid=$1 and userprofileid=$2", pipelineId, userProfileId)
	switch err := row.Scan(&a.Access, &a.LastUpdated); err {
	case sql.ErrNoRows:
		fmt.Printf("userpipelineaccess id was not found\n")
		return a, err
	case nil:
		fmt.Println("userpipelineaccess id was found")
		return a, nil
	default:
		return a, err
	}
	return a, nil
}

func GetUsersForPipeline(pipelineId string, db *sql.DB) (a []UserProfile, err error) {

	a = make([]UserProfile, 0)

	rows, err := db.Query("SELECT userprofile.id, userprofile.firstname, userprofile.lastname, userpipelineaccess.access, userprofile.email FROM userprofile, userpipelineaccess WHERE userprofile.id = userpipelineaccess.userprofileid")
	if err != nil {
		fmt.Println(err.Error())
		return a, err
	}

	for rows.Next() {
		p := UserProfile{}
		err = rows.Scan(&p.Id, &p.FirstName, &p.LastName, &p.Access, &p.Email)
		if err != nil {
			fmt.Println(err.Error())
			return a, err
		}
		a = append(a, p)
	}

	return a, nil
}
