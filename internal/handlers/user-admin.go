package handlers

import (
	"database/sql"
	"time"

	"html/template"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/rs/xid"
	"gitlab.com/churro-group/churro/internal/user"
)

// gets called when you add a new user
func (u *HandlerWrapper) UserAdmin(w http.ResponseWriter, r *http.Request) {

	u.Log.Infof("UserAdmin called\n")

	r.ParseForm()

	firstname := r.Form["firstname"][0]
	lastname := r.Form["lastname"][0]
	password := r.Form["password"][0]
	email := r.Form["email"][0]
	access := r.Form["access"][0]

	if firstname == "" {
		respondWithError(w, http.StatusBadRequest, "Invalid username, first and last")
		return
	}
	if lastname == "" {
		respondWithError(w, http.StatusBadRequest, "Invalid username, first and last")
		return
	}
	if password == "" {
		respondWithError(w, http.StatusBadRequest, "Invalid password")
		return
	}
	if email == "" {
		respondWithError(w, http.StatusBadRequest, "Invalid email")
		return
	}
	if access == "" {
		respondWithError(w, http.StatusBadRequest, "Invalid access")
		return
	}

	x := user.UserProfile{
		FirstName:   firstname,
		LastName:    lastname,
		Password:    password,
		Email:       email,
		Access:      access,
		LastUpdated: time.Now(),
		Id:          xid.New().String()}

	db, err := sql.Open("sqlite3", user.AdminDB.DBPath)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	err = x.Create(db)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	db.Close()

	u.Log.Infof("adding %s to UserProfiles\n", x.Email)

	http.Redirect(w, r, "/users", 302)
}

type AccessValues struct {
	Key      string
	Selected string
}

type UserDetailPage struct {
	User   user.UserProfile
	Values []AccessValues
}

// get a user's details
func (h *HandlerWrapper) UserAdminDetail(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]
	h.Log.Infof("UserAdminDetail called id=%s\n", id)

	db, err := sql.Open("sqlite3", user.AdminDB.DBPath)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	var up user.UserProfile
	up, err = user.Get(id, db)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	db.Close()

	u := user.UserProfile{
		Id:        id,
		FirstName: up.FirstName,
		LastName:  up.LastName,
		Password:  up.Password,
		Access:    up.Access,
		Email:     up.Email}

	tmpl, err := template.ParseFiles("pages/useradmindetail.html", "pages/navbar.html")
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	pageValues := UserDetailPage{
		User:   u,
		Values: make([]AccessValues, 0)}

	v := AccessValues{Key: "Read"}
	if u.Access == "Read" {
		v.Selected = "selected"
	}
	pageValues.Values = append(pageValues.Values, v)
	v = AccessValues{Key: "Write"}
	if u.Access == "Write" {
		v.Selected = "selected"
	}
	pageValues.Values = append(pageValues.Values, v)
	v = AccessValues{Key: "Admin"}
	if u.Access == "Admin" {
		v.Selected = "selected"
	}
	pageValues.Values = append(pageValues.Values, v)

	tmpl.ExecuteTemplate(w, "layout", &pageValues)
}

func (u *HandlerWrapper) UserAdminUpdate(w http.ResponseWriter, r *http.Request) {

	vars := mux.Vars(r)
	id := vars["id"]
	u.Log.Infof("UserAdminUpdate called vars=%+v\n", vars)

	r.ParseForm()

	firstname := r.Form["firstname"][0]
	lastname := r.Form["lastname"][0]
	password := r.Form["pwd"][0]
	email := r.Form["email"][0]
	access := r.Form["access"][0]

	if firstname == "" {
		respondWithError(w, http.StatusBadRequest, "Invalid username, first and last")
		return
	}
	if lastname == "" {
		respondWithError(w, http.StatusBadRequest, "Invalid username, first and last")
		return
	}
	if password == "" {
		respondWithError(w, http.StatusBadRequest, "Invalid password")
		return
	}
	if email == "" {
		respondWithError(w, http.StatusBadRequest, "Invalid email")
		return
	}
	if access == "" {
		respondWithError(w, http.StatusBadRequest, "Invalid access")
		return
	}

	// get the UserProfile
	db, err := sql.Open("sqlite3", user.AdminDB.DBPath)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	var existingUser user.UserProfile
	existingUser, err = user.Get(id, db)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	// update the UserProfile
	existingUser.Password = password
	existingUser.Access = access
	existingUser.Email = email
	existingUser.FirstName = firstname
	existingUser.LastName = lastname

	err = existingUser.Update(db)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}
	db.Close()

	http.Redirect(w, r, "/users", 302)
}

type ShowCreateUserForm struct {
	ErrorText string
}

func (u *HandlerWrapper) ShowCreateUser(w http.ResponseWriter, r *http.Request) {
	tmpl, err := template.ParseFiles("pages/user-create.html", "pages/navbar.html")
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}
	x := ShowCreateUserForm{ErrorText: u.ErrorText}
	tmpl.ExecuteTemplate(w, "layout", x)

}

type UsersPage struct {
	List []user.UserProfile
}

// display all the users
func (u *HandlerWrapper) Users(w http.ResponseWriter, r *http.Request) {
	pageValues := UsersPage{}

	db, err := sql.Open("sqlite3", user.AdminDB.DBPath)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	pageValues.List, err = user.GetAll(db)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	db.Close()

	u.Log.Infof("%d users read\n", len(pageValues.List))

	tmpl, err := template.ParseFiles("pages/useradmin.html", "pages/navbar.html")
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	tmpl.ExecuteTemplate(w, "layout", &pageValues)
}

func (u *HandlerWrapper) UserAdminDelete(w http.ResponseWriter, r *http.Request) {

	vars := mux.Vars(r)
	id := vars["id"]
	u.Log.Infof("UserAdminDelete called id=%s\n", id)

	db, err := sql.Open("sqlite3", user.AdminDB.DBPath)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	a := user.UserProfile{Id: id}
	err = a.Delete(db)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	db.Close()

	http.Redirect(w, r, "/users", 302)
}
