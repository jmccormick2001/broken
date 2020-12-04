package handlers

import (
	"database/sql"
	"fmt"
	"html/template"
	"net/http"
	"net/url"
	"strings"

	"github.com/gorilla/mux"
	"gitlab.com/churro-group/churro/internal/user"
)

type PipelineUsersForm struct {
	PipelineName string
	PipelineId   string
	Users        []user.UserProfile
	ErrorText    string
}

func (u *HandlerWrapper) ShowPipelineUsers(w http.ResponseWriter, r *http.Request) {

	u.ErrorText = ""

	vars := mux.Vars(r)
	pipelineUsersForm := PipelineUsersForm{}
	pipelineUsersForm.PipelineId = vars["id"]
	pipelineUsersForm.PipelineName = vars["pipelinename"]
	u.Log.Infof("ShowPipelineUsers  pipeline: id %+v\n", pipelineUsersForm)

	db, err := sql.Open("sqlite3", user.AdminDB.DBPath)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	var list []user.UserProfile
	list, err = user.GetAll(db)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}
	pipelineUsersForm.Users = list

	db.Close()

	tmpl, err := template.ParseFiles("pages/pipeline-users.html", "pages/navbar.html")
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	//	pipelineUsersForm.ErrorText = u.ErrorText

	tmpl.ExecuteTemplate(w, "layout", pipelineUsersForm)
}

func (u *HandlerWrapper) UpdatePipelineUsers(w http.ResponseWriter, r *http.Request) {

	r.ParseForm()
	pipelineId := r.Form["pipelineid"][0]

	selectedUserIds := getSelectedUserIds(r.Form)
	u.Log.Infof("selected user ids %+v\n", selectedUserIds)

	db, err := sql.Open("sqlite3", user.AdminDB.DBPath)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}
	//update the pipeline's list of users, remove all first, then
	// add this list since a user can select and deselect multiples

	// remove all first
	uap := user.UserPipelineAccess{}
	uap.PipelineId = pipelineId

	err = uap.DeleteAll(db)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	// next, add in all the selected users
	//list := make([]user.UserProfile, 0)
	for k, v := range selectedUserIds {
		userProf := user.UserProfile{}
		userProf.Id = k
		userProf.Access = v

		// get the UserProfile for this selection

		up, err := user.Get(k, db)
		if err != nil {
			w.Write([]byte(err.Error()))
			return
		}
		userProf.LastName = up.LastName
		userProf.FirstName = up.FirstName
		userProf.Email = up.Email

		uap := user.UserPipelineAccess{}
		uap.PipelineId = pipelineId
		uap.UserProfileId = k
		uap.Access = v
		err = uap.Create(db)
		if err != nil {
			w.Write([]byte(err.Error()))
			return
		}
	}

	db.Close()

	u.Log.Infof("update pipeline users function..pipeline [%s] form [%+v]\n", pipelineId, r.Form)
	targetURL := fmt.Sprintf("/pipelines/%s", pipelineId)
	http.Redirect(w, r, targetURL, 302)
}

func (u *HandlerWrapper) DeletePipelineUser(w http.ResponseWriter, r *http.Request) {

	vars := mux.Vars(r)
	pipelineId := vars["id"]
	//userId := vars["uid"]

	u.Log.Infof("DeletePipelineUser vars: %+v\n", vars)
	u.Log.Infof("TODO convert to use user.UserPipelineAccess\n")

	targetURL := fmt.Sprintf("/pipelines/%s", pipelineId)
	u.Log.Infof("DeletePipelineUser targetURL is [%s]\n", targetURL)
	http.Redirect(w, r, targetURL, 302)

}

// return a map with the key being the user Id, and the value being
// the access value for that user Id
func getSelectedUserIds(form url.Values) (values map[string]string) {
	values = make(map[string]string)
	for k, _ := range form {
		if strings.Contains(k, "remember-") {
			tmp := strings.Split(k, "-")
			userId := tmp[1]
			values[userId] = "not yet figured out"
		}
	}

	// for any userIds selected, get the access value
	for k, _ := range values {
		accessValue, found := form["access-"+k]
		if found {
			values[k] = accessValue[0]
		}
	}
	return values
}

type PipelineUserForm struct {
	ErrorText    string
	PipelineId   string
	PipelineName string
	User         user.UserProfile
}

// called when a pipeline user is viewed from a pipeline's list of users
func (u *HandlerWrapper) PipelineUser(w http.ResponseWriter, r *http.Request) {
	form := PipelineUserForm{}

	vars := mux.Vars(r)
	pipelineId := vars["id"]
	userId := vars["userid"]
	pipelineName := vars["pipelinename"]

	u.Log.Infof("pipeline: vars %+v\n", vars)

	form.PipelineId = pipelineId
	form.PipelineName = pipelineName

	db, err := sql.Open("sqlite3", user.AdminDB.DBPath)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}
	// get the UserProfile for this user
	form.User, err = user.Get(userId, db)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}
	// get the UserPipelineAccess for this pipeline/user
	var uap user.UserPipelineAccess
	uap, err = user.GetUserPipelineAccess(pipelineId, userId, db)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	form.User.Access = uap.Access

	db.Close()

	tmpl, err := template.ParseFiles("pages/pipeline-user.html", "pages/navbar.html")
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}
	tmpl.ExecuteTemplate(w, "layout", form)

}

func (u *HandlerWrapper) UpdatePipelineUser(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	uap := user.UserPipelineAccess{}
	uap.UserProfileId = vars["uid"]

	r.ParseForm()
	uap.PipelineId = r.Form["pipelineid"][0]
	uap.Access = r.Form["useraccess"][0]

	u.Log.Infof("UpdatePipelineUser called %+v\n", uap)

	db, err := sql.Open("sqlite3", user.AdminDB.DBPath)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}
	err = uap.Update(db)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}
	db.Close()

	targetURL := fmt.Sprintf("/pipelines/%s", uap.PipelineId)
	http.Redirect(w, r, targetURL, 302)
}
