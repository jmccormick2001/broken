package handlers

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	"github.com/rs/xid"
	"gitlab.com/churro-group/churro/internal/pipeline"
	"gitlab.com/churro-group/churro/internal/user"
	"gitlab.com/churro-group/churro/internal/watch"
	pb "gitlab.com/churro-group/churro/rpc/ctl"
)

type WatchDirectoryForm struct {
	ErrorText    string
	PipelineId   string
	PipelineName string
	WatchDirId   string
	WatchDir     watch.WatchDirectory
	ExtractRules []watch.ExtractRule
}

func (u *HandlerWrapper) ShowCreateWatchDir(w http.ResponseWriter, r *http.Request) {

	u.ErrorText = ""
	vars := mux.Vars(r)
	u.Log.Infof("ShowCreateWatchDir called : vars %+v\n", vars)
	pipelineId := vars["id"]

	db, err := sql.Open("sqlite3", user.AdminDB.DBPath)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}
	a, err := pipeline.Get(pipelineId, db)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}
	db.Close()

	watchDirForm := WatchDirectoryForm{
		PipelineId:   pipelineId,
		PipelineName: a.Name,
		ErrorText:    u.ErrorText,
		WatchDirId:   vars["wdid"]}

	tmpl, err := template.ParseFiles("pages/watchdir-create.html", "pages/navbar.html")
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	tmpl.ExecuteTemplate(w, "layout", watchDirForm)
}

func (u *HandlerWrapper) PipelineWatchDir(w http.ResponseWriter, r *http.Request) {

	wdf := WatchDirectoryForm{}

	vars := mux.Vars(r)
	u.Log.Infof("PipelineWatchDir called: vars %+v\n", vars)
	wdf.PipelineId = vars["id"]
	wdf.WatchDirId = vars["wdid"]

	// get the pipeline connection details
	db, err := sql.Open("sqlite3", user.AdminDB.DBPath)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}
	a, err := pipeline.Get(wdf.PipelineId, db)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}
	db.Close()

	client, err := GetServiceConnection(a.ServiceCrt, a.Name)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	req := pb.GetWatchDirectoryRequest{
		Namespace:  a.Name,
		WatchdirId: wdf.WatchDirId,
	}

	response, err := client.GetWatchDirectory(context.Background(), &req)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	var value watch.WatchDirectory
	err = json.Unmarshal([]byte(response.WatchdirString), &value)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	wdf.WatchDir = value

	wdf.PipelineName = a.Name

	for _, v := range value.ExtractRules {
		wdf.ExtractRules = append(wdf.ExtractRules, v)
	}

	tmpl, err := template.ParseFiles("pages/watchdir.html", "pages/navbar.html")
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}
	wdf.ErrorText = u.ErrorText
	tmpl.ExecuteTemplate(w, "layout", wdf)
}

func (u *HandlerWrapper) CreateWatchDir(w http.ResponseWriter, r *http.Request) {

	r.ParseForm()
	u.Log.Infof("CreateWatchDir called form %+v\n", r.Form)
	pipelineId := r.Form["pipelineid"][0]
	var d watch.WatchDirectory
	d.Id = xid.New().String()
	d.Name = r.Form["watchname"][0]
	d.Path = r.Form["watchpath"][0]
	d.Scheme = r.Form["watchscheme"][0]
	d.Regex = r.Form["watchregex"][0]
	d.Tablename = r.Form["watchtablename"][0]
	pipelineName := r.Form["pipelinename"][0]
	d.LastUpdated = time.Now()
	d.ExtractRules = make(map[string]watch.ExtractRule)

	if d.Path == "" {
		a := HandlerWrapper{ErrorText: "path is blank"}
		a.ShowCreateWatchDir(w, r)
		return
	}
	if d.Scheme == "" {
		a := HandlerWrapper{ErrorText: "scheme is blank"}
		a.ShowCreateWatchDir(w, r)
		return
	}
	if d.Tablename == "" {
		a := HandlerWrapper{ErrorText: "tablename is blank"}
		a.ShowCreateWatchDir(w, r)
		return
	}
	if d.Regex == "" {
		a := HandlerWrapper{ErrorText: "regex is blank"}
		a.ShowCreateWatchDir(w, r)
		return
	}
	if d.Name == "" {
		a := HandlerWrapper{ErrorText: "name is blank"}
		a.ShowCreateWatchDir(w, r)
		return
		//respondWithError(w, http.StatusBadRequest, "Invalid request payload")
	}

	u.Log.Infof("adding new watchdir %+v\n", d)

	db, err := sql.Open("sqlite3", user.AdminDB.DBPath)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}
	a, err := pipeline.Get(pipelineId, db)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}
	db.Close()

	client, err := GetServiceConnection(a.ServiceCrt, pipelineName)
	if err != nil {
		a := HandlerWrapper{ErrorText: err.Error()}
		a.ShowCreateWatchDir(w, r)
		return
	}

	b, _ := json.Marshal(&d)
	req := pb.CreateWatchDirectoryRequest{
		Namespace:      pipelineName,
		WatchdirString: string(b),
	}

	response, err := client.CreateWatchDirectory(context.Background(), &req)
	if err != nil {
		a := HandlerWrapper{ErrorText: err.Error()}
		a.ShowCreateWatchDir(w, r)
		return
	}
	u.Log.Infof("%+v\n", response)

	targetURL := fmt.Sprintf("/pipelines/%s", pipelineId)
	http.Redirect(w, r, targetURL, 302)

}
func (u *HandlerWrapper) UpdateWatchDir(w http.ResponseWriter, r *http.Request) {

	r.ParseForm()
	pipelineId := r.Form["pipelineid"][0]
	pipelineName := r.Form["pipelinename"][0]
	watchDirId := r.Form["watchdirid"][0]

	db, err := sql.Open("sqlite3", user.AdminDB.DBPath)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}
	a, err := pipeline.Get(pipelineId, db)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}
	db.Close()

	client, err := GetServiceConnection(a.ServiceCrt, pipelineName)
	if err != nil {
		a := HandlerWrapper{ErrorText: err.Error()}
		a.PipelineWatchDir(w, r)
		return
	}

	req := pb.GetWatchDirectoryRequest{
		Namespace:  pipelineName,
		WatchdirId: watchDirId}

	response, err := client.GetWatchDirectory(context.Background(), &req)
	u.Log.Infof("%+v\n", response)
	if err != nil {
		a := HandlerWrapper{ErrorText: err.Error()}
		a.PipelineWatchDir(w, r)
		return
	}

	var wdir watch.WatchDirectory
	err = json.Unmarshal([]byte(response.WatchdirString), &wdir)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	wdir.Name = r.Form["name"][0]
	if wdir.Name == "" {
		a := HandlerWrapper{ErrorText: "name is blank"}
		a.PipelineWatchDir(w, r)
		return
	}
	wdir.Path = r.Form["path"][0]
	if wdir.Path == "" {
		a := HandlerWrapper{ErrorText: "path is blank"}
		a.PipelineWatchDir(w, r)
		return
	}
	wdir.Regex = r.Form["regex"][0]
	if wdir.Regex == "" {
		a := HandlerWrapper{ErrorText: "Regex is blank"}
		a.PipelineWatchDir(w, r)
		return
	}
	wdir.Scheme = r.Form["scheme"][0]
	if wdir.Scheme == "" {
		a := HandlerWrapper{ErrorText: "Scheme is blank"}
		a.PipelineWatchDir(w, r)
		return
	}
	wdir.Tablename = r.Form["tablename"][0]
	if wdir.Tablename == "" {
		a := HandlerWrapper{ErrorText: "Tablename is blank"}
		a.PipelineWatchDir(w, r)
		return
	}

	b, _ := json.Marshal(&wdir)
	wreq := pb.UpdateWatchDirectoryRequest{
		Namespace:      pipelineName,
		WatchdirString: string(b),
	}

	resp2, err2 := client.UpdateWatchDirectory(context.Background(), &wreq)
	if err2 != nil {
		a := HandlerWrapper{ErrorText: err.Error()}
		a.PipelineWatchDir(w, r)
		return
	}
	u.Log.Infof("updated watch dir %+v\n", resp2)
	targetURL := fmt.Sprintf("/pipelines/%s", r.Form["pipelineid"][0])
	http.Redirect(w, r, targetURL, 302)
}

func (u *HandlerWrapper) DeleteWatchDir(w http.ResponseWriter, r *http.Request) {

	vars := mux.Vars(r)
	u.Log.Infof("DeleteWatchDir called...vars %+v\n", vars)
	pipelineId := vars["id"]
	watchDirId := vars["wdid"]

	db, err := sql.Open("sqlite3", user.AdminDB.DBPath)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}
	a, err := pipeline.Get(pipelineId, db)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}
	db.Close()

	client, err := GetServiceConnection(a.ServiceCrt, a.Name)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	req := pb.DeleteWatchDirectoryRequest{
		Namespace:  a.Name,
		WatchdirId: watchDirId,
	}

	response, err := client.DeleteWatchDirectory(context.Background(), &req)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}
	u.Log.Infof("%+v\n", response)
	targetURL := fmt.Sprintf("/pipelines/%s", pipelineId)
	http.Redirect(w, r, targetURL, 302)

}
