package handlers

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"html/template"

	"github.com/gorilla/mux"
	"github.com/rs/xid"

	"net/http"
	"strconv"
	"time"

	"gitlab.com/churro-group/churro/api/v1alpha1"
	"gitlab.com/churro-group/churro/internal/pipeline"
	"gitlab.com/churro-group/churro/internal/user"
	"gitlab.com/churro-group/churro/internal/watch"
	pb "gitlab.com/churro-group/churro/rpc/ctl"
)

type ExtractRuleForm struct {
	ErrorText     string
	PipelineId    string
	PipelineName  string
	WatchDirId    string
	ExtractRuleId string
}

func (u *HandlerWrapper) UpdateExtractRule(w http.ResponseWriter, r *http.Request) {

	vars := mux.Vars(r)
	id, err := strconv.Atoi(vars["id"])
	if err != nil {
		respondWithError(w, http.StatusBadRequest, "Invalid product ID")
		return
	}
	u.Log.Infof("id %d\n", id)

	var p v1alpha1.Pipeline
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&p); err != nil {
		respondWithError(w, http.StatusBadRequest, "Invalid request payload")
		return
	}
	defer r.Body.Close()

	respondWithJSON(w, http.StatusOK, p)

}
func (u *HandlerWrapper) DeleteExtractRule(w http.ResponseWriter, r *http.Request) {

	vars := mux.Vars(r)
	req := pb.DeleteExtractRuleRequest{}
	req.Namespace = vars["pipelinename"]
	req.WatchdirId = vars["wdid"]
	req.ExtractRuleId = vars["rid"]

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

	client, err := GetServiceConnection(a.ServiceCrt, req.Namespace)
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, "error DeleteExtractRule "+err.Error())
		return
	}

	response, err := client.DeleteExtractRule(context.Background(), &req)
	u.Log.Infof("%+v\n", response)
	if err != nil {
		respondWithError(w, http.StatusInternalServerError, "error DeleteExtractRule "+err.Error())
		return
	}

	targetURL := fmt.Sprintf("/pipelines/%s/watchdirs/%s?pipelinename=%s",
		pipelineId, req.WatchdirId, req.Namespace)
	http.Redirect(w, r, targetURL, 302)
}

func (u *HandlerWrapper) ShowCreateExtractRule(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	u.Log.Infof("ShowCreateExtractRule called vars %+v\n", vars)

	extractRuleForm := ExtractRuleForm{}
	extractRuleForm.PipelineId = vars["id"]
	extractRuleForm.PipelineName = vars["pipelinename"]
	extractRuleForm.WatchDirId = vars["wdid"]
	extractRuleForm.ExtractRuleId = vars["rid"]

	tmpl, err := template.ParseFiles("pages/extract-rules-create.html", "pages/navbar.html")
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}
	extractRuleForm.ErrorText = u.ErrorText
	tmpl.ExecuteTemplate(w, "layout", extractRuleForm)

}

func (u *HandlerWrapper) CreateExtractRule(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	u.Log.Infof("CreateExtractRule called vars %+v\n", vars)
	pipelineId := vars["id"]
	watchDirId := vars["wdid"]

	r.ParseForm()
	u.Log.Infof("Form was %+v\n", r.Form)

	p := watch.ExtractRule{}
	p.Id = xid.New().String()
	p.WatchDirectoryId = watchDirId
	p.ColumnName = r.Form["columnname"][0]
	p.RuleSource = r.Form["rulesource"][0]
	p.MatchValues = r.Form["matchvalues"][0]
	p.LastUpdated = time.Now()
	pipelineName := r.Form["pipelinename"][0]

	u.Log.Infof("create the following...\n")
	u.Log.Infof("%+v\n", p)

	if p.ColumnName == "" {
		a := HandlerWrapper{}
		a.ErrorText = "column name is blank"
		a.ShowCreateExtractRule(w, r)
		return
	}
	if p.RuleSource == "" {
		a := HandlerWrapper{}
		a.ErrorText = "rule source is blank"
		a.ShowCreateExtractRule(w, r)
		return
	}

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
		a := HandlerWrapper{}
		a.ErrorText = err.Error()
		a.ShowCreateExtractRule(w, r)
		return
	}

	req := pb.CreateExtractRuleRequest{}
	req.Namespace = a.Name
	b, _ := json.Marshal(&p)
	req.ExtractRuleString = string(b)

	response, err := client.CreateExtractRule(context.Background(), &req)
	if err != nil {
		a := HandlerWrapper{}
		a.ErrorText = err.Error()
		a.ShowCreateExtractRule(w, r)
		return
	}
	u.Log.Infof("%+v\n", response)
	targetUrl := fmt.Sprintf("/pipelines/%s/watchdirs/%s?pipelinename=%s", pipelineId, watchDirId, pipelineName)
	http.Redirect(w, r, targetUrl, 302)

}
