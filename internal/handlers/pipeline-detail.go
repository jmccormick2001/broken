package handlers

import (
	"context"
	"database/sql"
	"encoding/json"
	"html/template"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
	"gitlab.com/churro-group/churro/api/v1alpha1"
	"gitlab.com/churro-group/churro/internal/pipeline"
	"gitlab.com/churro-group/churro/internal/transform"
	"gitlab.com/churro-group/churro/internal/user"
	"gitlab.com/churro-group/churro/internal/watch"
	pb "gitlab.com/churro-group/churro/rpc/ctl"
)

// PipelineDetail is a wrapper around a bunch of pipeline data
// we use this as a model for the web UI only
type PipelineDetail struct {
	Id                 string
	Name               string
	Port               string
	Pipeline           v1alpha1.Pipeline
	TransformFunctions []transform.TransformFunction
	Users              []user.UserProfile
	TransformRules     []transform.TransformRule
	WatchDirectories   []watch.WatchDirectory
}

func (u *HandlerWrapper) PipelineDetailHandler(w http.ResponseWriter, r *http.Request) {

	vars := mux.Vars(r)
	pipelineId := vars["id"]
	u.Log.Infof("pipeline detail: id %s\n", pipelineId)

	var err error
	pipelineDetail := PipelineDetail{}

	pipelineDetail.Id = pipelineId

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

	// get the pipeline users

	usersList, err := user.GetUsersForPipeline(pipelineId, db)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	db.Close()

	pipelineDetail.Users = usersList

	byt := []byte(a.Cr)
	var p v1alpha1.Pipeline
	err = json.Unmarshal(byt, &p)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	pipelineDetail.Pipeline = p
	u.Log.Infof("jeff setting pipeline name to %s\n", a.Name)
	pipelineDetail.Name = a.Name
	pipelineDetail.Port = strconv.Itoa(a.Port)

	// get the pipeline info from the pipeline's ctl service
	client, err := GetServiceConnection(a.ServiceCrt, a.Name)
	if err != nil {
		//w.Write([]byte(err.Error()))
		u.Log.Infof("error connecting to pipeline ctl service %s\n", err.Error())
	} else {
		gtfReq := pb.GetTransformFunctionsRequest{Namespace: pipelineDetail.Name}
		gtfResponse, err := client.GetTransformFunctions(context.Background(), &gtfReq)
		if err != nil {
			w.Write([]byte(err.Error()))
			return
		}
		byt = []byte(gtfResponse.FunctionsString)
		var list []transform.TransformFunction
		err = json.Unmarshal(byt, &list)
		if err != nil {
			w.Write([]byte(err.Error()))
			return
		}
		pipelineDetail.TransformFunctions = list

		gtrReq := pb.GetTransformRulesRequest{Namespace: pipelineDetail.Name}
		gtrResponse, err := client.GetTransformRules(context.Background(), &gtrReq)
		if err != nil {
			w.Write([]byte(err.Error()))
			return
		}

		var values []transform.TransformRule
		err = json.Unmarshal([]byte(gtrResponse.RulesString), &values)
		if err != nil {
			w.Write([]byte(err.Error()))
			return
		}

		pipelineDetail.TransformRules = values

		gwdRequest := pb.GetWatchDirectoriesRequest{Namespace: pipelineDetail.Name}

		gwdResponse, err := client.GetWatchDirectories(context.Background(), &gwdRequest)
		if err != nil {
			w.Write([]byte(err.Error()))
			return
		}

		var watchDirs []watch.WatchDirectory
		err = json.Unmarshal([]byte(gwdResponse.WatchdirsString), &watchDirs)
		if err != nil {
			w.Write([]byte(err.Error()))
			return
		}
		pipelineDetail.WatchDirectories = watchDirs
	}

	u.Log.Infof("functions %d rules %d dirs %d\n", len(pipelineDetail.TransformFunctions), len(pipelineDetail.TransformRules), len(pipelineDetail.WatchDirectories))

	tmpl, err := template.ParseFiles("pages/pipeline-detail.html", "pages/navbar.html")
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	tmpl.ExecuteTemplate(w, "layout", pipelineDetail)
}

func (u *HandlerWrapper) UpdatePipelineDetail(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()

	pipelineId := r.Form["pipelineid"][0]

	if pipelineId == "" || r.Form["loaderpctheadroom"][0] == "" || r.Form["loaderqueuesize"][0] == "" {
		respondWithError(w, http.StatusBadRequest, "Invalid request payload")
		return
	}

	i, err := strconv.Atoi(r.Form["loaderqueuesize"][0])
	if err != nil {
		respondWithError(w, http.StatusBadRequest, "Invalid loaderQueueSize")
		return
	}
	loaderQueueSize := i

	i, err = strconv.Atoi(r.Form["loaderpctheadroom"][0])
	if err != nil {
		respondWithError(w, http.StatusBadRequest, "Invalid loaderPctHeadRoom")
		return
	}
	loaderPctHeadRoom := i

	u.Log.Infof("updating queuesize=%d headroom=%d\n", loaderQueueSize, loaderPctHeadRoom)

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

	//update the CR before updating
	byt := []byte(a.Cr)
	var p v1alpha1.Pipeline
	err = json.Unmarshal(byt, &p)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	p.Spec.LoaderConfig.QueueSize = loaderQueueSize
	p.Spec.LoaderConfig.PctHeadRoom = loaderPctHeadRoom

	b, _ := json.Marshal(&p)
	a.Cr = string(b)

	err = a.Update(db)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}
	db.Close()

	http.Redirect(w, r, "/", 302)
}

func (u *HandlerWrapper) CreatePipelineDetail(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()

	p := v1alpha1.Pipeline{}

	p.ObjectMeta.Name = r.Form["pipelinename"][0]
	port := r.Form["portnumber"][0]
	p.ObjectMeta.Labels = make(map[string]string)
	p.ObjectMeta.Labels["name"] = p.ObjectMeta.Name

	if p.ObjectMeta.Name == "" {
		a := HandlerWrapper{}
		a.ErrorText = "pipeline name is blank"
		a.ShowCreatePipeline(w, r)
		return
	}

	portNumber, err := strconv.Atoi(port)
	if err != nil {
		a := HandlerWrapper{}
		a.ErrorText = "port is required to be integer"
		a.ShowCreatePipeline(w, r)
		return
	}

	i, err := strconv.Atoi(r.Form["loaderpctheadroom"][0])
	if err != nil {
		a := HandlerWrapper{}
		a.ErrorText = "head room is required to be integer, in the range of 25-80"
		a.ShowCreatePipeline(w, r)
		return
	}
	p.Spec.Port = portNumber
	p.Spec.LoaderConfig.PctHeadRoom = i
	i, err = strconv.Atoi(r.Form["loaderqueuesize"][0])
	if err != nil {
		a := HandlerWrapper{}
		a.ErrorText = "queue size is required to be integer, in the range of 25-80"
		a.ShowCreatePipeline(w, r)
		return
	}
	p.Spec.LoaderConfig.QueueSize = i

	db, err := sql.Open("sqlite3", user.AdminDB.DBPath)
	if err != nil {
		a := HandlerWrapper{}
		a.ErrorText = err.Error()
		a.ShowCreatePipeline(w, r)
		return
	}

	x := pipeline.Pipeline{}
	x.Name = p.ObjectMeta.Name
	x.Port = portNumber
	b, _ := json.Marshal(&p)
	x.Cr = string(b)
	x.Id, err = x.Create(db)

	// create the CR to launch the pipeline on k8s
	u.Log.Infof("about to run pipeline.CreatePipeline\n")
	err = pipeline.CreatePipeline(u.Log, p)
	if err != nil {
		a := HandlerWrapper{}
		a.ErrorText = err.Error()
		a.ShowCreatePipeline(w, r)
		return
	}

	// get the newly created CR since it contains the generated
	// servicecrt credential that we want to cache in the admindb

	updated, err := pipeline.GetPipeline(p.ObjectMeta.Name)
	if err != nil {
		a := HandlerWrapper{}
		a.ErrorText = err.Error()
		a.ShowCreatePipeline(w, r)
		return
	}

	c, _ := json.Marshal(&updated)
	x.Cr = string(c)
	x.ServiceCrt = updated.Spec.ServiceCredentials.ServiceCrt

	x.Update(db)

	db.Close()

	// here for the mock, we will create a Ctl server on this port
	// to simulate creating the CR in a unique namespace
	//go ctl.StartCtlServer(p, true, x.Name, "", "")

	u.Log.Info("created new pipeline..")
	http.Redirect(w, r, "/", 302)
}

func (u *HandlerWrapper) DeletePipelineDetail(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	pipelineId := vars["id"]
	u.Log.Infof("DeletePipelineDetail called %s\n", pipelineId)

	a := pipeline.Pipeline{}
	a.Id = pipelineId

	db, err := sql.Open("sqlite3", user.AdminDB.DBPath)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	a, err = pipeline.Get(pipelineId, db)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	err = a.Delete(db)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}
	db.Close()

	// delete the CR for this pipeline
	err = pipeline.DeletePipeline(u.Log, a.Name)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	http.Redirect(w, r, "/", 302)
}

func respondWithError(w http.ResponseWriter, code int, message string) {
	respondWithJSON(w, code, map[string]string{"error": message})
}

func respondWithJSON(w http.ResponseWriter, code int, payload interface{}) {
	response, _ := json.Marshal(payload)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	w.Write(response)
}

func (u *HandlerWrapper) PipelineDownloadFile(w http.ResponseWriter, r *http.Request) {

	vars := mux.Vars(r)
	pipelineId := vars["id"]
	fname := vars["fname"]
	u.Log.Infof("pipeline detail: id %s file %s\n", pipelineId, fname)

	w.Write([]byte("Gorilla!\n"))
}

type ShowCreatePipelineForm struct {
	ErrorText string
}

func (u *HandlerWrapper) ShowCreatePipeline(w http.ResponseWriter, r *http.Request) {
	u.Log.Infof("ErrorText in ShowCreatePipeline is %s\n", u.ErrorText)
	tmpl, err := template.ParseFiles("pages/pipeline-create.html", "pages/navbar.html")
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}
	x := ShowCreatePipelineForm{ErrorText: u.ErrorText}
	tmpl.ExecuteTemplate(w, "layout", x)

}
