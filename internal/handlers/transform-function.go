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
	"gitlab.com/churro-group/churro/internal/transform"
	"gitlab.com/churro-group/churro/internal/user"
	pb "gitlab.com/churro-group/churro/rpc/ctl"
)

func (u *HandlerWrapper) CreateTransformFunction(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	u.Log.Infof("Form was %+v\n", r.Form)

	p := transform.TransformFunction{}
	p.Id = xid.New().String()
	pipelineId := r.Form["pipelineid"][0]
	pipelineName := r.Form["pipelinename"][0]
	p.Name = r.Form["transformname"][0]
	p.Source = r.Form["transformsource"][0]
	p.LastUpdated = time.Now()

	if p.Name == "" {
		a := HandlerWrapper{ErrorText: "transform name is blank"}
		a.ShowCreateTransformFunction(w, r)
		return
	}
	if p.Source == "" {
		a := HandlerWrapper{ErrorText: "transform source is blank"}
		a.ShowCreateTransformFunction(w, r)
		return
	}

	u.Log.Infof("TODO -  get the port from the pipeline\n")

	db, err := sql.Open("sqlite3", user.AdminDB.DBPath)
	if err != nil {
		a := HandlerWrapper{ErrorText: err.Error()}
		a.ShowCreateTransformFunction(w, r)
		return
	}
	a, err := pipeline.Get(pipelineId, db)
	if err != nil {
		a := HandlerWrapper{ErrorText: err.Error()}
		a.ShowCreateTransformFunction(w, r)
	}

	db.Close()

	u.Log.Infof("pipelineId: %s\n", pipelineId)
	client, err := GetServiceConnection(a.ServiceCrt, pipelineName)
	if err != nil {
		a := HandlerWrapper{ErrorText: err.Error()}
		a.ShowCreateTransformFunction(w, r)
		return
	}

	req := pb.CreateTransformFunctionRequest{}
	req.Namespace = pipelineName
	b, _ := json.Marshal(&p)
	req.FunctionString = string(b)

	response, err := client.CreateTransformFunction(context.Background(), &req)
	if err != nil {
		a := HandlerWrapper{ErrorText: err.Error()}
		a.ShowCreateTransformFunction(w, r)
		return
	}

	u.Log.Infof("created function %+v\n", response)

	targetURL := fmt.Sprintf("/pipelines/%s", pipelineId)
	http.Redirect(w, r, targetURL, 302)

}

type FunctionFormValue struct {
	FunctionName string
	Selected     string
}

type TransformFunctionForm struct {
	PipelineName string
	PipelineId   string
	Functions    []FunctionFormValue
	ErrorText    string
}

func (u *HandlerWrapper) ShowCreateTransformFunction(w http.ResponseWriter, r *http.Request) {

	u.ErrorText = ""

	vars := mux.Vars(r)
	u.Log.Infof("show-create-transform-function pipeline: id %s\n", vars["id"])
	transformFunctionForm := TransformFunctionForm{
		PipelineId:   vars["id"],
		PipelineName: vars["pipelinename"]}
	u.Log.Infof("show-create-transform-function pipeline: tfid %s\n", vars["tfid"])

	tmpl, err := template.ParseFiles("pages/transform-function-create.html", "pages/navbar.html")
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	transformFunctionForm.ErrorText = u.ErrorText

	tmpl.ExecuteTemplate(w, "layout", transformFunctionForm)
}

type FunctionForm struct {
	ErrorText    string
	PipelineId   string
	PipelineName string
	Function     transform.TransformFunction
}

func (u *HandlerWrapper) TransformFunction(w http.ResponseWriter, r *http.Request) {

	ff := FunctionForm{}
	vars := mux.Vars(r)
	u.Log.Infof("vars here are %+v\n", vars)
	u.Log.Infof("r here are %+v\n", r)
	pipelineId := vars["id"]
	functionId := vars["tfid"]
	pipelineName := vars["pipelinename"]

	ff.PipelineId = pipelineId
	ff.PipelineName = pipelineName

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
		w.Write([]byte(err.Error()))
		return
	}

	req := pb.GetTransformFunctionRequest{
		Namespace:  pipelineName,
		FunctionId: functionId}

	response, err := client.GetTransformFunction(context.Background(), &req)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	var function transform.TransformFunction
	err = json.Unmarshal([]byte(response.FunctionString), &function)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}
	u.Log.Infof("got function %+v\n", function)
	ff.Function = function

	tmpl, err := template.ParseFiles("pages/transform-function.html", "pages/navbar.html")
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}
	ff.ErrorText = u.ErrorText
	tmpl.ExecuteTemplate(w, "layout", ff)
}

func (u *HandlerWrapper) UpdateTransformFunction(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	f := transform.TransformFunction{Name: r.Form["transformname"][0]}
	pipelineId := r.Form["pipelineid"][0]
	pipelineName := r.Form["pipelinename"][0]
	if f.Name == "" {
		a := HandlerWrapper{ErrorText: "transform name can not be blank"}
		a.TransformFunction(w, r)
		return
	}
	f.Source = r.Form["transformsource"][0]
	if f.Source == "" {
		a := HandlerWrapper{ErrorText: "transform source can not be blank"}
		a.TransformFunction(w, r)
		return
	}

	f.Id = r.Form["functionid"][0]

	db, err := sql.Open("sqlite3", user.AdminDB.DBPath)
	if err != nil {
		a := HandlerWrapper{ErrorText: err.Error()}
		a.TransformFunction(w, r)
	}
	a, err := pipeline.Get(pipelineId, db)
	if err != nil {
		a := HandlerWrapper{ErrorText: err.Error()}
		a.TransformFunction(w, r)
	}

	db.Close()

	client, err := GetServiceConnection(a.ServiceCrt, pipelineName)
	if err != nil {
		a := HandlerWrapper{ErrorText: err.Error()}
		a.TransformFunction(w, r)
		return
	}

	b, _ := json.Marshal(&f)
	req := pb.UpdateTransformFunctionRequest{Namespace: pipelineName,
		FunctionString: string(b)}

	response, err := client.UpdateTransformFunction(context.Background(), &req)
	if err != nil {
		a := HandlerWrapper{ErrorText: err.Error()}
		a.TransformFunction(w, r)
		return
	}
	u.Log.Infof("%+v\n", response)

	u.Log.Infof("updated transform function..")
	targetURL := fmt.Sprintf("/pipelines/%s", pipelineId)
	http.Redirect(w, r, targetURL, 302)
}

func (u *HandlerWrapper) DeleteTransformFunction(w http.ResponseWriter, r *http.Request) {

	// TransformFunctions
	vars := mux.Vars(r)
	pipelineId := vars["id"]
	functionId := vars["tfid"]

	u.Log.Infof("delete transformfunction method called vars %+v\n", vars)

	// look up the pipeline name from the admin database
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

	req := pb.DeleteTransformFunctionRequest{}
	req.Namespace = a.Name
	req.FunctionId = functionId

	response, err := client.DeleteTransformFunction(context.Background(), &req)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}
	u.Log.Infof("%+v\n", response)
	url := fmt.Sprintf("/pipelines/%s", pipelineId)
	http.Redirect(w, r, url, 302)

}

func getTransformFunctions(serviceCrt, pipelineName string) (list []transform.TransformFunction, err error) {
	var client pb.CtlClient
	client, err = GetServiceConnection(serviceCrt, pipelineName)
	if err != nil {
		return list, err
	}
	req := pb.GetTransformFunctionsRequest{Namespace: pipelineName}

	response, err := client.GetTransformFunctions(context.Background(), &req)
	if err != nil {
		return list, err
	}
	byt := []byte(response.FunctionsString)
	err = json.Unmarshal(byt, &list)
	if err != nil {
		return list, err
	}
	return list, nil
}
