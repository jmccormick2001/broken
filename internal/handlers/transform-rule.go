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

func (u *HandlerWrapper) ShowCreateTransformRule(w http.ResponseWriter, r *http.Request) {

	u.ErrorText = ""

	// TransformFunctions
	vars := mux.Vars(r)
	u.Log.Infof("show-create-transform-rule pipeline: vars %+v\n", vars)
	tfForm := TransformFunctionForm{
		PipelineId:   vars["id"],
		PipelineName: vars["pipelinename"],
	}

	tmpl, err := template.ParseFiles("pages/transform-rule-create.html", "pages/navbar.html")
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}
	//check to make sure there are transform functions defined

	// get the pipeline
	db, err := sql.Open("sqlite3", user.AdminDB.DBPath)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	a, err := pipeline.Get(tfForm.PipelineId, db)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	db.Close()

	var functions []transform.TransformFunction
	functions, err = getTransformFunctions(a.ServiceCrt, tfForm.PipelineName)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	if len(functions) == 0 {
		u.ErrorText = "transform functions need to be defined"
	} else {
		tfForm.Functions = make([]FunctionFormValue, 0)
		for i := 0; i < len(functions); i++ {
			f := FunctionFormValue{
				FunctionName: functions[i].Name,
				Selected:     "",
			}
			tfForm.Functions = append(tfForm.Functions, f)
		}
	}

	tfForm.ErrorText = u.ErrorText
	tmpl.ExecuteTemplate(w, "layout", tfForm)
}

type TransformRuleForm struct {
	ErrorText    string
	PipelineId   string
	PipelineName string
	Functions    []FunctionFormValue
	Rule         transform.TransformRule
}

func (u *HandlerWrapper) TransformRule(w http.ResponseWriter, r *http.Request) {

	u.ErrorText = ""

	vars := mux.Vars(r)
	u.Log.Infof("TransformRule: vars %+v\n", vars)

	f := TransformRuleForm{PipelineId: vars["id"],
		PipelineName: vars["pipelinename"]}

	// get the pipeline
	db, err := sql.Open("sqlite3", user.AdminDB.DBPath)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	a, err := pipeline.Get(f.PipelineId, db)
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

	req := pb.GetTransformRuleRequest{}
	req.Namespace = f.PipelineName
	req.RuleId = vars["trid"]

	response, err := client.GetTransformRule(context.Background(), &req)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	var rule transform.TransformRule
	err = json.Unmarshal([]byte(response.RuleString), &rule)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}
	u.Log.Infof("got rule %+v\n", rule)

	f.Rule = rule

	var functions []transform.TransformFunction
	functions, err = getTransformFunctions(a.ServiceCrt, f.PipelineName)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	if len(functions) == 0 {
		u.ErrorText = "transform functions need to be defined"
	} else {
		f.Functions = make([]FunctionFormValue, 0)
		for i := 0; i < len(functions); i++ {
			x := FunctionFormValue{}
			x.FunctionName = functions[i].Name
			x.Selected = ""
			if rule.TransformFunctionName == functions[i].Name {
				x.Selected = "selected"
			}
			f.Functions = append(f.Functions, x)
		}
	}

	u.Log.Infof("functions length to the form is %d\n", len(f.Functions))

	f.PipelineId = vars["id"]

	tmpl, err := template.ParseFiles("pages/transform-rule.html", "pages/navbar.html")
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}
	f.ErrorText = u.ErrorText
	tmpl.ExecuteTemplate(w, "layout", f)
}

func (u *HandlerWrapper) DeleteTransformRule(w http.ResponseWriter, r *http.Request) {

	vars := mux.Vars(r)
	u.Log.Infof("DeleteTransformRule called...%+v\n", vars)
	pipelineId := vars["id"]

	// get the pipeline
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

	req := pb.DeleteTransformRuleRequest{
		Namespace: a.Name,
		RuleId:    vars["trid"],
	}

	_, err = client.DeleteTransformRule(context.Background(), &req)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}
	url := fmt.Sprintf("/pipelines/%s", vars["id"])
	http.Redirect(w, r, url, 302)
}

func (u *HandlerWrapper) UpdateTransformRule(w http.ResponseWriter, r *http.Request) {

	u.Log.Infof("UpdatePipelineTransformRule called...\n")
	r.ParseForm()
	p := transform.TransformRule{Id: r.Form["id"][0]}
	pipelineId := r.Form["pipelineid"][0]
	if pipelineId == "" {
		a := HandlerWrapper{ErrorText: "pipeline id can not be blank"}
		a.TransformRule(w, r)
		return
	}
	p.Name = r.Form["transformrulename"][0]
	if p.Name == "" {
		a := HandlerWrapper{ErrorText: "rule name can not be blank"}
		a.TransformRule(w, r)
		return
	}
	p.Path = r.Form["transformrulepath"][0]
	if p.Path == "" {
		a := HandlerWrapper{ErrorText: "rule path can not be blank"}
		a.TransformRule(w, r)
		return
	}
	p.Scheme = r.Form["transformrulescheme"][0]
	if p.Scheme == "" {
		a := HandlerWrapper{ErrorText: "rule Scheme can not be blank"}
		a.TransformRule(w, r)
		return
	}
	p.TransformFunctionName = r.Form["transformfunctionname"][0]
	if p.TransformFunctionName == "" {
		a := HandlerWrapper{ErrorText: "rule function name can not be blank"}
		a.TransformRule(w, r)
		return
	}
	p.LastUpdated = time.Now()

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
		a := HandlerWrapper{ErrorText: err.Error()}
		a.TransformRule(w, r)
		return
	}

	req := pb.UpdateTransformRuleRequest{Namespace: a.Name}

	b, _ := json.Marshal(&p)
	req.RuleString = string(b)

	response, err := client.UpdateTransformRule(context.Background(), &req)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}
	u.Log.Infof("updated transform rule %+v\n", response)
	targetURL := fmt.Sprintf("/pipelines/%s", pipelineId)
	http.Redirect(w, r, targetURL, 302)
}

func (u *HandlerWrapper) CreateTransformRule(w http.ResponseWriter, r *http.Request) {

	r.ParseForm()
	u.Log.Infof("Form was %+v\n", r.Form)

	p := transform.TransformRule{}
	p.Id = xid.New().String()
	pipelineId := r.Form["pipelineid"][0]
	p.Name = r.Form["transformrulename"][0]
	p.Path = r.Form["transformrulepath"][0]
	p.Scheme = r.Form["transformrulescheme"][0]
	p.TransformFunctionName = r.Form["transformfunctionname"][0]
	p.LastUpdated = time.Now()

	if p.Path == "" {
		a := HandlerWrapper{ErrorText: "path is blank"}
		a.ShowCreateTransformRule(w, r)
		return
	}
	if p.Name == "" {
		a := HandlerWrapper{ErrorText: "name is blank"}
		a.ShowCreateTransformRule(w, r)
		return
	}

	b, _ := json.Marshal(&p)
	req := pb.CreateTransformRuleRequest{RuleString: string(b)}

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

	req.Namespace = a.Name

	client, err := GetServiceConnection(a.ServiceCrt, a.Name)
	if err != nil {
		a := HandlerWrapper{ErrorText: err.Error()}
		a.ShowCreateTransformRule(w, r)
		return
	}

	response, err := client.CreateTransformRule(context.Background(), &req)
	fmt.Printf("+%v\n", response)
	if err != nil {
		a := HandlerWrapper{ErrorText: err.Error()}
		a.ShowCreateTransformRule(w, r)
		return
	}

	targetURL := fmt.Sprintf("/pipelines/%s", pipelineId)
	http.Redirect(w, r, targetURL, 302)

}
