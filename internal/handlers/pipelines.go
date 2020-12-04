package handlers

import (
	"database/sql"
	"encoding/json"

	"gitlab.com/churro-group/churro/api/v1alpha1"
	"gitlab.com/churro-group/churro/internal/pipeline"
	"gitlab.com/churro-group/churro/internal/user"

	"html/template"
	"net/http"
)

type PipelinesPage struct {
	List v1alpha1.PipelineList
}

func (u *HandlerWrapper) Pipelines(w http.ResponseWriter, r *http.Request) {
	pageValues := PipelinesPage{}

	db, err := sql.Open("sqlite3", user.AdminDB.DBPath)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	var list []pipeline.Pipeline
	list, err = pipeline.GetAll(db)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	for i := 0; i < len(list); i++ {
		var va v1alpha1.Pipeline
		byt := []byte(list[i].Cr)
		err = json.Unmarshal(byt, &va)
		if err != nil {
			w.Write([]byte(err.Error()))
			return
		}
		va.Spec.Id = list[i].Id
		pageValues.List.Items = append(pageValues.List.Items, va)
	}

	u.Log.Infof("%d pipelines read\n", len(pageValues.List.Items))

	tmpl, err := template.ParseFiles("pages/home.html", "pages/navbar.html")
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	tmpl.ExecuteTemplate(w, "layout", &pageValues)
}
