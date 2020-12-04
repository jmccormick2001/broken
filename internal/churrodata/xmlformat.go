package churrodata

type XMLRow struct {
	Cols []string `json:"cols"`
}
type XMLFormat struct {
	Path         string   `json:"path"`
	Dataprov     string   `json:"dataprov"`
	Tablename    string   `json:"tablename"`
	PipelineName string   `json:"pipelinename"`
	ColumnNames  []string `json:"columnnames"`
	ColumnTypes  []string `json:"columntypes"`
	Records      []XMLRow `json:"records"`
}
