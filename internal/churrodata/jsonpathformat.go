package churrodata

type JsonPathRow struct {
	Cols []string `json:"cols"`
}
type JsonPathFormat struct {
	Path         string        `json:"path"`
	Dataprov     string        `json:"dataprov"`
	Tablename    string        `json:"tablename"`
	PipelineName string        `json:"pipelinename"`
	ColumnNames  []string      `json:"columnnames"`
	ColumnTypes  []string      `json:"columntypes"`
	Records      []JsonPathRow `json:"records"`
}
