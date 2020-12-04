package churrodata

type XLSRow struct {
	Cols []string `json:"cols"`
}
type XLSFormat struct {
	Path         string   `json:"path"`
	Dataprov     string   `json:"dataprov"`
	Tablename    string   `json:"tablename"`
	PipelineName string   `json:"pipelinename"`
	ColumnNames  []string `json:"columnnames"`
	ColumnTypes  []string `json:"columntypes"`
	Records      []XLSRow `json:"records"`
}
