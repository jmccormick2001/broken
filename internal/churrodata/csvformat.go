package churrodata

type CSVRow struct {
	Cols []string `json:"cols"`
}
type CSVFormat struct {
	Path         string   `json:"path"`
	Dataprov     string   `json:"dataprov"`
	Tablename    string   `json:"tablename"`
	PipelineName string   `json:"pipelinename"`
	ColumnNames  []string `json:"columnnames"`
	ColumnTypes  []string `json:"columntypes"`
	Records      []CSVRow `json:"records"`
}
