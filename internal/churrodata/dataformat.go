package churrodata

import (
	"encoding/json"
	"fmt"
)

type KV struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}
type Row struct {
	Fields []KV `json:"row"`
}
type IntermediateFormat struct {
	Path        string                   `json:"path"`
	Dataprov    string                   `json:"dataprov"`
	ColumnNames []string                 `json:"columnnames"`
	ColumnTypes []string                 `json:"columntypes"`
	Messages    []map[string]interface{} `json:"messages"`
}

func (s IntermediateFormat) String() string {
	b, err := json.MarshalIndent(s, "", "")
	if err != nil {
		fmt.Printf("marshalling error %s\n", err.Error())
		return ""
	}
	return string(b)
}
