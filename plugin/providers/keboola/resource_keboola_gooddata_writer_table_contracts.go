package keboola

type GoodDataColumn struct {
	Name            string `json:"name"`
	DataType        string `json:"dataType"`
	DataTypeSize    string `json:"dataTypeSize"`
	SchemaReference string `json:"schemaReference"`
	Reference       string `json:"reference"`
	SortOrder       string `json:"sortOrder"`
	SortLabel       string `json:"sortLabel"`
	Format          string `json:"format"`
	DateDimension   string `json:"dateDimension"`
	Title           string `json:"title"`
	Type            string `json:"type"`
}

type GoodDataTable struct {
	ID              string                    `json:"tableId,omitempty"`
	Title           string                    `json:"title"`
	Export          bool                      `json:"export"`
	Identifier      string                    `json:"identifier"`
	IncrementalDays KBCBooleanNumber          `json:"incrementalLoad"`
	Columns         map[string]GoodDataColumn `json:"columns"`
}
