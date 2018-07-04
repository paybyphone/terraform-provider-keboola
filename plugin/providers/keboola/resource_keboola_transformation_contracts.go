package keboola

//Input is a mapping from input tables to internal tables for
//use by the transformation queries.
type Input struct {
	Source        string                 `json:"source"`
	Destination   string                 `json:"destination"`
	WhereColumn   string                 `json:"whereColumn,omitempty"`
	WhereOperator string                 `json:"whereOperator,omitempty"`
	WhereValues   []string               `json:"whereValues,omitempty"`
	Indexes       [][]string             `json:"indexes,omitempty"`
	Columns       []string               `json:"columns,omitempty"`
	DataTypes     map[string]interface{} `json:"datatypes,omitempty"`
	Days          int                    `json:"days,omitempty"`
}

//Output is a mapping from the internal tables used by transformation queries
//to output tables.
type Output struct {
	Source              string   `json:"source"`
	Destination         string   `json:"destination"`
	Incremental         bool     `json:"incremental,omitempty"`
	PrimaryKey          []string `json:"primaryKey,omitempty"`
	DeleteWhereValues   []string `json:"deleteWhereValues,omitempty"`
	DeleteWhereOperator string   `json:"deleteWhereOperator,omitempty"`
	DeleteWhereColumn   string   `json:"deleteWhereColumn,omitempty"`
}

//Configuration holds the core configuration for each transformation, as
//it is structured in the Keboola Storage API.
type Configuration struct {
	Input       []Input         `json:"input,omitempty"`
	Output      []Output        `json:"output,omitempty"`
	Queries     []string        `json:"queries,omitempty"`
	ID          string          `json:"id,omitempty"`
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Disabled    bool            `json:"disabled,omitempty"`
	BackEnd     string          `json:"backend"`
	Phase       KBCNumberString `json:"phase"`
	Type        string          `json:"type"`
}

//Transformation is the data model for data transformations within
//the Keboola Storage API.
type Transformation struct {
	ID            string        `json:"id"`
	Configuration Configuration `json:"configuration"`
}
