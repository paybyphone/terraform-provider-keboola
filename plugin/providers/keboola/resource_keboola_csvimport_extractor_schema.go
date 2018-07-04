package keboola

type CSVImportExtractor struct {
	ID            string            `json:"id"`
	Name          string            `json:"name"`
	Description   string            `json:"description"`
	Configuration CSVUploadSettings `json:"configuration"`
}

type CSVUploadSettings struct {
	Destination string   `json:"destination"`
	Incremental bool     `json:"incremental"`
	PrimaryKey  []string `json:"primaryKey"`
	Delimiter   string   `json:"delimiter"`
	Enclosure   string   `json:"enclosure"`
}
