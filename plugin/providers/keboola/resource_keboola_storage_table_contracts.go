package keboola

//StorageTable is the data model for Storage Tables within
//the Keboola Storage API.
type StorageTable struct {
	ID             string   `json:"id,omitempty"`
	Name           string   `json:"name"`
	Delimiter      string   `json:"delimiter"`
	Enclosure      string   `json:"enclosure,omitempty"`
	Transactional  bool     `json:"transactional,omitempty"`
	Columns        []string `json:"columns"`
	PrimaryKey     []string `json:"primaryKey"`
	IndexedColumns []string `json:"indexedColumns"`
}

//UploadFileResult contains the id of the CSV file uploaded to AWS S3.
type UploadFileResult struct {
	ID int `json:"id"`
}
