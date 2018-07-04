package keboola

//TransformationBucket is the data model for data transformations within
//the Keboola Storage API.
type TransformationBucket struct {
	ID          string `json:"id,omitempty"`
	Name        string `json:"name,omitempty"`
	Description string `json:"description"`
}
