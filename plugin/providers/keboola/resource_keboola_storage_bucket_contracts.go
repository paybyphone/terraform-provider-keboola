package keboola

//StorageBucket is the data model for storage buckets within
//the Keboola Storage API.
type StorageBucket struct {
ID          string `json:"id,omitempty"`
Name        string `json:"name"`
Stage       string `json:"stage"`
Description string `json:"description"`
Backend     string `json:"backend,omitempty"`
}
