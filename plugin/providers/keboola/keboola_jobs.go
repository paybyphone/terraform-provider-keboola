package keboola

//StorageJobStatus contains the job status and results for the table load.
type StorageJobStatus struct {
	ID      int    `json:"id"`
	URL     string `json:"url"`
	Status  string `json:"status"`
	Results struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	} `json:"results"`
}

//SyrupJobStatus contains the job status and results for syrup api based jobs.
type SyrupJobStatus struct {
	ID     int    `json:"id"`
	URL    string `json:"url"`
	Status string `json:"status"`
	// Result struct {
	// 	ID   string `json:"id"`
	// 	Name string `json:"name"`
	// } `json:"result"`
}
