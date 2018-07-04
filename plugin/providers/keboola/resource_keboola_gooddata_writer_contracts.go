package keboola

type CreateGoodDataProject struct {
	WriterID    string `json:"writerId"`
	Description string `json:"description"`
	AuthToken   string `json:"authToken"`
}

type GoodDataWriter struct {
	ID          string `json:"id,omitempty"`
	Name        string `json:"name"`
	Description string `json:"description"`
}
