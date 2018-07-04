package keboola

type GoodDataUserManagementParameters struct {
	Writer string `json:"gd_writer"`
}

type GoodDataUserManagementInput struct {
	Tables []Input `json:"tables,omitempty"`
}

type GoodDataUserManagementOutput struct {
	Tables []Output `json:"tables,omitempty"`
}

type GoodDataUserManagementStorage struct {
	Input  GoodDataUserManagementInput  `json:"input"`
	Output GoodDataUserManagementOutput `json:"output"`
}

type GoodDataUserManagementConfiguration struct {
	Storage    GoodDataUserManagementStorage    `json:"storage"`
	Parameters GoodDataUserManagementParameters `json:"parameters"`
}

type GoodDataUserManagement struct {
	ID            string                              `json:"id,omitempty"`
	Name          string                              `json:"name"`
	Description   string                              `json:"description,omitempty"`
	Configuration GoodDataUserManagementConfiguration `json:"configuration"`
}
