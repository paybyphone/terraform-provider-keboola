package keboola

type AccessToken struct {
	ID                    string                 `json:"id,omitempty"`
	Description           string                 `json:"description"`
	CreatedAt             KBCTime                `json:"created"`
	CanManageBuckets      bool                   `json:"canManageBuckets"`
	CanManageTokens       bool                   `json:"canManageTokens"`
	CanReadAllFileUploads bool                   `json:"canReadAllFileUploads"`
	ExpiresIn             KBCTime                `json:"expires"`
	ComponentAccess       []string               `json:"componentAccess"`
	BucketPermissions     map[string]interface{} `json:"bucketPermissions"`
}
