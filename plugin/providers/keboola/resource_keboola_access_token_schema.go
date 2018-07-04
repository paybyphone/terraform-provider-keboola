package keboola

import "github.com/hashicorp/terraform/helper/schema"

func resourceKeboolaAccessToken() *schema.Resource {
	return &schema.Resource{
		Create: resourceKeboolaAccessTokenCreate,
		Read:   resourceKeboolaAccessTokenRead,
		Update: resourceKeboolaAccessTokenUpdate,
		Delete: resourceKeboolaAccessTokenDelete,
		Importer: &schema.ResourceImporter{
			State: schema.ImportStatePassthrough,
		},

		Schema: map[string]*schema.Schema{
			"description": {
				Type:     schema.TypeString,
				Required: true,
			},
			"can_manage_buckets": {
				Type:     schema.TypeBool,
				Optional: true,
				ForceNew: true,
				Default:  false,
			},
			"can_manage_tokens": {
				Type:     schema.TypeBool,
				Optional: true,
				ForceNew: true,
				Default:  false,
			},
			"can_read_all_file_uploads": {
				Type:     schema.TypeBool,
				Optional: true,
				ForceNew: true,
				Default:  false,
			},
			"expires_in": {
				Type:     schema.TypeInt,
				Optional: true,
				ForceNew: true,
				Default:  nil,
			},
			"component_access": {
				Type:     schema.TypeList,
				Optional: true,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
			},
			"bucket_permissions": {
				Type:         schema.TypeMap,
				Optional:     true,
				ValidateFunc: validateAccessTokenBucketPermissions,
			},
		},
	}
}
