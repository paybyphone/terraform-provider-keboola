package keboola

import "github.com/hashicorp/terraform/helper/schema"

func resourceKeboolaStorageBucket() *schema.Resource {
	return &schema.Resource{
		Create: resourceKeboolaStorageBucketCreate,
		Read:   resourceKeboolaStorageBucketRead,
		Delete: resourceKeboolaStorageBucketDelete,
		Importer: &schema.ResourceImporter{
			State: schema.ImportStatePassthrough,
		},

		Schema: map[string]*schema.Schema{
			"name": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			"stage": {
				Type:         schema.TypeString,
				Required:     true,
				ForceNew:     true,
				ValidateFunc: validateStorageBucketStage,
			},
			"description": {
				Type:     schema.TypeString,
				Optional: true,
				ForceNew: true,
			},
			"backend": {
				Type:         schema.TypeString,
				Optional:     true,
				ForceNew:     true,
				ValidateFunc: validateStorageBucketBackend,
			},
		},
	}
}
