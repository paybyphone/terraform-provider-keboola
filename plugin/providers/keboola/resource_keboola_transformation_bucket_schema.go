package keboola

import "github.com/hashicorp/terraform/helper/schema"

func resourceKeboolaTransformationBucket() *schema.Resource {
	return &schema.Resource{
		Create: resourceKeboolaTransformBucketCreate,
		Read:   resourceKeboolaTransformBucketRead,
		Update: resourceKeboolaTransformBucketUpdate,
		Delete: resourceKeboolaTransformBucketDelete,
		Importer: &schema.ResourceImporter{
			State: schema.ImportStatePassthrough,
		},

		Schema: map[string]*schema.Schema{
			"name": {
				Type:     schema.TypeString,
				Required: true,
			},
			"description": {
				Type:     schema.TypeString,
				Optional: true,
			},
		},
	}
}
