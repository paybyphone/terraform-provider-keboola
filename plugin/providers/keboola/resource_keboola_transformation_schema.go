package keboola

import "github.com/hashicorp/terraform/helper/schema"

func resourceKeboolaTransformation() *schema.Resource {
	return &schema.Resource{
		Create: resourceKeboolaTransformCreate,
		Read:   resourceKeboolaTransformRead,
		Update: resourceKeboolaTransformUpdate,
		Delete: resourceKeboolaTransformDelete,

		Schema: map[string]*schema.Schema{
			"bucket_id": {
				Type:     schema.TypeString,
				Required: true,
			},
			"name": {
				Type:     schema.TypeString,
				Required: true,
			},
			"description": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"backend": {
				Type:     schema.TypeString,
				Required: true,
			},
			"phase": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"type": {
				Type:     schema.TypeString,
				Required: true,
			},
			"disabled": {
				Type:     schema.TypeBool,
				Optional: true,
			},
			"queries": {
				Type:     schema.TypeList,
				Optional: true,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
			},
			"output": &outputSchema,
			"input":  &inputSchema,
		},
	}
}
