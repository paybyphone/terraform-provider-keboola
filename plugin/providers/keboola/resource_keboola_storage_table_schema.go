package keboola

import "github.com/hashicorp/terraform/helper/schema"

func resourceKeboolaStorageTable() *schema.Resource {
	return &schema.Resource{
		Create: resourceKeboolaStorageTableCreate,
		Read:   resourceKeboolaStorageTableRead,
		Delete: resourceKeboolaStorageTableDelete,

		Schema: map[string]*schema.Schema{
			"bucket_id": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			"name": {
				Type:     schema.TypeString,
				Required: true,
				ForceNew: true,
			},
			"delimiter": {
				Type:     schema.TypeString,
				Optional: true,
				ForceNew: true,
			},
			"enclosure": {
				Type:     schema.TypeString,
				Optional: true,
				ForceNew: true,
			},
			"transactional": {
				Type:     schema.TypeBool,
				Optional: true,
				ForceNew: true,
			},
			"primary_key": {
				Type:     schema.TypeList,
				Optional: true,
				ForceNew: true,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
			},
			"columns": {
				Type:     schema.TypeSet,
				Optional: true,
				ForceNew: true,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
			},
			"indexed_columns": {
				Type:     schema.TypeList,
				Optional: true,
				ForceNew: true,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
			},
		},
	}
}
