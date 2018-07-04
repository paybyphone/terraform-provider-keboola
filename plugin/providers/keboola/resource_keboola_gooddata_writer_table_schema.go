package keboola

import "github.com/hashicorp/terraform/helper/schema"

func resourceKeboolaGoodDataTable() *schema.Resource {
	return &schema.Resource{
		Create: resourceKeboolaGoodDataTableCreate,
		Read:   resourceKeboolaGoodDataTableRead,
		Update: resourceKeboolaGoodDataTableUpdate,
		Delete: resourceKeboolaGoodDataTableDelete,

		Schema: map[string]*schema.Schema{
			"writer_id": {
				Type:     schema.TypeString,
				Required: true,
			},
			"identifier": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"title": {
				Type:     schema.TypeString,
				Required: true,
			},
			"export": {
				Type:     schema.TypeBool,
				Required: true,
			},
			"incremental_days": {
				Type:     schema.TypeInt,
				Optional: true,
			},
			"column": {
				Type:     schema.TypeSet,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"data_type": {
							Type:     schema.TypeString,
							Optional: true,
						},
						"data_type_size": {
							Type:     schema.TypeString,
							Optional: true,
						},
						"date_dimension": {
							Type:     schema.TypeString,
							Optional: true,
						},
						"reference": {
							Type:     schema.TypeString,
							Optional: true,
						},
						"schema_reference": {
							Type:     schema.TypeString,
							Optional: true,
						},
						"format": {
							Type:     schema.TypeString,
							Optional: true,
						},
						"name": {
							Type:     schema.TypeString,
							Required: true,
						},
						"title": {
							Type:     schema.TypeString,
							Optional: true,
						},
						"type": {
							Type:     schema.TypeString,
							Required: true,
						},
					},
				},
			},
		},
	}
}
