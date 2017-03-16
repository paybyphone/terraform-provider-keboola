package keboola

import "github.com/hashicorp/terraform/helper/schema"

var outputSchema = schema.Schema{
	Type:     schema.TypeList,
	Optional: true,
	Elem: &schema.Resource{
		Schema: map[string]*schema.Schema{
			"source": &schema.Schema{
				Type:     schema.TypeString,
				Required: true,
			},
			"destination": &schema.Schema{
				Type:     schema.TypeString,
				Required: true,
			},
			"deleteWhereColumn": &schema.Schema{
				Type:     schema.TypeString,
				Optional: true,
			},
			"deleteWhereValues": &schema.Schema{
				Type:     schema.TypeList,
				Optional: true,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
			},
			"deleteWhereOperator": &schema.Schema{
				Type:     schema.TypeString,
				Optional: true,
			},
			"primaryKey": &schema.Schema{
				Type:     schema.TypeList,
				Optional: true,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
			},
			"incremental": &schema.Schema{
				Type:     schema.TypeBool,
				Optional: true,
			},
		},
	},
}
