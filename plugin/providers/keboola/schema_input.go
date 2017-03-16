package keboola

import "github.com/hashicorp/terraform/helper/schema"

var inputSchema = schema.Schema{
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
			"datatypes": &schema.Schema{
				Type:     schema.TypeMap,
				Optional: true,
			},
			"whereColumn": &schema.Schema{
				Type:     schema.TypeString,
				Optional: true,
				Default:  "",
			},
			"whereValues": &schema.Schema{
				Type:     schema.TypeList,
				Optional: true,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
			},
			"whereOperator": &schema.Schema{
				Type:     schema.TypeString,
				Optional: true,
				Default:  "eq",
			},
			"columns": &schema.Schema{
				Type:     schema.TypeList,
				Optional: true,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
			},
			"indexes": &schema.Schema{
				Type:     schema.TypeList,
				Optional: true,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
			},
			"days": &schema.Schema{
				Type:     schema.TypeInt,
				Optional: true,
			},
		},
	},
}
