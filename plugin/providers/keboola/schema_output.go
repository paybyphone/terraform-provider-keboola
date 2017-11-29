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
			"delete_where_column": &schema.Schema{
				Type:     schema.TypeString,
				Optional: true,
			},
			"delete_where_values": &schema.Schema{
				Type:     schema.TypeList,
				Optional: true,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
			},
			"delete_where_operator": &schema.Schema{
				Type:     schema.TypeString,
				Optional: true,
			},
			"primary_key": &schema.Schema{
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

func mapOutputSchemaToModel(outputs []interface{}) []Output {
	mappedOutputs := make([]Output, 0, len(outputs))

	for _, outputConfig := range outputs {
		config := outputConfig.(map[string]interface{})

		mappedOutput := Output{
			Source:              config["source"].(string),
			Destination:         config["destination"].(string),
			Incremental:         config["incremental"].(bool),
			DeleteWhereOperator: config["delete_where_operator"].(string),
			DeleteWhereColumn:   config["delete_where_column"].(string),
		}

		if q := config["primary_key"]; q != nil {
			mappedOutput.PrimaryKey = AsStringArray(q.([]interface{}))
		}

		if q := config["delete_where_values"]; q != nil {
			mappedOutput.DeleteWhereValues = AsStringArray(q.([]interface{}))
		}

		mappedOutputs = append(mappedOutputs, mappedOutput)
	}

	return mappedOutputs
}

func mapOutputModelToSchema(outputs []Output) []map[string]interface{} {
	var mappedOutputs []map[string]interface{}

	for _, output := range outputs {
		mappedOutput := map[string]interface{}{
			"source":                output.Source,
			"destination":           output.Destination,
			"incremental":           output.Incremental,
			"primary_key":           output.PrimaryKey,
			"delete_where_operator": output.DeleteWhereOperator,
			"delete_where_values":   output.DeleteWhereValues,
			"delete_where_column":   output.DeleteWhereColumn,
		}

		mappedOutputs = append(mappedOutputs, mappedOutput)
	}
	return mappedOutputs
}
