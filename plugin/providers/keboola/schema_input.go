package keboola

import (
	"strings"

	"github.com/hashicorp/terraform/helper/schema"
)

var inputSchema = schema.Schema{
	Type:     schema.TypeList,
	Optional: true,
	Elem: &schema.Resource{
		Schema: map[string]*schema.Schema{
			"source": {
				Type:     schema.TypeString,
				Required: true,
			},
			"destination": {
				Type:     schema.TypeString,
				Required: true,
			},
			"datatypes": {
				Type:     schema.TypeMap,
				Optional: true,
			},
			"where_column": {
				Type:     schema.TypeString,
				Optional: true,
				Default:  "",
			},
			"where_values": {
				Type:     schema.TypeList,
				Optional: true,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
			},
			"where_operator": {
				Type:     schema.TypeString,
				Optional: true,
				Default:  "eq",
			},
			"columns": {
				Type:     schema.TypeList,
				Optional: true,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
			},
			"indexes": {
				Type:     schema.TypeList,
				Optional: true,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
			},
			"days": {
				Type:     schema.TypeInt,
				Optional: true,
			},
		},
	},
}

func mapInputSchemaToModel(inputs []interface{}) []Input {
	mappedInputs := make([]Input, 0, len(inputs))

	for _, inputConfig := range inputs {
		config := inputConfig.(map[string]interface{})

		mappedInput := Input{
			Source:        config["source"].(string),
			Destination:   config["destination"].(string),
			WhereOperator: config["where_operator"].(string),
			WhereColumn:   config["where_column"].(string),
			DataTypes:     config["datatypes"].(map[string]interface{}),
			Days:          config["days"].(int),
		}

		if q := config["where_values"]; q != nil {
			mappedInput.WhereValues = AsStringArray(q.([]interface{}))
		}

		if q := config["columns"]; q != nil {
			mappedInput.Columns = AsStringArray(q.([]interface{}))
		}

		if q := config["indexes"]; q != nil {
			dest := make([][]string, 0, len(q.([]interface{})))
			for _, q := range q.([]interface{}) {
				if q != nil {
					indexes := strings.Split(q.(string), ",")
					dest = append(dest, indexes)
				}
			}
			mappedInput.Indexes = dest
		}

		mappedInputs = append(mappedInputs, mappedInput)
	}

	return mappedInputs
}

func mapInputModelToSchema(inputs []Input) []map[string]interface{} {
	var mappedInputs []map[string]interface{}

	for _, input := range inputs {
		mappedInput := map[string]interface{}{
			"source":         input.Source,
			"destination":    input.Destination,
			"columns":        input.Columns,
			"where_operator": input.WhereOperator,
			"where_values":   input.WhereValues,
			"where_column":   input.WhereColumn,
			"datatypes":      input.DataTypes,
			"days":           input.Days,
		}

		if input.Indexes != nil {
			mappedIndexes := make([]string, 0, len(input.Indexes))

			for _, i := range input.Indexes {
				combinedIndex := strings.Join(i, ",")
				mappedIndexes = append(mappedIndexes, combinedIndex)
			}

			mappedInput["indexes"] = mappedIndexes
		}

		mappedInputs = append(mappedInputs, mappedInput)
	}

	return mappedInputs
}
