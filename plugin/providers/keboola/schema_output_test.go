package keboola

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMappingFromOutputToSchema(t *testing.T) {
	outputs := make([]interface{}, 0, 1)

	testOutput := map[string]interface{}{
		"source":                "test source",
		"destination":           "test destination",
		"incremental":           true,
		"delete_where_operator": "test operator",
		"delete_where_column":   "test column",
	}

	outputs = append(outputs, testOutput)
	result := mapOutputSchemaToModel(outputs)

	assert.Equal(t, testOutput["source"].(string), result[0].Source, "Original source and mapped source should match")
	assert.Equal(t, testOutput["destination"].(string), result[0].Destination, "Original destination and mapped destination should match")
	assert.Equal(t, testOutput["incremental"].(bool), result[0].Incremental, "Original incremental and mapped incremental should match")
	assert.Equal(t, testOutput["delete_where_operator"].(string), result[0].DeleteWhereOperator, "Original deleteWhereOperator and mapped deleteWhereOperator should match")
	assert.Equal(t, testOutput["delete_where_column"].(string), result[0].DeleteWhereColumn, "Original deleteWhereColumn and mapped deleteWhereColumn should match")
}
