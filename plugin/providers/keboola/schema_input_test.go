package keboola

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMappingFromInputToSchema(t *testing.T) {
	inputs := make([]interface{}, 0, 1)

	testInput := map[string]interface{}{
		"source":         "test source",
		"destination":    "test destination",
		"where_operator": "test operator",
		"where_column":   "test column",
		"datatypes": map[string]interface{}{
			"foo": "bar",
		},
		"days": 2,
		"changed_since": "-2 days",
	}

	inputs = append(inputs, testInput)
	result := mapInputSchemaToModel(inputs)

	assert.Equal(t, testInput["source"].(string), result[0].Source, "Original source and mapped source should match")
	assert.Equal(t, testInput["destination"].(string), result[0].Destination, "Original destination and mapped destination should match")
	assert.Equal(t, testInput["where_operator"].(string), result[0].WhereOperator, "Original whereOperator and mapped whereOperator should match")
	assert.Equal(t, testInput["where_column"].(string), result[0].WhereColumn, "Original whereColumn and mapped whereColumn should match")
	assert.Equal(t, testInput["datatypes"].(map[string]interface{}), result[0].DataTypes, "Original datatypes and mapped datatypes should match")
	assert.Equal(t, testInput["days"].(int), result[0].Days, "Original days and mapped days should match")
	assert.Equal(t, testInput["changed_since"].(string), result[0].ChangedSince, "Original changedSince and mapped changedSince should match")
}
