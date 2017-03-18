package keboola

//AsStringArray converts an array of interfaces to an array of strings
//Terraform stores array data within the ResourceData as []interface{}
func AsStringArray(source []interface{}) []string {
	destination := make([]string, 0, len(source))
	for _, q := range source {
		if q != nil {
			destination = append(destination, q.(string))
		}
	}

	return destination
}
