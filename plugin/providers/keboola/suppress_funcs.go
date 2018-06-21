package keboola

import (
	"strings"
	"unicode"

	"github.com/hashicorp/terraform/helper/schema"
)

func stripWhitespace(str string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsSpace(r) {
			return -1
		}

		return r
	}, str)
}

//noinspection GoUnusedParameter
func suppressEquivalentJSON(k, old, new string, d *schema.ResourceData) bool {
	return stripWhitespace(old) == stripWhitespace(new)
}
