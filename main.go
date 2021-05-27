package main

import (
	"github.com/hashicorp/terraform-plugin-sdk/plugin"
	"github.com/paybyphone/terraform-provider-keboola/plugin/providers/keboola"
)

func main() {
	plugin.Serve(&plugin.ServeOpts{
		ProviderFunc: keboola.Provider,
	})
}
