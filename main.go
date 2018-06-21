package main

import (
	"github.com/hashicorp/terraform/plugin"
	"github.com/plmwong/terraform-provider-keboola/plugin/providers/keboola"
)

func main() {
	plugin.Serve(&plugin.ServeOpts{
		ProviderFunc: keboola.Provider,
	})
}
