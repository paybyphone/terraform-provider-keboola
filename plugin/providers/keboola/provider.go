package keboola

import (
	"log"

	"github.com/hashicorp/terraform/helper/schema"
	"github.com/hashicorp/terraform/terraform"
)

// Provider returns a terraform.ResourceProvider for the Keboola provider.
func Provider() terraform.ResourceProvider {
	return &schema.Provider{
		Schema: map[string]*schema.Schema{
			"api_key": {
				Type:        schema.TypeString,
				Required:    true,
				DefaultFunc: schema.EnvDefaultFunc("STORAGE_API_KEY", nil),
			},
		},

		ResourcesMap: map[string]*schema.Resource{
			"keboola_storage_table":            resourceKeboolaStorageTable(),
			"keboola_storage_bucket":           resourceKeboolaStorageBucket(),
			"keboola_transformation":           resourceKeboolaTransformation(),
			"keboola_transformation_bucket":    resourceKeboolaTransformationBucket(),
			"keboola_gooddata_user_management": resourceKeboolaGoodDataUserManagement(),
		},

		ConfigureFunc: providerConfigure,
	}
}

func providerConfigure(d *schema.ResourceData) (interface{}, error) {
	client := &KbcClient{
		APIKey: d.Get("api_key").(string),
	}

	log.Println("[INFO] Initializing Keboola REST client")
	return client, nil
}
