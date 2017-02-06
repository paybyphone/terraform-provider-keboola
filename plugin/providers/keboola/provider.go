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
			"keboola_transformation":        resourceKeboolaTransformation(),
			"keboola_transformation_bucket": resourceKeboolaTransformationBucket(),
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
