package keboola

import (
	"log"
	"strings"

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
			"keboola_storage_table":              resourceKeboolaStorageTable(),
			"keboola_storage_bucket":             resourceKeboolaStorageBucket(),
			"keboola_transformation":             resourceKeboolaTransformation(),
			"keboola_transformation_bucket":      resourceKeboolaTransformationBucket(),
			"keboola_gooddata_writer":            resourceKeboolaGoodDataWriter(),
			"keboola_gooddata_writer_table":      resourceKeboolaGoodDataTable(),
			"keboola_gooddata_user_management":   resourceKeboolaGoodDataUserManagement(),
			"keboola_snowflake_writer":           resourceKeboolaSnowflakeWriter(),
			"keboola_snowflake_writer_tables":    resourceKeboolaSnowflakeWriterTables(),
			"keboola_postgresql_writer":          resourceKeboolaPostgreSQLWriter(),
			"keboola_postgresql_writer_tables":   resourceKeboolaPostgreSQLWriterTables(),
			"keboola_access_token":               resourceKeboolaAccessToken(),
			"keboola_orchestration":              resourceKeboolaOrchestration(),
			"keboola_orchestration_tasks":        resourceKeboolaOrchestrationTasks(),
			"keboola_csvimport_extractor":        resourceKeboolaCSVImportExtractor(),
			"keboola_snowflake_extractor":        resourceKeboolaSnowflakeExtractor(),
			"keboola_snowflake_extractor_tables": resourceKeboolaSnowflakeExtractorTables(),
		},

		ConfigureFunc: providerConfigure,
	}
}

func providerConfigure(d *schema.ResourceData) (interface{}, error) {
	log.Println("[INFO] Initializing Keboola REST client")
	client := &KBCClient{
		APIKey: strings.TrimSpace(d.Get("api_key").(string)),
	}
	return client, nil
}
