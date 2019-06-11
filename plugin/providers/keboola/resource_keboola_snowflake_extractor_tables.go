package keboola

import (
	"encoding/json"
	"fmt"
	"log"
	"net/url"

	"github.com/hashicorp/terraform/helper/schema"
	"github.com/plmwong/terraform-provider-keboola/plugin/providers/keboola/buffer"
)

func resourceKeboolaSnowflakeExtractorTables() *schema.Resource {
	return &schema.Resource{
		Create: resourceKeboolaSnowflakeExtractorTablesCreate,
		Read:   resourceKeboolaSnowflakeExtractorTablesRead,
		Update: resourceKeboolaSnowflakeExtractorTablesUpdate,
		Delete: resourceKeboolaSnowflakeExtractorTablesDelete,

		Schema: map[string]*schema.Schema{
			"extractor_id": {
				Type:     schema.TypeString,
				Required: true,
			},
			"table": {
				Type:     schema.TypeSet,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"id": {
							Type:     schema.TypeString,
							Optional: true,
						},
						"name": {
							Type:     schema.TypeString,
							Required: true,
						},
						"enabled": {
							Type:     schema.TypeBool,
							Optional: true,
							Default:  true,
						},
						"incremental": {
							Type:     schema.TypeBool,
							Optional: true,
							Default:  false,
						},
						"output_table": {
							Type:     schema.TypeString,
							Required: true,
						},
						"primary_key": {
							Type:     schema.TypeList,
							Optional: true,
							Elem: &schema.Schema{
								Type: schema.TypeString,
							},
						},
						"query": {
							Type:     schema.TypeString,
							Optional: true,
						},
						"columns": {
							Type:     schema.TypeList,
							Optional: true,
							Elem: &schema.Schema{
								Type: schema.TypeString,
							},
						},
						"input_table": {
							Type:     schema.TypeList,
							Optional: true,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"schema": {
										Type:     schema.TypeString,
										Optional: true,
									},
									"table_name": {
										Type:     schema.TypeString,
										Optional: true,
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

func resourceKeboolaSnowflakeExtractorTablesCreate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Creating Snowflake Extractor Tables in Keboola.")

	extractorID := d.Get("extractor_id").(string)
	tables := d.Get("table").(*schema.Set).List()

	extractorTables := make([]SnowflakeExtractorTable, 0, len(tables))

	for _, table :=  range tables {
		config := table.(map[string]interface{})

		extractorTable := SnowflakeExtractorTable{
			Name: 			config["name"].(string),
			OutputTable:	config["output_table"].(string),
			Incremental:	config["incremental"].(bool),
		}

		if pk := config["primary_key"]; pk != nil {
			extractorTable.PrimaryKey = AsStringArray(pk.([]interface{}))
		}

		if query := config["query"]; query != nil {
			extractorTable.Query = query.(string)
		} else {
			inputTableConfig := config["input_table"].(map[string]interface{})
			extractorTable.InputTable = SnowflakeExtractorInputTable{
				Schema:		inputTableConfig["schema"].(string),
				TableName:	inputTableConfig["tableName"].(string),
			}

			if c := config["columns"]; c != nil {
				extractorTable.Columns = AsStringArray(c.([]interface{}))
			}
		}

		extractorTables = append(extractorTables, extractorTable)
	}

	client := meta.(*KBCClient)

	getExtractorResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.ex-db-snowflake/configs/%s", extractorID))

	if hasErrors(err, getExtractorResponse) {
		return extractError(err, getExtractorResponse)
	}

	var snowflakeExtractor SnowflakeExtractor

	decoder := json.NewDecoder(getExtractorResponse.Body)
	err = decoder.Decode(&snowflakeExtractor)

	if err != nil {
		return err
	}

	snowflakeExtractor.Configuration.Parameters.Tables = extractorTables

	snowflakeConfigJSON, err := json.Marshal(snowflakeExtractor.Configuration)

	if err != nil {
		return err
	}

	updateSnowflakeForm := url.Values{}
	updateSnowflakeForm.Add("configuration", string(snowflakeConfigJSON))
	updateSnowflakeForm.Add("changeDescription", "Update Snowflake tables")

	updateSnowflakeBuffer := buffer.FromForm(updateSnowflakeForm)

	updateResponse, err := client.PutToStorage(fmt.Sprintf("storage/components/keboola.ex-db-snowflake/configs/%s", extractorID), updateSnowflakeBuffer)

	if hasErrors(err, updateResponse) {
		return extractError(err, updateResponse)
	}

	d.SetId(extractorID)

	return resourceKeboolaSnowflakeExtractorTablesRead(d, meta)
}

func resourceKeboolaSnowflakeExtractorTablesRead(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Reading Snowflake Extractor Tables from Keboola.")

	if d.Id() == "" {
		return nil
	}

	client := meta.(*KBCClient)

	getSnowflakeExtractorResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.ex-db-snowflake/configs/%s", d.Id()))

	if hasErrors(err, getSnowflakeExtractorResponse) {
		if getSnowflakeExtractorResponse.StatusCode == 404 {
			d.SetId("")
			return nil
		}

		return extractError(err, getSnowflakeExtractorResponse)
	}

	var SnowflakeExtractor SnowflakeExtractor

	decoder := json.NewDecoder(getSnowflakeExtractorResponse.Body)
	err = decoder.Decode(&SnowflakeExtractor)

	if err != nil {
		return err
	}

	var tables []map[string]interface{}

	for _, extractorTable := range SnowflakeExtractor.Configuration.Parameters.Tables {

		tableDetails := map[string]interface{}{
			"id":				extractorTable.ID,
			"name":				extractorTable.Name,
			"enabled":			extractorTable.Enabled,
			"incremental":		extractorTable.Incremental,
			"output_table":		extractorTable.OutputTable,
			"primary_key":		extractorTable.PrimaryKey,
		}

		if extractorTable.Query != "" {
			tableDetails["query"] = extractorTable.Query
		} else {
			tableDetails["columns"] = extractorTable.Columns

			inputTableDetails := map[string]interface{}{
				"schema":		extractorTable.InputTable.Schema,
				"table_name":	extractorTable.InputTable.TableName,
			}

			tableDetails["input_table"] = inputTableDetails
		}

		tables = append(tables, tableDetails)
	}

	d.Set("table", tables)

	return nil
}

func resourceKeboolaSnowflakeExtractorTablesUpdate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Updating Snowflake Extractor Tables in Keboola.")

	tables := d.Get("table").(*schema.Set).List()

	extractorTables := make([]SnowflakeExtractorTable, 0, len(tables))

	for _, table := range tables {
		config := table.(map[string]interface{})

		extractorTable := SnowflakeExtractorTable{
			Name: 			config["name"].(string),
			OutputTable:	config["output_table"].(string),
			Incremental:	config["incremental"].(bool),
		}

		if pk := config["primary_key"]; pk != nil {
			extractorTable.PrimaryKey = AsStringArray(pk.([]interface{}))
		}

		if query := config["query"]; query != nil {
			extractorTable.Query = query.(string)
		} else {
			inputTableConfig := config["input_table"].(map[string]interface{})
			extractorTable.InputTable = SnowflakeExtractorInputTable{
				Schema:		inputTableConfig["schema"].(string),
				TableName:	inputTableConfig["tableName"].(string),
			}

			if c := config["columns"]; c != nil {
				extractorTable.Columns = AsStringArray(c.([]interface{}))
			}
		}

		extractorTables = append(extractorTables, extractorTable)
	}

	client := meta.(*KBCClient)

	getExtractorResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.ex-db-snowflake/configs/%s", d.Id()))

	if hasErrors(err, getExtractorResponse) {
		return extractError(err, getExtractorResponse)
	}

	var SnowflakeExtractor SnowflakeExtractor

	decoder := json.NewDecoder(getExtractorResponse.Body)
	err = decoder.Decode(&SnowflakeExtractor)

	if err != nil {
		return err
	}

	SnowflakeExtractor.Configuration.Parameters.Tables = extractorTables

	snowflakeConfigJSON, err := json.Marshal(SnowflakeExtractor.Configuration)

	if err != nil {
		return err
	}

	updateSnowflakeForm := url.Values{}
	updateSnowflakeForm.Add("configuration", string(snowflakeConfigJSON))
	updateSnowflakeForm.Add("changeDescription", "Update Snowflake tables")

	updateSnowflakeBuffer := buffer.FromForm(updateSnowflakeForm)

	updateResponse, err := client.PutToStorage(fmt.Sprintf("storage/components/keboola.ex-db-snowflake/configs/%s", d.Id()), updateSnowflakeBuffer)

	if hasErrors(err, updateResponse) {
		return extractError(err, updateResponse)
	}

	return resourceKeboolaSnowflakeExtractorTablesRead(d, meta)
}

func resourceKeboolaSnowflakeExtractorTablesDelete(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[INFO] Clearing Snowflake Extractor Tables in Keboola: %s", d.Id())

	client := meta.(*KBCClient)

	getExtractorResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.ex-db-snowflake/configs/%s", d.Id()))

	if hasErrors(err, getExtractorResponse) {
		return extractError(err, getExtractorResponse)
	}

	var SnowflakeExtractor SnowflakeExtractor

	decoder := json.NewDecoder(getExtractorResponse.Body)
	err = decoder.Decode(&SnowflakeExtractor)

	if err != nil {
		return err
	}

	var emptyTables []SnowflakeExtractorTable
	SnowflakeExtractor.Configuration.Parameters.Tables = emptyTables

	snowflakeConfigJSON, err := json.Marshal(SnowflakeExtractor.Configuration)

	if err != nil {
		return err
	}

	clearSnowflakeTablesForm := url.Values{}
	clearSnowflakeTablesForm.Add("configuration", string(snowflakeConfigJSON))
	clearSnowflakeTablesForm.Add("changeDescription", "Update Snowflake tables")

	clearSnowflakeTablesBuffer := buffer.FromForm(clearSnowflakeTablesForm)

	clearResponse, err := client.PutToStorage(fmt.Sprintf("storage/components/keboola.ex-db-snowflake/configs/%s", d.Id()), clearSnowflakeTablesBuffer)

	if hasErrors(err, clearResponse) {
		return extractError(err, clearResponse)
	}

	d.SetId("")

	return nil
}
