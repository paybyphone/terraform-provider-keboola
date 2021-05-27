package keboola

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/url"

	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
	"github.com/paybyphone/terraform-provider-keboola/plugin/providers/keboola/buffer"
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
							Type:     schema.TypeInt,
							Computed: true,
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
	}
}

func generateExtractorTableID(checkList map[int]bool) int {

	const MaxID = 99999

	id := rand.Intn(MaxID)

	for checkList[id] {
		id = rand.Intn(MaxID)
	}

	return id
}

func resourceKeboolaSnowflakeExtractorTablesCreate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Creating Snowflake Extractor Tables in Keboola.")

	extractorID := d.Get("extractor_id").(string)
	tables := d.Get("table").(*schema.Set).List()

	extractorTables := make([]SnowflakeExtractorTable, 0, len(tables))

	distinctNames := make(map[string]bool)
	distinctIds := make(map[int]bool)

	for _, table := range tables {
		config := table.(map[string]interface{})

		extractorTable := SnowflakeExtractorTable{
			Name:        config["name"].(string),
			OutputTable: config["output_table"].(string),
			Incremental: config["incremental"].(bool),
			Enabled:     true,
		}

		if distinctNames[extractorTable.Name] {
			return fmt.Errorf("table with name already exists: %s", extractorTable.Name)
		}

		distinctNames[extractorTable.Name] = true

		extractorTable.ID = generateExtractorTableID(distinctIds)

		distinctIds[extractorTable.ID] = true

		if pk := config["primary_key"]; pk != nil {
			extractorTable.PrimaryKey = AsStringArray(pk.([]interface{}))
		}

		if query := config["query"].(string); query != "" {
			extractorTable.Query = query
		} else {
			extractorTable.InputTable = &SnowflakeExtractorInputTable{
				Schema:    config["schema"].(string),
				TableName: config["table_name"].(string),
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
			"id":           extractorTable.ID,
			"name":         extractorTable.Name,
			"enabled":      extractorTable.Enabled,
			"incremental":  extractorTable.Incremental,
			"output_table": extractorTable.OutputTable,
			"primary_key":  extractorTable.PrimaryKey,
		}

		if extractorTable.Query != "" {
			tableDetails["query"] = extractorTable.Query
		} else {
			tableDetails["schema"] = extractorTable.InputTable.Schema
			tableDetails["table_name"] = extractorTable.InputTable.TableName
			tableDetails["columns"] = extractorTable.Columns
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

	distinctNames := make(map[string]bool)
	distinctIds := make(map[int]bool)

	for _, table := range tables {
		config := table.(map[string]interface{})

		extractorTable := SnowflakeExtractorTable{
			Name:        config["name"].(string),
			OutputTable: config["output_table"].(string),
			Incremental: config["incremental"].(bool),
			Enabled:     true,
		}

		if distinctNames[extractorTable.Name] {
			return fmt.Errorf("table with name already exists: %s", extractorTable.Name)
		}

		distinctNames[extractorTable.Name] = true

		extractorTable.ID = generateExtractorTableID(distinctIds)

		distinctIds[extractorTable.ID] = true

		if pk := config["primary_key"]; pk != nil {
			extractorTable.PrimaryKey = AsStringArray(pk.([]interface{}))
		}

		if query := config["query"].(string); query != "" {
			extractorTable.Query = query
		} else {
			extractorTable.InputTable = &SnowflakeExtractorInputTable{
				Schema:    config["schema"].(string),
				TableName: config["table_name"].(string),
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

func getSnowflakeExtractorFromId(id string, client *KBCClient) (SnowflakeExtractorFromResponse *SnowflakeExtractor, err error) {

	getExtractorResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.ex-db-snowflake/configs/%s", id))

	if hasErrors(err, getExtractorResponse) {
		return nil, extractError(err, getExtractorResponse)
	}

	decoder := json.NewDecoder(getExtractorResponse.Body)
	err = decoder.Decode(&SnowflakeExtractorFromResponse)

	if err != nil {
		return nil, err
	}

	return SnowflakeExtractorFromResponse, nil
}
