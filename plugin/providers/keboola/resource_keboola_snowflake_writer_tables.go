package keboola

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/url"

	"github.com/hashicorp/terraform/helper/schema"
)

func resourceKeboolaSnowflakeWriterTables() *schema.Resource {
	return &schema.Resource{
		Create: resourceKeboolaSnowflakeWriterTablesCreate,
		Read:   resourceKeboolaSnowflakeWriterTablesRead,
		Update: resourceKeboolaSnowflakeWriterTablesUpdate,
		Delete: resourceKeboolaSnowflakeWriterTablesDelete,

		Schema: map[string]*schema.Schema{
			"writer_id": &schema.Schema{
				Type:     schema.TypeString,
				Required: true,
			},
			"table": &schema.Schema{
				Type:     schema.TypeList,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"dbName": &schema.Schema{
							Type:     schema.TypeString,
							Required: true,
						},
						"export": &schema.Schema{
							Type:     schema.TypeBool,
							Optional: true,
							Default:  false,
						},
						"tableId": &schema.Schema{
							Type:     schema.TypeString,
							Optional: true,
						},
						"incremental": &schema.Schema{
							Type:     schema.TypeBool,
							Optional: true,
							Default:  false,
						},
						"column": &schema.Schema{
							Type:     schema.TypeList,
							Optional: true,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"name": &schema.Schema{
										Type:     schema.TypeString,
										Optional: true,
									},
									"dbName": &schema.Schema{
										Type:     schema.TypeString,
										Optional: true,
									},
									"type": &schema.Schema{
										Type:     schema.TypeString,
										Optional: true,
									},
									"size": &schema.Schema{
										Type:     schema.TypeString,
										Required: true,
									},
									"nullable": &schema.Schema{
										Type:     schema.TypeBool,
										Optional: true,
										Default:  false,
									},
									"default": &schema.Schema{
										Type:     schema.TypeString,
										Optional: true,
										Default:  "",
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

func resourceKeboolaSnowflakeWriterTablesCreate(d *schema.ResourceData, meta interface{}) error {
	log.Print("[INFO] Creating Orchestration Tasks in Keboola.")

	writerID := d.Get("writer_id").(string)

	tables := d.Get("table").([]interface{})
	mappedTables := make([]SnowflakeWriterTable, 0, len(tables))

	for _, table := range tables {
		config := table.(map[string]interface{})

		mappedTable := SnowflakeWriterTable{
			DatabaseName: config["dbName"].(string),
			Export:       config["export"].(bool),
			TableID:      config["tableId"].(string),
			Incremental:  config["incremental"].(bool),
		}

		columnConfigs := config["column"].([]interface{})
		mappedColumns := make([]SnowflakeWriterTableItem, 0, len(columnConfigs))
		for _, column := range columnConfigs {
			columnConfig := column.(map[string]interface{})

			mappedColumn := SnowflakeWriterTableItem{
				Name:         columnConfig["name"].(string),
				DatabaseName: columnConfig["dbName"].(string),
				Type:         columnConfig["type"].(string),
				Size:         columnConfig["size"].(string),
				IsNullable:   columnConfig["nullable"].(bool),
				DefaultValue: columnConfig["default"].(string),
			}

			mappedColumns = append(mappedColumns, mappedColumn)
		}

		mappedTable.Items = mappedColumns
		mappedTables = append(mappedTables, mappedTable)
	}

	client := meta.(*KbcClient)

	getResp, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.wr-db-snowflake/configs/%s", writerID))

	if hasErrors(err, getResp) {
		return extractError(err, getResp)
	}

	var snowflakeWriter SnowflakeWriter

	decoder := json.NewDecoder(getResp.Body)
	err = decoder.Decode(&snowflakeWriter)

	if err != nil {
		return err
	}

	snowflakeWriter.Configuration.Parameters.Tables = mappedTables

	snowflakeConfigJSON, err := json.Marshal(snowflakeWriter.Configuration)

	if err != nil {
		return err
	}

	updateSnowflakeForm := url.Values{}
	updateSnowflakeForm.Add("configuration", string(snowflakeConfigJSON))
	updateSnowflakeForm.Add("changeDescription", "Update Snowflake tables")

	snowflakeConfigBuffer := bytes.NewBufferString(updateSnowflakeForm.Encode())

	putResp, err := client.PutFormToSyrup(fmt.Sprintf("docker/keboola.wr-db-snowflake/configs/%s", writerID), snowflakeConfigBuffer)

	if hasErrors(err, putResp) {
		return extractError(err, putResp)
	}

	d.SetId(writerID)

	return resourceKeboolaSnowflakeWriterTablesRead(d, meta)
}

func resourceKeboolaSnowflakeWriterTablesRead(d *schema.ResourceData, meta interface{}) error {
	log.Print("[INFO] Reading Snowflake Writer Tables from Keboola.")

	if d.Id() == "" {
		return nil
	}

	client := meta.(*KbcClient)

	getResp, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.wr-db-snowflake/configs/%s", d.Id()))

	if hasErrors(err, getResp) {
		return extractError(err, getResp)
	}

	var snowflakeWriter SnowflakeWriter

	decoder := json.NewDecoder(getResp.Body)
	err = decoder.Decode(&snowflakeWriter)

	if err != nil {
		return err
	}

	var tables []map[string]interface{}

	for _, tableConfig := range snowflakeWriter.Configuration.Parameters.Tables {
		tableDetails := map[string]interface{}{
			"dbName":  tableConfig.DatabaseName,
			"export":  tableConfig.Export,
			"tableId": tableConfig.TableID,
		}

		var columns []map[string]interface{}

		for _, item := range tableConfig.Items {
			columnDetails := map[string]interface{}{
				"name":     item.Name,
				"dbName":   item.DatabaseName,
				"type":     item.Type,
				"size":     item.Size,
				"nullable": item.IsNullable,
				"default":  item.DefaultValue,
			}

			columns = append(columns, columnDetails)
		}

		tableDetails["column"] = columns

		tables = append(tables, tableDetails)
	}

	d.Set("table", tables)

	return nil
}

func resourceKeboolaSnowflakeWriterTablesUpdate(d *schema.ResourceData, meta interface{}) error {
	log.Print("[INFO] Updating Orchestration Tasks in Keboola.")

	tables := d.Get("table").([]interface{})
	mappedTables := make([]SnowflakeWriterTable, 0, len(tables))

	for _, table := range tables {
		config := table.(map[string]interface{})

		mappedTable := SnowflakeWriterTable{
			DatabaseName: config["dbName"].(string),
			Export:       config["export"].(bool),
			TableID:      config["tableId"].(string),
			Incremental:  config["incremental"].(bool),
		}

		columnConfigs := config["column"].([]interface{})
		mappedColumns := make([]SnowflakeWriterTableItem, 0, len(columnConfigs))
		for _, column := range columnConfigs {
			columnConfig := column.(map[string]interface{})

			mappedColumn := SnowflakeWriterTableItem{
				Name:         columnConfig["name"].(string),
				DatabaseName: columnConfig["dbName"].(string),
				Type:         columnConfig["type"].(string),
				Size:         columnConfig["size"].(string),
				IsNullable:   columnConfig["nullable"].(bool),
				DefaultValue: columnConfig["default"].(string),
			}

			mappedColumns = append(mappedColumns, mappedColumn)
		}

		mappedTable.Items = mappedColumns
		mappedTables = append(mappedTables, mappedTable)
	}

	client := meta.(*KbcClient)

	getResp, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.wr-db-snowflake/configs/%s", d.Id()))

	if hasErrors(err, getResp) {
		return extractError(err, getResp)
	}

	var snowflakeWriter SnowflakeWriter

	decoder := json.NewDecoder(getResp.Body)
	err = decoder.Decode(&snowflakeWriter)

	if err != nil {
		return err
	}

	snowflakeWriter.Configuration.Parameters.Tables = mappedTables

	snowflakeConfigJSON, err := json.Marshal(snowflakeWriter.Configuration)

	if err != nil {
		return err
	}

	updateSnowflakeForm := url.Values{}
	updateSnowflakeForm.Add("configuration", string(snowflakeConfigJSON))
	updateSnowflakeForm.Add("changeDescription", "Update Snowflake tables")

	snowflakeConfigBuffer := bytes.NewBufferString(updateSnowflakeForm.Encode())

	putResp, err := client.PutFormToSyrup(fmt.Sprintf("docker/keboola.wr-db-snowflake/configs/%s", d.Id()), snowflakeConfigBuffer)

	if hasErrors(err, putResp) {
		return extractError(err, putResp)
	}

	return resourceKeboolaSnowflakeWriterTablesRead(d, meta)
}

func resourceKeboolaSnowflakeWriterTablesDelete(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[INFO] Clearing Orchestration Tasks in Keboola: %s", d.Id())

	client := meta.(*KbcClient)

	getResp, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.wr-db-snowflake/configs/%s", d.Id()))

	if hasErrors(err, getResp) {
		return extractError(err, getResp)
	}

	var snowflakeWriter SnowflakeWriter

	decoder := json.NewDecoder(getResp.Body)
	err = decoder.Decode(&snowflakeWriter)

	if err != nil {
		return err
	}

	var emptyTables []SnowflakeWriterTable
	snowflakeWriter.Configuration.Parameters.Tables = emptyTables

	snowflakeConfigJSON, err := json.Marshal(snowflakeWriter.Configuration)

	if err != nil {
		return err
	}

	updateSnowflakeForm := url.Values{}
	updateSnowflakeForm.Add("configuration", string(snowflakeConfigJSON))
	updateSnowflakeForm.Add("changeDescription", "Update Snowflake tables")

	snowflakeConfigBuffer := bytes.NewBufferString(updateSnowflakeForm.Encode())

	putResp, err := client.PutFormToSyrup(fmt.Sprintf("docker/keboola.wr-db-snowflake/configs/%s", d.Id()), snowflakeConfigBuffer)

	if hasErrors(err, putResp) {
		return extractError(err, putResp)
	}

	d.SetId("")

	return nil
}
