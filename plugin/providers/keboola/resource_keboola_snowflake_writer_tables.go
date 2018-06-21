package keboola

import (
	"encoding/json"
	"fmt"
	"log"
	"net/url"

	"github.com/hashicorp/terraform/helper/schema"
	"github.com/plmwong/terraform-provider-keboola/plugin/providers/keboola/buffer"
)

func resourceKeboolaSnowflakeWriterTables() *schema.Resource {
	return &schema.Resource{
		Create: resourceKeboolaSnowflakeWriterTablesCreate,
		Read:   resourceKeboolaSnowflakeWriterTablesRead,
		Update: resourceKeboolaSnowflakeWriterTablesUpdate,
		Delete: resourceKeboolaSnowflakeWriterTablesDelete,

		Schema: map[string]*schema.Schema{
			"writer_id": {
				Type:     schema.TypeString,
				Required: true,
			},
			"table": {
				Type:     schema.TypeList,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"db_name": {
							Type:     schema.TypeString,
							Required: true,
						},
						"export": {
							Type:     schema.TypeBool,
							Optional: true,
							Default:  false,
						},
						"table_id": {
							Type:     schema.TypeString,
							Optional: true,
						},
						"incremental": {
							Type:     schema.TypeBool,
							Optional: true,
							Default:  false,
						},
						"primary_key": {
							Type:     schema.TypeList,
							Optional: true,
							Elem: &schema.Schema{
								Type: schema.TypeString,
							},
						},
						"column": {
							Type:     schema.TypeList,
							Optional: true,
							Elem: &schema.Resource{
								Schema: map[string]*schema.Schema{
									"name": {
										Type:     schema.TypeString,
										Optional: true,
									},
									"db_name": {
										Type:     schema.TypeString,
										Optional: true,
									},
									"type": {
										Type:     schema.TypeString,
										Optional: true,
									},
									"size": {
										Type:     schema.TypeString,
										Required: true,
									},
									"nullable": {
										Type:     schema.TypeBool,
										Optional: true,
										Default:  false,
									},
									"default": {
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
	log.Println("[INFO] Creating Snowflake Writer Tables in Keboola.")

	writerID := d.Get("writer_id").(string)
	tables := d.Get("table").([]interface{})

	mappedTables := make([]SnowflakeWriterTable, 0, len(tables))
	storageTables := make([]SnowflakeWriterStorageTable, 0, len(tables))

	for _, table := range tables {
		config := table.(map[string]interface{})

		mappedTable := SnowflakeWriterTable{
			DatabaseName: config["db_name"].(string),
			Export:       config["export"].(bool),
			TableID:      config["table_id"].(string),
			Incremental:  config["incremental"].(bool),
		}

		if q := config["primary_key"]; q != nil {
			mappedTable.PrimaryKey = AsStringArray(q.([]interface{}))
		}

		storageTable := SnowflakeWriterStorageTable{
			Source:      mappedTable.TableID,
			Destination: fmt.Sprintf("%s.csv", mappedTable.TableID),
		}

		columnConfigs := config["column"].([]interface{})
		mappedColumns := make([]SnowflakeWriterTableItem, 0, len(columnConfigs))
		columnNames := make([]string, 0, len(columnConfigs))
		for _, column := range columnConfigs {
			columnConfig := column.(map[string]interface{})

			mappedColumn := SnowflakeWriterTableItem{
				Name:         columnConfig["name"].(string),
				DatabaseName: columnConfig["db_name"].(string),
				Type:         columnConfig["type"].(string),
				Size:         columnConfig["size"].(string),
				IsNullable:   columnConfig["nullable"].(bool),
				DefaultValue: columnConfig["default"].(string),
			}

			mappedColumns = append(mappedColumns, mappedColumn)
			columnNames = append(columnNames, mappedColumn.Name)
		}

		mappedTable.Items = mappedColumns
		storageTable.Columns = columnNames

		mappedTables = append(mappedTables, mappedTable)
		storageTables = append(storageTables, storageTable)
	}

	client := meta.(*KBCClient)

	getWriterResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.wr-db-snowflake/configs/%s", writerID))

	if hasErrors(err, getWriterResponse) {
		return extractError(err, getWriterResponse)
	}

	var snowflakeWriter SnowflakeWriter

	decoder := json.NewDecoder(getWriterResponse.Body)
	err = decoder.Decode(&snowflakeWriter)

	if err != nil {
		return err
	}

	snowflakeWriter.Configuration.Parameters.Tables = mappedTables
	snowflakeWriter.Configuration.Storage.Input.Tables = storageTables

	snowflakeConfigJSON, err := json.Marshal(snowflakeWriter.Configuration)

	if err != nil {
		return err
	}

	updateSnowflakeForm := url.Values{}
	updateSnowflakeForm.Add("configuration", string(snowflakeConfigJSON))
	updateSnowflakeForm.Add("changeDescription", "Update Snowflake tables")

	updateSnowflakeBuffer := buffer.FromForm(updateSnowflakeForm)

	updateResponse, err := client.PutToStorage(fmt.Sprintf("storage/components/keboola.wr-db-snowflake/configs/%s", writerID), updateSnowflakeBuffer)

	if hasErrors(err, updateResponse) {
		return extractError(err, updateResponse)
	}

	d.SetId(writerID)

	return resourceKeboolaSnowflakeWriterTablesRead(d, meta)
}

func resourceKeboolaSnowflakeWriterTablesRead(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Reading Snowflake Writer Tables from Keboola.")

	if d.Id() == "" {
		return nil
	}

	client := meta.(*KBCClient)

	getSnowflakeWriterResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.wr-db-snowflake/configs/%s", d.Id()))

	if hasErrors(err, getSnowflakeWriterResponse) {
		if getSnowflakeWriterResponse.StatusCode == 404 {
			d.SetId("")
			return nil
		}

		return extractError(err, getSnowflakeWriterResponse)
	}

	var snowflakeWriter SnowflakeWriter

	decoder := json.NewDecoder(getSnowflakeWriterResponse.Body)
	err = decoder.Decode(&snowflakeWriter)

	if err != nil {
		return err
	}

	var tables []map[string]interface{}

	for _, tableConfig := range snowflakeWriter.Configuration.Parameters.Tables {
		tableDetails := map[string]interface{}{
			"db_name":     tableConfig.DatabaseName,
			"export":      tableConfig.Export,
			"table_id":    tableConfig.TableID,
			"incremental": tableConfig.Incremental,
			"primary_key": tableConfig.PrimaryKey,
		}

		var columns []map[string]interface{}

		for _, item := range tableConfig.Items {
			columnDetails := map[string]interface{}{
				"name":     item.Name,
				"db_name":  item.DatabaseName,
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
	log.Println("[INFO] Updating Snowflake Writer Tables in Keboola.")

	tables := d.Get("table").([]interface{})

	mappedTables := make([]SnowflakeWriterTable, 0, len(tables))
	storageTables := make([]SnowflakeWriterStorageTable, 0, len(tables))

	for _, table := range tables {
		config := table.(map[string]interface{})

		mappedTable := SnowflakeWriterTable{
			DatabaseName: config["db_name"].(string),
			Export:       config["export"].(bool),
			TableID:      config["table_id"].(string),
			Incremental:  config["incremental"].(bool),
		}

		if q := config["primary_key"]; q != nil {
			mappedTable.PrimaryKey = AsStringArray(q.([]interface{}))
		}

		storageTable := SnowflakeWriterStorageTable{
			Source:      mappedTable.TableID,
			Destination: fmt.Sprintf("%s.csv", mappedTable.TableID),
		}

		columnConfigs := config["column"].([]interface{})
		mappedColumns := make([]SnowflakeWriterTableItem, 0, len(columnConfigs))
		columnNames := make([]string, 0, len(columnConfigs))
		for _, column := range columnConfigs {
			columnConfig := column.(map[string]interface{})

			mappedColumn := SnowflakeWriterTableItem{
				Name:         columnConfig["name"].(string),
				DatabaseName: columnConfig["db_name"].(string),
				Type:         columnConfig["type"].(string),
				Size:         columnConfig["size"].(string),
				IsNullable:   columnConfig["nullable"].(bool),
				DefaultValue: columnConfig["default"].(string),
			}

			mappedColumns = append(mappedColumns, mappedColumn)
			columnNames = append(columnNames, mappedColumn.Name)
		}

		mappedTable.Items = mappedColumns
		storageTable.Columns = columnNames

		mappedTables = append(mappedTables, mappedTable)
		storageTables = append(storageTables, storageTable)
	}

	client := meta.(*KBCClient)

	getWriterResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.wr-db-snowflake/configs/%s", d.Id()))

	if hasErrors(err, getWriterResponse) {
		return extractError(err, getWriterResponse)
	}

	var snowflakeWriter SnowflakeWriter

	decoder := json.NewDecoder(getWriterResponse.Body)
	err = decoder.Decode(&snowflakeWriter)

	if err != nil {
		return err
	}

	snowflakeWriter.Configuration.Parameters.Tables = mappedTables
	snowflakeWriter.Configuration.Storage.Input.Tables = storageTables

	snowflakeConfigJSON, err := json.Marshal(snowflakeWriter.Configuration)

	if err != nil {
		return err
	}

	updateSnowflakeForm := url.Values{}
	updateSnowflakeForm.Add("configuration", string(snowflakeConfigJSON))
	updateSnowflakeForm.Add("changeDescription", "Update Snowflake tables")

	updateSnowflakeBuffer := buffer.FromForm(updateSnowflakeForm)

	updateResponse, err := client.PutToStorage(fmt.Sprintf("storage/components/keboola.wr-db-snowflake/configs/%s", d.Id()), updateSnowflakeBuffer)

	if hasErrors(err, updateResponse) {
		return extractError(err, updateResponse)
	}

	return resourceKeboolaSnowflakeWriterTablesRead(d, meta)
}

func resourceKeboolaSnowflakeWriterTablesDelete(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[INFO] Clearing Snowflake Writer Tables in Keboola: %s", d.Id())

	client := meta.(*KBCClient)

	getWriterResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.wr-db-snowflake/configs/%s", d.Id()))

	if hasErrors(err, getWriterResponse) {
		return extractError(err, getWriterResponse)
	}

	var snowflakeWriter SnowflakeWriter

	decoder := json.NewDecoder(getWriterResponse.Body)
	err = decoder.Decode(&snowflakeWriter)

	if err != nil {
		return err
	}

	var emptyTables []SnowflakeWriterTable
	snowflakeWriter.Configuration.Parameters.Tables = emptyTables

	var emptyStorageTables []SnowflakeWriterStorageTable
	snowflakeWriter.Configuration.Storage.Input.Tables = emptyStorageTables

	snowflakeConfigJSON, err := json.Marshal(snowflakeWriter.Configuration)

	if err != nil {
		return err
	}

	clearSnowflakeTablesForm := url.Values{}
	clearSnowflakeTablesForm.Add("configuration", string(snowflakeConfigJSON))
	clearSnowflakeTablesForm.Add("changeDescription", "Update Snowflake tables")

	clearSnowflakeTablesBuffer := buffer.FromForm(clearSnowflakeTablesForm)

	clearResponse, err := client.PutToStorage(fmt.Sprintf("storage/components/keboola.wr-db-snowflake/configs/%s", d.Id()), clearSnowflakeTablesBuffer)

	if hasErrors(err, clearResponse) {
		return extractError(err, clearResponse)
	}

	d.SetId("")

	return nil
}
