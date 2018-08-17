package keboola

import (
	"encoding/json"
	"fmt"
	"log"
	"net/url"

	"github.com/hashicorp/terraform/helper/schema"
	"github.com/plmwong/terraform-provider-keboola/plugin/providers/keboola/buffer"
)

func resourceKeboolaSnowflakeWriterTablesCreate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Creating Snowflake Writer Tables in Keboola.")

	writerID := d.Get("writer_id").(string)
	tables := d.Get("table").(*schema.Set).List()

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

	resp, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.wr-db-snowflake/configs/%s", writerID))

	if hasErrors(err, resp) {
		return extractError(err, resp)
	}

	var writer SnowflakeWriter

	decoder := json.NewDecoder(resp.Body)
	err = decoder.Decode(&writer)

	if err != nil {
		return err
	}

	writer.Configuration.Parameters.Tables = mappedTables
	writer.Configuration.Storage.Input.Tables = storageTables

	snowflakeConfigJSON, err := json.Marshal(writer.Configuration)

	if err != nil {
		return err
	}

	form := url.Values{}
	form.Add("configuration", string(snowflakeConfigJSON))
	form.Add("changeDescription", "Update Snowflake tables")

	formData := buffer.FromForm(form)

	resp, err = client.PutToStorage(fmt.Sprintf("storage/components/keboola.wr-db-snowflake/configs/%s", writerID), formData)

	if hasErrors(err, resp) {
		return extractError(err, resp)
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

	resp, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.wr-db-snowflake/configs/%s", d.Id()))

	if hasErrors(err, resp) {
		if err != nil {
			return err
		}

		if resp.StatusCode == 404 {
			d.SetId("")
			return nil
		}

		return extractError(err, resp)
	}

	var writer SnowflakeWriter

	decoder := json.NewDecoder(resp.Body)
	err = decoder.Decode(&writer)

	if err != nil {
		return err
	}

	var tables []map[string]interface{}

	for _, tableConfig := range writer.Configuration.Parameters.Tables {
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

	resp, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.wr-db-snowflake/configs/%s", d.Id()))

	if hasErrors(err, resp) {
		return extractError(err, resp)
	}

	var writer SnowflakeWriter

	decoder := json.NewDecoder(resp.Body)
	err = decoder.Decode(&writer)

	if err != nil {
		return err
	}

	writer.Configuration.Parameters.Tables = mappedTables
	writer.Configuration.Storage.Input.Tables = storageTables

	snowflakeConfigJSON, err := json.Marshal(writer.Configuration)

	if err != nil {
		return err
	}

	form := url.Values{}
	form.Add("configuration", string(snowflakeConfigJSON))
	form.Add("changeDescription", "Update Snowflake tables")

	formData := buffer.FromForm(form)

	resp, err = client.PutToStorage(fmt.Sprintf("storage/components/keboola.wr-db-snowflake/configs/%s", d.Id()), formData)

	if hasErrors(err, resp) {
		return extractError(err, resp)
	}

	return resourceKeboolaSnowflakeWriterTablesRead(d, meta)
}

func resourceKeboolaSnowflakeWriterTablesDelete(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[INFO] Clearing Snowflake Writer Tables in Keboola: %s", d.Id())

	client := meta.(*KBCClient)

	resp, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.wr-db-snowflake/configs/%s", d.Id()))

	if hasErrors(err, resp) {
		return extractError(err, resp)
	}

	var writer SnowflakeWriter

	decoder := json.NewDecoder(resp.Body)
	err = decoder.Decode(&writer)

	if err != nil {
		return err
	}

	var emptyTables []SnowflakeWriterTable
	writer.Configuration.Parameters.Tables = emptyTables

	var emptyStorageTables []SnowflakeWriterStorageTable
	writer.Configuration.Storage.Input.Tables = emptyStorageTables

	snowflakeConfigJSON, err := json.Marshal(writer.Configuration)

	if err != nil {
		return err
	}

	form := url.Values{}
	form.Add("configuration", string(snowflakeConfigJSON))
	form.Add("changeDescription", "Update Snowflake tables")

	formData := buffer.FromForm(form)

	resp, err = client.PutToStorage(fmt.Sprintf("storage/components/keboola.wr-db-snowflake/configs/%s", d.Id()), formData)

	if hasErrors(err, resp) {
		return extractError(err, resp)
	}

	d.SetId("")

	return nil
}
