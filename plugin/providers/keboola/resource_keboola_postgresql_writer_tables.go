package keboola

import (
	"encoding/json"
	"fmt"
	"log"
	"net/url"

	"github.com/hashicorp/terraform/helper/schema"
	"github.com/plmwong/terraform-provider-keboola/plugin/providers/keboola/buffer"
)

func resourceKeboolaPostgreSQLWriterTablesCreate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Creating PostgreSQL Writer Tables in Keboola.")

	writerID := d.Get("writer_id").(string)
	tables := d.Get("table").([]interface{})

	mappedTables := make([]PostgreSQLWriterTable, 0, len(tables))
	storageTables := make([]PostgreSQLWriterStorageTable, 0, len(tables))

	for _, table := range tables {
		config := table.(map[string]interface{})

		mappedTable := PostgreSQLWriterTable{
			DatabaseName: config["db_name"].(string),
			Export:       config["export"].(bool),
			TableID:      config["table_id"].(string),
			Incremental:  config["incremental"].(bool),
		}

		if q := config["primary_key"]; q != nil {
			mappedTable.PrimaryKey = AsStringArray(q.([]interface{}))
		}

		storageTable := PostgreSQLWriterStorageTable{
			Source:      mappedTable.TableID,
			Destination: fmt.Sprintf("%s.csv", mappedTable.TableID),
		}

		columnConfigs := config["column"].([]interface{})
		mappedColumns := make([]PostgreSQLWriterTableItem, 0, len(columnConfigs))
		columnNames := make([]string, 0, len(columnConfigs))
		for _, column := range columnConfigs {
			columnConfig := column.(map[string]interface{})

			mappedColumn := PostgreSQLWriterTableItem{
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

	resp, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.wr-db-pgsql/configs/%s", writerID))

	if hasErrors(err, resp) {
		return extractError(err, resp)
	}

	var writer PostgreSQLWriter

	decoder := json.NewDecoder(resp.Body)
	err = decoder.Decode(&writer)

	if err != nil {
		return err
	}

	writer.Configuration.Parameters.Tables = mappedTables
	writer.Configuration.Storage.Input.Tables = storageTables

	postgresqlConfigJSON, err := json.Marshal(writer.Configuration)

	if err != nil {
		return err
	}

	form := url.Values{}
	form.Add("configuration", string(postgresqlConfigJSON))
	form.Add("change_description", "Update PostgreSQL tables")

	formData := buffer.FromForm(form)

	resp, err = client.PutToStorage(fmt.Sprintf("storage/components/keboola.wr-db-pgsql/configs/%s", writerID), formData)

	if hasErrors(err, resp) {
		return extractError(err, resp)
	}

	d.SetId(writerID)

	return resourceKeboolaPostgreSQLWriterTablesRead(d, meta)
}

func resourceKeboolaPostgreSQLWriterTablesRead(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Reading PostgreSQL Writer Tables from Keboola.")

	if d.Id() == "" {
		return nil
	}

	client := meta.(*KBCClient)

	resp, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.wr-db-pgsql/configs/%s", d.Id()))

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

	var writer PostgreSQLWriter

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

func resourceKeboolaPostgreSQLWriterTablesUpdate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Updating PostgreSQL Writer Tables in Keboola.")

	tables := d.Get("table").([]interface{})

	mappedTables := make([]PostgreSQLWriterTable, 0, len(tables))
	storageTables := make([]PostgreSQLWriterStorageTable, 0, len(tables))

	for _, table := range tables {
		config := table.(map[string]interface{})

		mappedTable := PostgreSQLWriterTable{
			DatabaseName: config["db_name"].(string),
			Export:       config["export"].(bool),
			TableID:      config["table_id"].(string),
			Incremental:  config["incremental"].(bool),
		}

		if q := config["primary_key"]; q != nil {
			mappedTable.PrimaryKey = AsStringArray(q.([]interface{}))
		}

		storageTable := PostgreSQLWriterStorageTable{
			Source:      mappedTable.TableID,
			Destination: fmt.Sprintf("%s.csv", mappedTable.TableID),
		}

		columnConfigs := config["column"].([]interface{})
		mappedColumns := make([]PostgreSQLWriterTableItem, 0, len(columnConfigs))
		columnNames := make([]string, 0, len(columnConfigs))
		for _, column := range columnConfigs {
			columnConfig := column.(map[string]interface{})

			mappedColumn := PostgreSQLWriterTableItem{
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

	resp, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.wr-db-pgsql/configs/%s", d.Id()))

	if hasErrors(err, resp) {
		return extractError(err, resp)
	}

	var writer PostgreSQLWriter

	decoder := json.NewDecoder(resp.Body)
	err = decoder.Decode(&writer)

	if err != nil {
		return err
	}

	writer.Configuration.Parameters.Tables = mappedTables
	writer.Configuration.Storage.Input.Tables = storageTables

	postgresqlConfigJSON, err := json.Marshal(writer.Configuration)

	if err != nil {
		return err
	}

	form := url.Values{}
	form.Add("configuration", string(postgresqlConfigJSON))
	form.Add("changeDescription", "Update PostgreSQL tables")

	formData := buffer.FromForm(form)

	resp, err = client.PutToStorage(fmt.Sprintf("storage/components/keboola.wr-db-pgsql/configs/%s", d.Id()), formData)

	if hasErrors(err, resp) {
		return extractError(err, resp)
	}

	return resourceKeboolaPostgreSQLWriterTablesRead(d, meta)
}

func resourceKeboolaPostgreSQLWriterTablesDelete(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[INFO] Clearing PostgreSQL Writer Tables in Keboola: %s", d.Id())

	client := meta.(*KBCClient)

	resp, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.wr-db-pgsql/configs/%s", d.Id()))

	if hasErrors(err, resp) {
		return extractError(err, resp)
	}

	var writer PostgreSQLWriter

	decoder := json.NewDecoder(resp.Body)
	err = decoder.Decode(&writer)

	if err != nil {
		return err
	}

	var emptyTables []PostgreSQLWriterTable
	writer.Configuration.Parameters.Tables = emptyTables

	var emptyStorageTables []PostgreSQLWriterStorageTable
	writer.Configuration.Storage.Input.Tables = emptyStorageTables

	postgresqlConfigJSON, err := json.Marshal(writer.Configuration)

	if err != nil {
		return err
	}

	form := url.Values{}
	form.Add("configuration", string(postgresqlConfigJSON))
	form.Add("changeDescription", "Update PostgreSQL tables")

	formData := buffer.FromForm(form)

	resp, err = client.PutToStorage(fmt.Sprintf("storage/components/keboola.wr-db-pgsql/configs/%s", d.Id()), formData)

	if hasErrors(err, resp) {
		return extractError(err, resp)
	}

	d.SetId("")

	return nil
}
