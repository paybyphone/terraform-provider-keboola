package keboola

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/url"

	"github.com/hashicorp/terraform/helper/schema"
)

func resourceKeboolaPostgreSQLWriterTables() *schema.Resource {
	return &schema.Resource{
		Create: resourceKeboolaPostgreSQLWriterTablesCreate,
		Read:   resourceKeboolaPostgreSQLWriterTablesRead,
		Update: resourceKeboolaPostgreSQLWriterTablesUpdate,
		Delete: resourceKeboolaPostgreSQLWriterTablesDelete,

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
						"primaryKey": &schema.Schema{
							Type:     schema.TypeList,
							Optional: true,
							Elem: &schema.Schema{
								Type: schema.TypeString,
							},
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

func resourceKeboolaPostgreSQLWriterTablesCreate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Creating PostgreSQL Writer Tables in Keboola.")

	writerID := d.Get("writer_id").(string)
	tables := d.Get("table").([]interface{})

	mappedTables := make([]PostgreSQLWriterTable, 0, len(tables))
	storageTables := make([]PostgreSQLWriterStorageTable, 0, len(tables))

	for _, table := range tables {
		config := table.(map[string]interface{})

		mappedTable := PostgreSQLWriterTable{
			DatabaseName: config["dbName"].(string),
			Export:       config["export"].(bool),
			TableID:      config["tableId"].(string),
			Incremental:  config["incremental"].(bool),
		}

		if q := config["primaryKey"]; q != nil {
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
				DatabaseName: columnConfig["dbName"].(string),
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

	getWriterResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.wr-db-pgsql/configs/%s", writerID))

	if hasErrors(err, getWriterResponse) {
		return extractError(err, getWriterResponse)
	}

	var postgresqlWriter PostgreSQLWriter

	decoder := json.NewDecoder(getWriterResponse.Body)
	err = decoder.Decode(&postgresqlWriter)

	if err != nil {
		return err
	}

	postgresqlWriter.Configuration.Parameters.Tables = mappedTables
	postgresqlWriter.Configuration.Storage.Input.Tables = storageTables

	postgresqlConfigJSON, err := json.Marshal(postgresqlWriter.Configuration)

	if err != nil {
		return err
	}

	updatePostgreSQLForm := url.Values{}
	updatePostgreSQLForm.Add("configuration", string(postgresqlConfigJSON))
	updatePostgreSQLForm.Add("changeDescription", "Update PostgreSQL tables")

	updatePostgreSQLBuffer := bytes.NewBufferString(updatePostgreSQLForm.Encode())

	updateResponse, err := client.PutFormToSyrup(fmt.Sprintf("docker/keboola.wr-db-pgsql/configs/%s", writerID), updatePostgreSQLBuffer)

	if hasErrors(err, updateResponse) {
		return extractError(err, updateResponse)
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

	getResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.wr-db-pgsql/configs/%s", d.Id()))

	if hasErrors(err, getResponse) {
		return extractError(err, getResponse)
	}

	var postgresqlWriter PostgreSQLWriter

	decoder := json.NewDecoder(getResponse.Body)
	err = decoder.Decode(&postgresqlWriter)

	if err != nil {
		return err
	}

	var tables []map[string]interface{}

	for _, tableConfig := range postgresqlWriter.Configuration.Parameters.Tables {
		tableDetails := map[string]interface{}{
			"dbName":      tableConfig.DatabaseName,
			"export":      tableConfig.Export,
			"tableId":     tableConfig.TableID,
			"incremental": tableConfig.Incremental,
			"primaryKey":  tableConfig.PrimaryKey,
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

func resourceKeboolaPostgreSQLWriterTablesUpdate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Updating PostgreSQL Writer Tables in Keboola.")

	tables := d.Get("table").([]interface{})

	mappedTables := make([]PostgreSQLWriterTable, 0, len(tables))
	storageTables := make([]PostgreSQLWriterStorageTable, 0, len(tables))

	for _, table := range tables {
		config := table.(map[string]interface{})

		mappedTable := PostgreSQLWriterTable{
			DatabaseName: config["dbName"].(string),
			Export:       config["export"].(bool),
			TableID:      config["tableId"].(string),
			Incremental:  config["incremental"].(bool),
		}

		if q := config["primaryKey"]; q != nil {
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
				DatabaseName: columnConfig["dbName"].(string),
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

	getWriterResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.wr-db-pgsql/configs/%s", d.Id()))

	if hasErrors(err, getWriterResponse) {
		return extractError(err, getWriterResponse)
	}

	var postgresqlWriter PostgreSQLWriter

	decoder := json.NewDecoder(getWriterResponse.Body)
	err = decoder.Decode(&postgresqlWriter)

	if err != nil {
		return err
	}

	postgresqlWriter.Configuration.Parameters.Tables = mappedTables
	postgresqlWriter.Configuration.Storage.Input.Tables = storageTables

	postgresqlConfigJSON, err := json.Marshal(postgresqlWriter.Configuration)

	if err != nil {
		return err
	}

	updatePostgreSQLForm := url.Values{}
	updatePostgreSQLForm.Add("configuration", string(postgresqlConfigJSON))
	updatePostgreSQLForm.Add("changeDescription", "Update PostgreSQL tables")

	updatePostgreSQLBuffer := bytes.NewBufferString(updatePostgreSQLForm.Encode())

	updateResponse, err := client.PutFormToSyrup(fmt.Sprintf("docker/keboola.wr-db-pgsql/configs/%s", d.Id()), updatePostgreSQLBuffer)

	if hasErrors(err, updateResponse) {
		return extractError(err, updateResponse)
	}

	return resourceKeboolaPostgreSQLWriterTablesRead(d, meta)
}

func resourceKeboolaPostgreSQLWriterTablesDelete(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[INFO] Clearing PostgreSQL Writer Tables in Keboola: %s", d.Id())

	client := meta.(*KBCClient)

	getWriterResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.wr-db-pgsql/configs/%s", d.Id()))

	if hasErrors(err, getWriterResponse) {
		return extractError(err, getWriterResponse)
	}

	var postgresqlWriter PostgreSQLWriter

	decoder := json.NewDecoder(getWriterResponse.Body)
	err = decoder.Decode(&postgresqlWriter)

	if err != nil {
		return err
	}

	var emptyTables []PostgreSQLWriterTable
	postgresqlWriter.Configuration.Parameters.Tables = emptyTables

	var emptyStorageTables []PostgreSQLWriterStorageTable
	postgresqlWriter.Configuration.Storage.Input.Tables = emptyStorageTables

	postgresqlConfigJSON, err := json.Marshal(postgresqlWriter.Configuration)

	if err != nil {
		return err
	}

	clearPostgreSQLTablesForm := url.Values{}
	clearPostgreSQLTablesForm.Add("configuration", string(postgresqlConfigJSON))
	clearPostgreSQLTablesForm.Add("changeDescription", "Update PostgreSQL tables")

	clearPostgreSQLTablesBuffer := bytes.NewBufferString(clearPostgreSQLTablesForm.Encode())

	clearResponse, err := client.PutFormToSyrup(fmt.Sprintf("docker/keboola.wr-db-pgsql/configs/%s", d.Id()), clearPostgreSQLTablesBuffer)

	if hasErrors(err, clearResponse) {
		return extractError(err, clearResponse)
	}

	d.SetId("")

	return nil
}
