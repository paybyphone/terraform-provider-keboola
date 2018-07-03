package keboola

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"

	"github.com/hashicorp/terraform/helper/hashcode"
	"github.com/hashicorp/terraform/helper/schema"
)

type GoodDataColumn struct {
	Name            string `json:"name"`
	DataType        string `json:"dataType"`
	DataTypeSize    string `json:"dataTypeSize"`
	SchemaReference string `json:"schemaReference"`
	Reference       string `json:"reference"`
	SortOrder       string `json:"sortOrder"`
	SortLabel       string `json:"sortLabel"`
	Format          string `json:"format"`
	DateDimension   string `json:"dateDimension"`
	Title           string `json:"title"`
	Type            string `json:"type"`
}

type GoodDataTable struct {
	ID              string                    `json:"tableId,omitempty"`
	Title           string                    `json:"title"`
	Export          bool                      `json:"export"`
	Identifier      string                    `json:"identifier"`
	IncrementalDays KBCBooleanNumber          `json:"incrementalLoad"`
	Columns         map[string]GoodDataColumn `json:"columns"`
}

func resourceKeboolaGoodDataTable() *schema.Resource {
	return &schema.Resource{
		Create: resourceKeboolaGoodDataTableCreate,
		Read:   resourceKeboolaGoodDataTableRead,
		Update: resourceKeboolaGoodDataTableUpdate,
		Delete: resourceKeboolaGoodDataTableDelete,

		Schema: map[string]*schema.Schema{
			"writer_id": {
				Type:     schema.TypeString,
				Required: true,
			},
			"identifier": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"title": {
				Type:     schema.TypeString,
				Required: true,
			},
			"export": {
				Type:     schema.TypeBool,
				Required: true,
			},
			"incremental_days": {
				Type:     schema.TypeInt,
				Optional: true,
			},
			"column": {
				Type:     schema.TypeSet,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"data_type": {
							Type:     schema.TypeString,
							Optional: true,
						},
						"data_type_size": {
							Type:     schema.TypeString,
							Optional: true,
						},
						"date_dimension": {
							Type:     schema.TypeString,
							Optional: true,
						},
						"reference": {
							Type:     schema.TypeString,
							Optional: true,
						},
						"schema_reference": {
							Type:     schema.TypeString,
							Optional: true,
						},
						"format": {
							Type:     schema.TypeString,
							Optional: true,
						},
						"name": {
							Type:     schema.TypeString,
							Required: true,
						},
						"title": {
							Type:     schema.TypeString,
							Optional: true,
						},
						"type": {
							Type:     schema.TypeString,
							Required: true,
						},
					},
				},
			},
		},
	}
}

func mapColumns(d *schema.ResourceData) map[string]GoodDataColumn {
	columns := d.Get("column").(*schema.Set).List()
	mappedColumns := make(map[string]GoodDataColumn)

	for _, columnConfig := range columns {
		config := columnConfig.(map[string]interface{})

		mappedColumn := GoodDataColumn{
			Name:            config["name"].(string),
			DataType:        config["data_type"].(string),
			DataTypeSize:    config["data_type_size"].(string),
			DateDimension:   config["date_dimension"].(string),
			Reference:       config["reference"].(string),
			SchemaReference: config["schema_reference"].(string),
			Format:          config["format"].(string),
			Title:           config["title"].(string),
			Type:            config["type"].(string),
		}

		mappedColumns[mappedColumn.Title] = mappedColumn
	}

	return mappedColumns
}

func resourceKeboolaGoodDataTableCreate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Creating GoodData Table in Keboola.")

	client := meta.(*KBCClient)

	writerID := d.Get("writer_id").(string)
	tableID := d.Get("title").(string)

	goodDataTableConfig := GoodDataTable{
		Title:           tableID,
		Export:          d.Get("export").(bool),
		Identifier:      d.Get("identifier").(string),
		IncrementalDays: KBCBooleanNumber(d.Get("incremental_days").(int)),
	}

	if d.Get("column") != nil {
		goodDataTableConfig.Columns = mapColumns(d)
	}

	goodDataTableJSON, err := json.Marshal(goodDataTableConfig)

	if err != nil {
		return err
	}

	goodDataTableBuffer := bytes.NewBuffer(goodDataTableJSON)

	createResponse, err := client.PostToSyrup(fmt.Sprintf("gooddata-writer/v2/%s/tables/%s", writerID, tableID), goodDataTableBuffer)

	if hasErrors(err, createResponse) {
		return extractError(err, createResponse)
	}

	resourceKeboolaGoodDataTableUpdate(d, meta)
	d.SetId(tableID)

	return resourceKeboolaGoodDataTableRead(d, meta)
}

func resourceKeboolaGoodDataTableRead(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Reading GoodData Tables from Keboola.")

	if d.Id() == "" {
		return nil
	}

	writerID := d.Get("writer_id").(string)

	client := meta.(*KBCClient)
	getResponse, err := client.GetFromSyrup(fmt.Sprintf("gooddata-writer/v2/%s/tables/%s?include=columns", writerID, d.Id()))

	if hasErrors(err, getResponse) {
		if getResponse.StatusCode == 400 || getResponse.StatusCode == 404 {
			d.SetId("")
			return nil
		}

		return extractError(err, getResponse)
	}

	var goodDataTable GoodDataTable

	decoder := json.NewDecoder(getResponse.Body)
	err = decoder.Decode(&goodDataTable)

	if err != nil {
		return err
	}

	columns := make([]interface{}, 0, len(goodDataTable.Columns))

	for _, column := range goodDataTable.Columns {
		columnDetails := map[string]interface{}{
			"data_type":        column.DataType,
			"data_type_size":   column.DataTypeSize,
			"date_dimension":   column.DateDimension,
			"reference":        column.Reference,
			"schema_reference": column.SchemaReference,
			"format":           column.Format,
			"name":             column.Name,
			"title":            column.Title,
			"type":             column.Type,
		}

		columns = append(columns, columnDetails)
	}

	if goodDataTable.ID == d.Id() {
		d.Set("id", goodDataTable.ID)
		d.Set("title", goodDataTable.Title)
		d.Set("export", goodDataTable.Export)
		d.Set("identifier", goodDataTable.Identifier)
		d.Set("incremental_days", goodDataTable.IncrementalDays)
		d.Set("column", schema.NewSet(columnSetHash, columns))
	}

	return nil
}

func columnSetHash(v interface{}) int {
	var buffer bytes.Buffer
	m := v.(map[string]interface{})

	if v, ok := m["name"]; ok {
		buffer.WriteString(fmt.Sprintf("%s-", v.(string)))
	}

	return hashcode.String(buffer.String())
}

func resourceKeboolaGoodDataTableUpdate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Updating GoodData Table in Keboola.")

	client := meta.(*KBCClient)

	writerID := d.Get("writer_id").(string)
	tableID := d.Get("title").(string)

	goodDataTableConfig := GoodDataTable{
		Title:           tableID,
		Export:          d.Get("export").(bool),
		Identifier:      d.Get("identifier").(string),
		IncrementalDays: KBCBooleanNumber(d.Get("incremental_days").(int)),
	}

	if d.Get("column") != nil {
		goodDataTableConfig.Columns = mapColumns(d)
	}

	goodDataTableJSON, err := json.Marshal(goodDataTableConfig)

	if err != nil {
		return err
	}

	goodDataTableBuffer := bytes.NewBuffer(goodDataTableJSON)

	updateResponse, err := client.PatchOnSyrup(fmt.Sprintf("gooddata-writer/v2/%s/tables/%s", writerID, tableID), goodDataTableBuffer)

	if hasErrors(err, updateResponse) {
		return extractError(err, updateResponse)
	}

	return resourceKeboolaGoodDataTableRead(d, meta)
}

func resourceKeboolaGoodDataTableDelete(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[INFO] Deleting GoodData Table in Keboola: %s", d.Id())

	writerID := d.Get("writer_id").(string)

	client := meta.(*KBCClient)
	destroyResponse, err := client.DeleteFromSyrup(fmt.Sprintf("gooddata-writer/v2/%s/tables/%s", writerID, d.Id()))

	if hasErrors(err, destroyResponse) {
		return extractError(err, destroyResponse)
	}

	d.SetId("")

	return nil
}
