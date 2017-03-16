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
	ID          string                    `json:"tableId,omitempty"`
	Title       string                    `json:"title"`
	Export      bool                      `json:"export"`
	Identifier  string                    `json:"identifier"`
	Incremental KBCBoolean                `json:"incrementalLoad"`
	Columns     map[string]GoodDataColumn `json:"columns"`
}

func resourceKeboolaGoodDataTable() *schema.Resource {
	return &schema.Resource{
		Create: resourceKeboolaGoodDataTableCreate,
		Read:   resourceKeboolaGoodDataTableRead,
		Update: resourceKeboolaGoodDataTableUpdate,
		Delete: resourceKeboolaGoodDataTableDelete,

		Schema: map[string]*schema.Schema{
			"writer_id": &schema.Schema{
				Type:     schema.TypeString,
				Required: true,
			},
			"identifier": &schema.Schema{
				Type:     schema.TypeString,
				Optional: true,
			},
			"title": &schema.Schema{
				Type:     schema.TypeString,
				Required: true,
			},
			"export": &schema.Schema{
				Type:     schema.TypeBool,
				Required: true,
			},
			"incremental": &schema.Schema{
				Type:     schema.TypeBool,
				Optional: true,
				Default:  false,
			},
			"column": &schema.Schema{
				Type:     schema.TypeSet,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"dataType": &schema.Schema{
							Type:     schema.TypeString,
							Optional: true,
						},
						"dataTypeSize": &schema.Schema{
							Type:     schema.TypeString,
							Optional: true,
						},
						"format": &schema.Schema{
							Type:     schema.TypeString,
							Optional: true,
						},
						"name": &schema.Schema{
							Type:     schema.TypeString,
							Required: true,
						},
						"title": &schema.Schema{
							Type:     schema.TypeString,
							Optional: true,
						},
						"type": &schema.Schema{
							Type:     schema.TypeString,
							Required: true,
						},
					},
				},
			},
		},
	}
}

func mapColumns(d *schema.ResourceData, meta interface{}) map[string]GoodDataColumn {
	columns := d.Get("column").(*schema.Set).List()
	mappedColumns := make(map[string]GoodDataColumn)

	for _, columnConfig := range columns {
		config := columnConfig.(map[string]interface{})

		mappedColumn := GoodDataColumn{
			Name:         config["name"].(string),
			DataType:     config["dataType"].(string),
			DataTypeSize: config["dataTypeSize"].(string),
			Format:       config["format"].(string),
			Title:        config["title"].(string),
			Type:         config["type"].(string),
		}

		mappedColumns[mappedColumn.Title] = mappedColumn
	}

	return mappedColumns
}

func resourceKeboolaGoodDataTableCreate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Creating GoodData Table in Keboola.")

	client := meta.(*KbcClient)

	writerID := d.Get("writer_id").(string)
	tableID := d.Get("title").(string)
	goodDataTableConfig := GoodDataTable{
		Title:       tableID,
		Export:      d.Get("export").(bool),
		Identifier:  d.Get("identifier").(string),
		Incremental: d.Get("incremental").(KBCBoolean),
	}

	if d.Get("column") != nil {
		goodDataTableConfig.Columns = mapColumns(d, meta)
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

	client := meta.(*KbcClient)
	getResponse, err := client.GetFromSyrup(fmt.Sprintf("gooddata-writer/v2/%s/tables/%s?include=columns", writerID, d.Id()))

	if hasErrors(err, getResponse) {
		if getResponse.StatusCode == 400 || getResponse.StatusCode == 404 {
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
			"dataType":     column.DataType,
			"dataTypeSize": column.DataTypeSize,
			"format":       column.Format,
			"name":         column.Name,
			"title":        column.Title,
			"type":         column.Type,
		}

		columns = append(columns, columnDetails)
	}

	if goodDataTable.ID == d.Id() {
		d.Set("id", goodDataTable.ID)
		d.Set("title", goodDataTable.Title)
		d.Set("export", goodDataTable.Export)
		d.Set("identifier", goodDataTable.Identifier)
		d.Set("incremental", goodDataTable.Incremental)
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

	client := meta.(*KbcClient)

	writerID := d.Get("writer_id").(string)
	tableID := d.Get("title").(string)
	goodDataTableConfig := GoodDataTable{
		Title:       tableID,
		Export:      d.Get("export").(bool),
		Identifier:  d.Get("identifier").(string),
		Incremental: d.Get("incremental").(KBCBoolean),
	}

	if d.Get("column") != nil {
		goodDataTableConfig.Columns = mapColumns(d, meta)
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

	client := meta.(*KbcClient)
	destroyResponse, err := client.DeleteFromSyrup(fmt.Sprintf("gooddata-writer/v2/%s/tables/%s", writerID, d.Id()))

	if hasErrors(err, destroyResponse) {
		return extractError(err, destroyResponse)
	}

	d.SetId("")

	return nil
}
