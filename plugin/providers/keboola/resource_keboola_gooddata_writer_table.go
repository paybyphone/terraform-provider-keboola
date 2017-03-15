package keboola

import (
	"bytes"
	"encoding/json"
	"errors"
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

type ConvertibleBoolean bool

func (bit *ConvertibleBoolean) UnmarshalJSON(data []byte) error {
	asString := string(data)
	if asString == "1" || asString == "true" {
		*bit = true
	} else if asString == "0" || asString == "false" {
		*bit = false
	} else {
		return errors.New(fmt.Sprintf("Boolean unmarshal error: invalid input %s", asString))
	}
	return nil
}

type GoodDataTable struct {
	ID          string                    `json:"tableId,omitempty"`
	Title       string                    `json:"title"`
	Export      bool                      `json:"export"`
	Identifier  string                    `json:"identifier"`
	Incremental ConvertibleBoolean        `json:"incrementalLoad"`
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
	log.Print("[INFO] Creating GoodData Table in Keboola.")

	client := meta.(*KbcClient)

	writerID := d.Get("writer_id").(string)
	tableID := d.Get("title").(string)
	gdTableConf := GoodDataTable{
		Title:       tableID,
		Export:      d.Get("export").(bool),
		Identifier:  d.Get("identifier").(string),
		Incremental: d.Get("incremental").(ConvertibleBoolean),
	}

	if d.Get("column") != nil {
		gdTableConf.Columns = mapColumns(d, meta)
	}

	gdTableJSON, err := json.Marshal(gdTableConf)

	if err != nil {
		return err
	}

	gdTableBuffer := bytes.NewBuffer(gdTableJSON)

	postResp, err := client.PostToSyrup(fmt.Sprintf("gooddata-writer/v2/%s/tables/%s", writerID, tableID), gdTableBuffer)

	if hasErrors(err, postResp) {
		return extractError(err, postResp)
	}

	d.SetId(tableID)
	resourceKeboolaGoodDataTableUpdate(d, meta)

	return resourceKeboolaGoodDataTableRead(d, meta)
}

func resourceKeboolaGoodDataTableRead(d *schema.ResourceData, meta interface{}) error {
	log.Print("[INFO] Reading GoodData Tables from Keboola.")

	if d.Id() == "" {
		return nil
	}

	writerID := d.Get("writer_id").(string)

	client := meta.(*KbcClient)
	getResp, err := client.GetFromSyrup(fmt.Sprintf("gooddata-writer/v2/%s/tables/%s?include=columns", writerID, d.Id()))

	if hasErrors(err, getResp) {
		if getResp.StatusCode == 400 || getResp.StatusCode == 404 {
			return nil
		}

		return extractError(err, getResp)
	}

	var goodDataTable GoodDataTable

	decoder := json.NewDecoder(getResp.Body)
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
		d.Set("column", schema.NewSet(columnHash, columns))
	}

	return nil
}

func columnHash(v interface{}) int {
	var buf bytes.Buffer
	m := v.(map[string]interface{})

	if v, ok := m["name"]; ok {
		buf.WriteString(fmt.Sprintf("%s-", v.(string)))
	}

	return hashcode.String(buf.String())
}

func resourceKeboolaGoodDataTableUpdate(d *schema.ResourceData, meta interface{}) error {
	log.Print("[INFO] Updating GoodData Table in Keboola.")

	client := meta.(*KbcClient)

	writerID := d.Get("writer_id").(string)
	tableID := d.Get("title").(string)
	gdTableConf := GoodDataTable{
		Title:       tableID,
		Export:      d.Get("export").(bool),
		Identifier:  d.Get("identifier").(string),
		Incremental: d.Get("incremental").(ConvertibleBoolean),
	}

	if d.Get("column") != nil {
		gdTableConf.Columns = mapColumns(d, meta)
	}

	gdTableJSON, err := json.Marshal(gdTableConf)

	if err != nil {
		return err
	}

	gdTableBuffer := bytes.NewBuffer(gdTableJSON)

	patchResp, err := client.PatchOnSyrup(fmt.Sprintf("gooddata-writer/v2/%s/tables/%s", writerID, tableID), gdTableBuffer)

	if hasErrors(err, patchResp) {
		return extractError(err, patchResp)
	}

	return resourceKeboolaGoodDataTableRead(d, meta)
}

func resourceKeboolaGoodDataTableDelete(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[INFO] Deleting GoodData Table in Keboola: %s", d.Id())

	writerID := d.Get("writer_id").(string)

	client := meta.(*KbcClient)
	delResp, err := client.DeleteFromSyrup(fmt.Sprintf("gooddata-writer/v2/%s/tables/%s", writerID, d.Id()))

	if hasErrors(err, delResp) {
		return extractError(err, delResp)
	}

	d.SetId("")

	return nil
}
