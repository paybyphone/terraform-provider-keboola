package keboola

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"

	"github.com/hashicorp/terraform/helper/hashcode"
	"github.com/hashicorp/terraform/helper/schema"
)

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

	table := GoodDataTable{
		Title:           tableID,
		Export:          d.Get("export").(bool),
		Identifier:      d.Get("identifier").(string),
		IncrementalDays: KBCBooleanNumber(d.Get("incremental_days").(int)),
	}

	if d.Get("column") != nil {
		table.Columns = mapColumns(d)
	}

	tableJSON, err := json.Marshal(table)

	if err != nil {
		return err
	}

	jsonData := bytes.NewBuffer(tableJSON)

	resp, err := client.PostToSyrup(fmt.Sprintf("gooddata-writer/v2/%s/tables/%s", writerID, tableID), jsonData)

	if hasErrors(err, resp) {
		return extractError(err, resp)
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
	resp, err := client.GetFromSyrup(fmt.Sprintf("gooddata-writer/v2/%s/tables/%s?include=columns", writerID, d.Id()))

	if hasErrors(err, resp) {
		if err != nil {
			return err
		}

		if resp.StatusCode == 400 || resp.StatusCode == 404 {
			d.SetId("")
			return nil
		}

		return extractError(err, resp)
	}

	var table GoodDataTable

	decoder := json.NewDecoder(resp.Body)
	err = decoder.Decode(&table)

	if err != nil {
		return err
	}

	columns := make([]interface{}, 0, len(table.Columns))

	for _, column := range table.Columns {
		column := map[string]interface{}{
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

		columns = append(columns, column)
	}

	if table.ID == d.Id() {
		d.Set("id", table.ID)
		d.Set("title", table.Title)
		d.Set("export", table.Export)
		d.Set("identifier", table.Identifier)
		d.Set("incremental_days", table.IncrementalDays)
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
	title := d.Get("title").(string)

	table := GoodDataTable{
		Title:           title,
		Export:          d.Get("export").(bool),
		Identifier:      d.Get("identifier").(string),
		IncrementalDays: KBCBooleanNumber(d.Get("incremental_days").(int)),
	}

	if d.Get("column") != nil {
		table.Columns = mapColumns(d)
	}

	tableJSON, err := json.Marshal(table)

	if err != nil {
		return err
	}

	jsonData := bytes.NewBuffer(tableJSON)

	resp, err := client.PatchOnSyrup(fmt.Sprintf("gooddata-writer/v2/%s/tables/%s", writerID, title), jsonData)

	if hasErrors(err, resp) {
		return extractError(err, resp)
	}

	return resourceKeboolaGoodDataTableRead(d, meta)
}

func resourceKeboolaGoodDataTableDelete(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[INFO] Deleting GoodData Table in Keboola: %s", d.Id())

	writerID := d.Get("writer_id").(string)

	client := meta.(*KBCClient)
	resp, err := client.DeleteFromSyrup(fmt.Sprintf("gooddata-writer/v2/%s/tables/%s", writerID, d.Id()))

	if hasErrors(err, resp) {
		return extractError(err, resp)
	}

	d.SetId("")

	return nil
}
