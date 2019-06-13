package keboola

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/hashicorp/terraform/helper/hashcode"
	"github.com/hashicorp/terraform/helper/schema"
	"github.com/plmwong/terraform-provider-keboola/plugin/providers/keboola/buffer"
	"log"
	"net/url"
)

const goodDataWriterComponentTemplate = "storage/components/keboola.gooddata-writer/configs/%s"

type DateDimension struct {
	Identifier  string `json:"identifier,omitempty"`
	IncludeTime bool   `json:"includeTime"`
	Template    string `json:"template"`
}

type GoodDataStorageTable struct {
	ChangedSince string   `json:"changed_since"`
	Columns      []string `json:"columns"`
	Source       string   `json:"source"`
}

type GoodDataProject struct {
	ProjectId string `json:"pid"`
}

type GoodDataUser struct {
	Login    string `json:"login"`
	Password string `json:"#password"`
}

type GoodDataWriterParameters struct {
	DateDimensions map[string]DateDimension `json:"dimensions,omitempty"`
	Tables         map[string]GoodDataTable `json:"tables,omitempty"`
	LoadOnly       bool                     `json:"loadOnly"`
	MultiLoad      bool                     `json:"multiLoad"`
	Project        GoodDataProject          `json:"project"`
	User           GoodDataUser             `json:"user"`
}

type GoodDataStorageInput struct {
	Tables []GoodDataStorageTable `json:"tables,omitempty"`
}

type GoodDataStorage struct {
	Input GoodDataStorageInput `json:"input,omitempty"`
}

type GoodDataWriterConfiguration struct {
	Parameters GoodDataWriterParameters `json:"parameters"`
	Storage    GoodDataStorage          `json:"storage,omitempty"`
}

type GoodDataWriterComponent struct {
	Name          string                      `json:"name"`
	Description   string                      `json:"description"`
	Configuration GoodDataWriterConfiguration `json:"configuration"`
}

func resourceKeboolaGoodDataWriterV3() *schema.Resource {
	return &schema.Resource{
		Create: resourceKeboolaGoodDataWriterV3Create,
		Read:   resourceKeboolaGoodDataWriterV3Read,
		Update: resourceKeboolaGoodDataWriterV3Update,
		Delete: resourceKeboolaGoodDataWriterV3Delete,
		Importer: &schema.ResourceImporter{
			State: schema.ImportStatePassthrough,
		},

		Schema: map[string]*schema.Schema{
			"project_id": {
				Required: true,
				Type:     schema.TypeString,
			},
			"name": {
				Required: true,
				Type:     schema.TypeString,
			},
			"description": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"load_only": {
				Type:     schema.TypeBool,
				Default:  false,
				Optional: true,
			},
			"multi_load": {
				Type:     schema.TypeBool,
				Default:  false,
				Optional: true,
			},
			"login": {
				Required: true,
				Type:     schema.TypeString,
			},
			"hashed_password": {
				Required:  true,
				Sensitive: true,
				Type:      schema.TypeString,
			},
			"tables": {
				Type:     schema.TypeSet,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"identifier": {
							Type:     schema.TypeString,
							Optional: true,
						},
						"title": {
							Type:     schema.TypeString,
							Required: true,
						},
						"changed_since": {
							Type:     schema.TypeString,
							Optional: true,
						},
						"columns": {
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
										Optional: true,
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
				},
			},
			"date_dimensions": {
				Type:     schema.TypeSet,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"name": {
							Type:     schema.TypeString,
							Required: true,
						},
						"include_time": {
							Type:     schema.TypeBool,
							Optional: true,
							Default:  false,
						},
						"template": {
							Type:     schema.TypeString,
							Required: true,
						},
						"identifier": {
							Type:     schema.TypeString,
							Optional: true,
						},
					},
				},
			},
		},
	}
}

func resourceKeboolaGoodDataWriterV3Create(d *schema.ResourceData, meta interface{}) error {
	componentAsBuffer, err := serializeGoodDataWriterComponent(d)
	if err != nil {
		return err
	}

	res, err := meta.(*KBCClient).PostToStorage(fmt.Sprintf(goodDataWriterComponentTemplate, ""), componentAsBuffer)
	if hasErrors(err, res) {
		return extractError(err, res)
	}

	createResult := CreateResourceResult{}
	err = json.NewDecoder(res.Body).Decode(&createResult)
	if err != nil {
		return err
	}

	d.SetId(string(createResult.ID))
	return resourceKeboolaGoodDataWriterV3Read(d, meta)
}

func resourceKeboolaGoodDataWriterV3Read(d *schema.ResourceData, meta interface{}) error {
	res, err := meta.(*KBCClient).GetFromStorage(fmt.Sprintf(goodDataWriterComponentTemplate, d.Id()))

	if hasErrors(err, res) {
		if res.StatusCode == 404 {
			d.SetId("")
			return nil
		}

		return extractError(err, res)
	}

	component := GoodDataWriterComponent{}
	err = json.NewDecoder(res.Body).Decode(&component)
	if err != nil {
		return err
	}

	parameters := component.Configuration.Parameters

	tables := make([]interface{}, 0, len(parameters.Tables))
	for _, table := range parameters.Tables {
		columns := make([]interface{}, 0, len(table.Columns))

		for _, column := range table.Columns {
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

		tableDetails := map[string]interface{}{
			"identifier":    table.Identifier,
			"title":         table.Title,
			"changed_since": table.IncrementalDays,
			"columns":       columns,
		}

		tables = append(tables, tableDetails)
	}

	dateDimensions := make([]interface{}, 0, len(parameters.DateDimensions))
	for dimensionName, dateDimension := range parameters.DateDimensions {
		mappedDimension := map[string]interface{}{
			"name":         dimensionName,
			"identifier":   dateDimension.Identifier,
			"include_time": dateDimension.IncludeTime,
			"template":     dateDimension.Template,
		}

		dateDimensions = append(dateDimensions, mappedDimension)
	}

	d.Set("project_id", parameters.Project.ProjectId)
	d.Set("name", component.Name)
	d.Set("description", component.Description)
	d.Set("login", parameters.User.Login)
	d.Set("hashed_password", parameters.User.Password)
	d.Set("load_only", parameters.LoadOnly)
	d.Set("multi_load", parameters.MultiLoad)

	d.Set("tables", schema.NewSet(func(i interface{}) int {
		return hashcode.String(i.(map[string]interface{})["title"].(string))
	}, tables))

	d.Set("date_dimensions", schema.NewSet(func(i interface{}) int {
		return hashcode.String(i.(map[string]interface{})["name"].(string))
	}, dateDimensions))

	return nil
}

func resourceKeboolaGoodDataWriterV3Update(d *schema.ResourceData, meta interface{}) error {
	componentAsBuffer, err := serializeGoodDataWriterComponent(d)
	if err != nil {
		return err
	}
	res, err := meta.(*KBCClient).PutToStorage(fmt.Sprintf(goodDataWriterComponentTemplate, d.Id()), componentAsBuffer)
	if hasErrors(err, res) {
		return extractError(err, res)
	}
	return resourceKeboolaGoodDataWriterV3Read(d, meta)
}

func resourceKeboolaGoodDataWriterV3Delete(d *schema.ResourceData, meta interface{}) error {
	res, err := meta.(*KBCClient).DeleteFromStorage(fmt.Sprintf(goodDataWriterComponentTemplate, d.Id()))
	if hasErrors(err, res) {
		return extractError(err, res)
	}

	d.SetId("")
	return nil
}

func serializeGoodDataWriterComponent(d *schema.ResourceData) (*bytes.Buffer, error) {

	goodDataStorage := GoodDataStorage{
		Input: GoodDataStorageInput{Tables: getStorageTables(d)},
	}

	if len(goodDataStorage.Input.Tables) == 0 {
		goodDataStorage = GoodDataStorage{}
	}

	configuration := GoodDataWriterConfiguration{
		Parameters: GoodDataWriterParameters{
			User: GoodDataUser{
				Login:    d.Get("login").(string),
				Password: d.Get("hashed_password").(string),
			},
			Project: GoodDataProject{
				ProjectId: d.Get("project_id").(string),
			},
			DateDimensions: getDateDimensions(d),
			Tables:         getTables(d),
			LoadOnly:       d.Get("load_only").(bool),
			MultiLoad:      d.Get("multi_load").(bool),
		},
		Storage: goodDataStorage,
	}

	componentForm := url.Values{}
	componentForm.Add("name", d.Get("name").(string))
	componentForm.Add("description", d.Get("description").(string))

	componentAsBytes, err := json.Marshal(configuration)
	if err != nil {
		return nil, err
	}
	componentForm.Add("configuration", string(componentAsBytes))

	return buffer.FromForm(componentForm), err
}

func getDateDimensions(d *schema.ResourceData) map[string]DateDimension {
	dateDimensions := make(map[string]DateDimension)

	if d.Get("date_dimensions") == nil {
		return dateDimensions
	}

	dimensions := d.Get("date_dimensions").(*schema.Set).List()

	for _, dimension := range dimensions {
		input := dimension.(map[string]interface{})

		key := input["name"].(string)
		dim := DateDimension{
			Identifier:  input["identifier"].(string),
			IncludeTime: input["include_time"].(bool),
			Template:    input["template"].(string),
		}

		dateDimensions[key] = dim
	}

	return dateDimensions
}

func getTables(d *schema.ResourceData) map[string]GoodDataTable {
	gdTables := make(map[string]GoodDataTable)

	if d.Get("tables") == nil {
		return gdTables
	}

	tables := d.Get("tables").(*schema.Set).List()

	for _, table := range tables {
		input := table.(map[string]interface{})

		columns := input["columns"].(*schema.Set).List()
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

		key := input["title"].(string)
		gdTables[key] = GoodDataTable{
			Title:      key,
			Identifier: input["identifier"].(string),
			Columns:    mappedColumns,
		}
	}

	return gdTables
}

func getStorageTables(d *schema.ResourceData) []GoodDataStorageTable {

	tables := d.Get("tables").(*schema.Set).List()

	storageTables := make([]GoodDataStorageTable, len(tables))

	for _, table := range tables {
		var columnNames []string

		tableInput := table.(map[string]interface{})
		columns := tableInput["columns"].(*schema.Set).List()

		for _, columnConfig := range columns {
			columnInput := columnConfig.(map[string]interface{})
			columnNames = append(columnNames, columnInput["title"].(string))
		}

		log.Printf("%v", tableInput)
		storageTables = append(storageTables, GoodDataStorageTable{
			Columns:      columnNames,
			ChangedSince: tableInput["changed_since"].(string),
			Source:       tableInput["title"].(string),
		})
	}

	return storageTables
}
