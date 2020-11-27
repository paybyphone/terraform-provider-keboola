package keboola

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/url"

	"github.com/hashicorp/terraform/helper/hashcode"
	"github.com/hashicorp/terraform/helper/schema"
	"github.com/paybyphone/terraform-provider-keboola/plugin/providers/keboola/buffer"
)

const userManagementComponentTemplate = "storage/components/kds-team.app-gd-user-management/configs/%s"

type GoodDataUserManagementComponentV2 struct {
	Name          string                                `json:"name"`
	Description   string                                `json:"description"`
	Configuration GoodDataUserManagementConfigurationV2 `json:"configuration"`
}

type GoodDataUserManagementConfigurationV2 struct {
	Storage    GoodDataUserManagementStorageV2    `json:"storage"`
	Parameters GoodDataUserManagementParametersV2 `json:"parameters"`
}

type GoodDataUserManagementParametersV2 struct {
	Password     string `json:"#password"`
	ProjectId    string `json:"pid"`
	Username     string `json:"username"`
	CustomDomain string `json:"domain_custom"`
}

type GoodDataUserManagementStorageV2 struct {
	Input GoodDataUserManagementStorageInputV2 `json:"input"`
}

type GoodDataUserManagementStorageInputV2 struct {
	Tables []GoodDataUserManagementInputTableV2 `json:"tables"`
}

type GoodDataUserManagementInputTableV2 struct {
	Columns       []string `json:"columns"`
	Source        string   `json:"source"`
	Destination   string   `json:"destination"`
	WhereColumn   string   `json:"where_column"`
	WhereOperator string   `json:"where_operator"`
	WhereValues   []string `json:"where_values"`
}

func resourceKeboolaGoodDataUserManagementV2() *schema.Resource {
	return &schema.Resource{
		Create: resourceGoodDataUserManagementCreateV2,
		Read:   resourceGoodDataUserManagementReadV2,
		Update: resourceGoodDataUserManagementUpdateV2,
		Delete: resourceGoodDataUserManagementDeleteV2,
		Importer: &schema.ResourceImporter{
			State: schema.ImportStatePassthrough,
		},

		Schema: map[string]*schema.Schema{
			"name": {
				Required: true,
				Type:     schema.TypeString,
			},
			"description": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"project_id": {
				Required: true,
				Type:     schema.TypeString,
			},
			"login": {
				Required: true,
				Type:     schema.TypeString,
			},
			"hashed_password": {
				Required:     true,
				Sensitive:    true,
				Type:         schema.TypeString,
				ValidateFunc: validateKBCEncryptedValue,
			},
			"custom_domain": {
				Type:     schema.TypeString,
				Optional: true,
			},
			"input_tables": {
				Type:     schema.TypeSet,
				Optional: true,
				MinItems: 1,
				MaxItems: 1,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"source": {
							Type:     schema.TypeString,
							Required: true,
						},
						"destination": {
							Type:     schema.TypeString,
							Required: true,
						},
						"where_column": {
							Type:     schema.TypeString,
							Optional: true,
							Default:  "",
						},
						"where_values": {
							Type:     schema.TypeList,
							Optional: true,
							Elem: &schema.Schema{
								Type: schema.TypeString,
							},
						},
						"where_operator": {
							Type:     schema.TypeString,
							Optional: true,
							Default:  "eq",
						},
						"columns": {
							Type:     schema.TypeList,
							Optional: true,
							Elem: &schema.Schema{
								Type: schema.TypeString,
							},
						},
					},
				},
			},
		},
	}
}

func resourceGoodDataUserManagementCreateV2(d *schema.ResourceData, meta interface{}) error {
	componentAsBuff, err := serializeUserManagementComponent(d)

	if err != nil {
		return err
	}

	res, err := meta.(*KBCClient).PostToStorage(fmt.Sprintf(userManagementComponentTemplate, ""), componentAsBuff)
	if hasErrors(err, res) {
		return extractError(err, res)
	}

	createResult := CreateResourceResult{}
	err = json.NewDecoder(res.Body).Decode(&createResult)
	if err != nil {
		return err
	}

	d.SetId(string(createResult.ID))
	return resourceGoodDataUserManagementReadV2(d, meta)
}

func resourceGoodDataUserManagementReadV2(d *schema.ResourceData, meta interface{}) error {
	res, err := meta.(*KBCClient).GetFromStorage(fmt.Sprintf(userManagementComponentTemplate, d.Id()))

	if hasErrors(err, res) {
		if res.StatusCode == 404 {
			d.SetId("")
			return nil
		}

		return extractError(err, res)
	}

	component := GoodDataUserManagementComponentV2{}
	err = json.NewDecoder(res.Body).Decode(&component)
	if err != nil {
		return err
	}

	inputTables := make([]interface{}, 0, len(component.Configuration.Storage.Input.Tables))

	for _, table := range component.Configuration.Storage.Input.Tables {
		tableDetails := map[string]interface{}{
			"source":         table.Source,
			"destination":    table.Destination,
			"columns":        table.Columns,
			"where_column":   table.WhereColumn,
			"where_values":   table.WhereValues,
			"where_operator": table.WhereOperator,
		}

		inputTables = append(inputTables, tableDetails)
	}

	parameters := component.Configuration.Parameters

	d.Set("name", component.Name)
	d.Set("description", component.Description)
	d.Set("project_id", parameters.ProjectId)
	d.Set("login", parameters.Username)
	d.Set("hashed_password", parameters.Password)
	d.Set("custom_domain", parameters.CustomDomain)

	d.Set("input_tables", schema.NewSet(func(i interface{}) int {
		return hashcode.String(i.(map[string]interface{})["source"].(string))
	}, inputTables))

	return nil
}

func resourceGoodDataUserManagementUpdateV2(d *schema.ResourceData, meta interface{}) error {
	componentAsBuff, err := serializeUserManagementComponent(d)

	if err != nil {
		return err
	}

	res, err := meta.(*KBCClient).PutToStorage(fmt.Sprintf(userManagementComponentTemplate, d.Id()), componentAsBuff)
	if hasErrors(err, res) {
		return extractError(err, res)
	}

	return resourceGoodDataUserManagementReadV2(d, meta)
}

func resourceGoodDataUserManagementDeleteV2(d *schema.ResourceData, meta interface{}) error {
	res, err := meta.(*KBCClient).DeleteFromStorage(fmt.Sprintf(userManagementComponentTemplate, d.Id()))
	if hasErrors(err, res) {
		return extractError(err, res)
	}

	d.SetId("")
	return nil
}

func serializeUserManagementComponent(d *schema.ResourceData) (*bytes.Buffer, error) {
	configuredTables := d.Get("input_tables").(*schema.Set).List()

	tables := make([]GoodDataUserManagementInputTableV2, 0, len(configuredTables))

	for _, inputTable := range configuredTables {
		tableInput := inputTable.(map[string]interface{})

		table := GoodDataUserManagementInputTableV2{
			Source:        tableInput["source"].(string),
			Destination:   tableInput["destination"].(string),
			WhereColumn:   tableInput["where_column"].(string),
			WhereOperator: tableInput["where_operator"].(string),
		}

		if q := tableInput["columns"]; q != nil {
			table.Columns = AsStringArray(q.([]interface{}))
		}

		if q := tableInput["where_values"]; q != nil {
			table.WhereValues = AsStringArray(q.([]interface{}))
		}

		tables = append(tables, table)
	}

	configuration := GoodDataUserManagementConfigurationV2{
		Storage: GoodDataUserManagementStorageV2{
			Input: GoodDataUserManagementStorageInputV2{
				Tables: tables,
			},
		},
		Parameters: GoodDataUserManagementParametersV2{
			Username:     d.Get("login").(string),
			Password:     d.Get("hashed_password").(string),
			ProjectId:    d.Get("project_id").(string),
			CustomDomain: d.Get("custom_domain").(string),
		},
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
