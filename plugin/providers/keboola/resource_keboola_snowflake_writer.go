package keboola

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"strconv"

	"github.com/hashicorp/terraform/helper/schema"
	"github.com/paybyphone/terraform-provider-keboola/plugin/providers/keboola/buffer"
)

//region Keboola API Contracts

type SnowflakeWriterDatabaseParameters struct {
	HostName          string `json:"host"`
	Database          string `json:"database"`
	Password          string `json:"password,omitempty"`
	EncryptedPassword string `json:"#password,omitempty"`
	Username          string `json:"user"`
	Schema            string `json:"schema"`
	Port              string `json:"port"`
	Driver            string `json:"driver"`
	Warehouse         string `json:"warehouse"`
}

type SnowflakeWriterTableItem struct {
	Name         string `json:"name"`
	DatabaseName string `json:"dbName"`
	Type         string `json:"type"`
	Size         string `json:"size"`
	IsNullable   bool   `json:"nullable"`
	DefaultValue string `json:"default"`
}

type SnowflakeWriterTable struct {
	DatabaseName string                     `json:"dbName"`
	Export       bool                       `json:"export"`
	Incremental  bool                       `json:"incremental"`
	TableID      string                     `json:"tableId"`
	PrimaryKey   []string                   `json:"primaryKey,omitempty"`
	Items        []SnowflakeWriterTableItem `json:"items"`
}

type SnowflakeWriterParameters struct {
	Database SnowflakeDatabaseParameters `json:"db"`
	Tables   []SnowflakeWriterTable      `json:"tables,omitempty"`
}

type SnowflakeWriterStorageTable struct {
	Source        string   `json:"source"`
	Destination   string   `json:"destination"`
	Columns       []string `json:"columns"`
	ChangedSince  string   `json:"changed_since,omitempty"`
	WhereColumn   string   `json:"where_column,omitempty"`
	WhereOperator string   `json:"where_operator,omitempty"`
	WhereValues   []string `json:"where_values,omitempty"`
}

type SnowflakeWriterStorage struct {
	Input struct {
		Tables []SnowflakeWriterStorageTable `json:"tables,omitempty"`
	} `json:"input,omitempty"`
}

type SnowflakeWriterConfiguration struct {
	Parameters SnowflakeWriterParameters `json:"parameters"`
	Storage    SnowflakeWriterStorage    `json:"storage,omitempty"`
}

type ProvisionSnowflakeResponse struct {
	Status      string `json:"status"`
	Credentials struct {
		ID          string `json:"id"`
		HostName    string `json:"hostname"`
		Port        int    `json:"port"`
		Database    string `json:"db"`
		Schema      string `json:"schema"`
		Warehouse   string `json:"warehouse"`
		Username    string `json:"user"`
		Password    string `json:"password"`
		WorkspaceID string `json:"workspaceId"`
	} `json:"credentials"`
}

//SnowflakeWriter is the data model for Snowflake Writers within
//the Keboola Storage API.
type SnowflakeWriter struct {
	ID            string                       `json:"id,omitempty"`
	Name          string                       `json:"name"`
	Description   string                       `json:"description"`
	Configuration SnowflakeWriterConfiguration `json:"configuration"`
}

//endregion

func resourceKeboolaSnowflakeWriter() *schema.Resource {
	return &schema.Resource{
		Create: resourceKeboolaSnowflakeWriterCreate,
		Read:   resourceKeboolaSnowflakeWriterRead,
		Update: resourceKeboolaSnowflakeWriterUpdate,
		Delete: resourceKeboolaSnowflakeWriterDelete,
		Importer: &schema.ResourceImporter{
			State: schema.ImportStatePassthrough,
		},

		Schema: map[string]*schema.Schema{
			"name": {
				Type:     schema.TypeString,
				Required: true,
			},
			"provision_new_instance": {
				Type:     schema.TypeBool,
				Optional: true,
				Default:  true,
				ForceNew: true,
			},
			"snowflake_db_parameters": &snowflakeDBParametersSchema,
			"description": {
				Type:     schema.TypeString,
				Optional: true,
			},
		},
	}
}

func resourceKeboolaSnowflakeWriterCreate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Creating Snowflake Writer in Keboola.")

	client := meta.(*KBCClient)

	d.Partial(true)

	createdSnowflakeID, err := createSnowflakeWriterConfiguration(d.Get("name").(string), d.Get("description").(string), client)

	if err != nil {
		return err
	}

	d.SetPartial("name")
	d.SetPartial("description")

	err = createSnowflakeAccessToken(createdSnowflakeID, client)

	if err != nil {
		return err
	}

	snowflakeDatabaseCredentials := d.Get("snowflake_db_parameters").(map[string]interface{})

	if d.Get("provision_new_instance").(bool) == true {
		provisionedSnowflake, err := provisionSnowflakeInstance(client)

		if err != nil {
			return err
		}

		snowflakeDatabaseCredentials = map[string]interface{}{
			"hostname":        provisionedSnowflake.Credentials.HostName,
			"port":            strconv.Itoa(provisionedSnowflake.Credentials.Port),
			"database":        provisionedSnowflake.Credentials.Database,
			"schema":          provisionedSnowflake.Credentials.Schema,
			"warehouse":       provisionedSnowflake.Credentials.Warehouse,
			"username":        provisionedSnowflake.Credentials.Username,
			"hashed_password": provisionedSnowflake.Credentials.Password,
		}
	}

	err = createSnowflakeCredentialsConfiguration(snowflakeDatabaseCredentials, createdSnowflakeID, client)

	if err != nil {
		return err
	}

	d.SetPartial("snowflake_db_parameters")

	d.SetId(createdSnowflakeID)

	d.Partial(false)

	return resourceKeboolaSnowflakeWriterRead(d, meta)
}

func createSnowflakeWriterConfiguration(name string, description string, client *KBCClient) (createdSnowflakeID string, err error) {
	createWriterForm := url.Values{}
	createWriterForm.Add("name", name)
	createWriterForm.Add("description", description)

	createWriterBuffer := buffer.FromForm(createWriterForm)

	createResponse, err := client.PostToStorage("storage/components/keboola.wr-db-snowflake/configs", createWriterBuffer)

	if hasErrors(err, createResponse) {
		return "", extractError(err, createResponse)
	}

	var createWriterResult CreateResourceResult

	decoder := json.NewDecoder(createResponse.Body)
	err = decoder.Decode(&createWriterResult)

	if err != nil {
		return "", err
	}

	return string(createWriterResult.ID), nil
}

func createSnowflakeAccessToken(snowflakeID string, client *KBCClient) error {
	createAccessTokenForm := url.Values{}
	createAccessTokenForm.Add("description", fmt.Sprintf("wrdbsnowflake_%s", snowflakeID))
	createAccessTokenForm.Add("canManageBuckets", "1")

	createAccessTokenBuffer := buffer.FromForm(createAccessTokenForm)

	createAccessTokenResponse, err := client.PostToStorage("storage/tokens", createAccessTokenBuffer)

	if hasErrors(err, createAccessTokenResponse) {
		return extractError(err, createAccessTokenResponse)
	}

	return nil
}

func provisionSnowflakeInstance(client *KBCClient) (provisionedSnowflakeResponse *ProvisionSnowflakeResponse, err error) {
	provisionSnowflakeBuffer := bytes.NewBufferString("{ \"type\": \"writer\" }")
	provisionSnowflakeResponse, err := client.PostToSyrup("provisioning/snowflake", provisionSnowflakeBuffer)

	if hasErrors(err, provisionSnowflakeResponse) {
		return nil, extractError(err, provisionSnowflakeResponse)
	}

	var provisionedSnowflake ProvisionSnowflakeResponse

	provisionedSnowflakeDecoder := json.NewDecoder(provisionSnowflakeResponse.Body)
	err = provisionedSnowflakeDecoder.Decode(&provisionedSnowflake)

	if err != nil {
		return nil, err
	}

	if provisionSnowflakeResponse.StatusCode < 200 || provisionSnowflakeResponse.StatusCode > 299 {
		return nil, fmt.Errorf("Unable to provision Snowflake instance (status code: %v)", provisionSnowflakeResponse.StatusCode)
	}

	return &provisionedSnowflake, nil
}

func createSnowflakeCredentialsConfiguration(snowflakeCredentials map[string]interface{}, createdSnowflakeID string, client *KBCClient) error {
	snowflakeWriterConfiguration := SnowflakeWriterConfiguration{}

	snowflakeWriterConfiguration.Parameters.Database = mapSnowflakeCredentialsToConfiguration(snowflakeCredentials, true)

	snowflakeWriterConfigurationJSON, err := json.Marshal(snowflakeWriterConfiguration)

	if err != nil {
		return err
	}

	updateConfigurationRequestForm := url.Values{}
	updateConfigurationRequestForm.Add("configuration", string(snowflakeWriterConfigurationJSON))
	updateConfigurationRequestForm.Add("changeDescription", "Created database credentials")

	updateConfigurationRequestBuffer := buffer.FromForm(updateConfigurationRequestForm)

	updateConfigurationResponse, err := client.PutToStorage(fmt.Sprintf("storage/components/keboola.wr-db-snowflake/configs/%s", createdSnowflakeID), updateConfigurationRequestBuffer)

	if hasErrors(err, updateConfigurationResponse) {
		return extractError(err, updateConfigurationResponse)
	}

	return nil
}

func resourceKeboolaSnowflakeWriterRead(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Reading Snowflake Writers from Keboola.")

	client := meta.(*KBCClient)
	getSnowflakeWriterResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.wr-db-snowflake/configs/%s", d.Id()))

	if d.Id() == "" {
		return nil
	}

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

	d.Set("id", snowflakeWriter.ID)
	d.Set("name", snowflakeWriter.Name)
	d.Set("description", snowflakeWriter.Description)

	if d.Get("provision_new_database") == false {
		dbParameters := make(map[string]interface{})

		databaseCredentials := snowflakeWriter.Configuration.Parameters.Database

		dbParameters["hostname"] = databaseCredentials.HostName
		dbParameters["port"] = databaseCredentials.Port
		dbParameters["database"] = databaseCredentials.Database
		dbParameters["schema"] = databaseCredentials.Schema
		dbParameters["warehouse"] = databaseCredentials.Warehouse
		dbParameters["username"] = databaseCredentials.Username
		dbParameters["hashed_password"] = databaseCredentials.EncryptedPassword

		d.Set("snowflake_db_parameters", dbParameters)
	}

	return nil
}

func resourceKeboolaSnowflakeWriterUpdate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Updating Snowflake Writer in Keboola.")

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

	snowflakeCredentials := d.Get("snowflake_db_parameters").(map[string]interface{})

	if d.Get("provision_new_instance").(bool) == false {
		snowflakeWriter.Configuration.Parameters.Database = mapSnowflakeCredentialsToConfiguration(snowflakeCredentials, true)
	}

	snowflakeConfigJSON, err := json.Marshal(snowflakeWriter.Configuration)

	if err != nil {
		return err
	}

	updateCredentialsForm := url.Values{}
	updateCredentialsForm.Add("name", d.Get("name").(string))
	updateCredentialsForm.Add("description", d.Get("description").(string))
	updateCredentialsForm.Add("configuration", string(snowflakeConfigJSON))
	updateCredentialsForm.Add("changeDescription", "Updated Snowflake Writer configuration via Terraform")

	updateCredentialsBuffer := buffer.FromForm(updateCredentialsForm)

	updateCredentialsResponse, err := client.PutToStorage(fmt.Sprintf("storage/components/keboola.wr-db-snowflake/configs/%s", d.Id()), updateCredentialsBuffer)

	if hasErrors(err, updateCredentialsResponse) {
		return extractError(err, updateCredentialsResponse)
	}

	return resourceKeboolaSnowflakeWriterRead(d, meta)
}

func resourceKeboolaSnowflakeWriterDelete(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[INFO] Deleting Snowflake Writer in Keboola: %s", d.Id())

	client := meta.(*KBCClient)
	destroyResponse, err := client.DeleteFromStorage(fmt.Sprintf("storage/components/keboola.wr-db-snowflake/configs/%s", d.Id()))

	if hasErrors(err, destroyResponse) {
		return extractError(err, destroyResponse)
	}

	d.SetId("")

	return nil
}
