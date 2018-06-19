package keboola

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"strconv"

	"github.com/hashicorp/terraform/helper/schema"
)

type SnowflakeWriterDatabaseParameters struct {
	HostName string `json:"host"`
	Database string `json:"database"`
	Password string `json:"password"`
	Username string `json:"user"`
	Schema   string `json:"schema"`
	Port     string `json:"port"`
	Driver   string `json:"driver"`
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
	Database SnowflakeWriterDatabaseParameters `json:"db"`
	Tables   []SnowflakeWriterTable            `json:"tables"`
}

type SnowflakeWriterStorageTable struct {
	Source      string   `json:"source"`
	Destination string   `json:"destination"`
	Columns     []string `json:"columns"`
}

type SnowflakeWriterStorage struct {
	Input struct {
		Tables []SnowflakeWriterStorageTable `json:"tables"`
	} `json:"input"`
}

type SnowflakeWriterConfiguration struct {
	Parameters SnowflakeWriterParameters `json:"parameters"`
	Storage    SnowflakeWriterStorage    `json:"storage"`
}

type ProvisionSnowflakeResponse struct {
	Status      string `json:"status"`
	Credentials struct {
		ID          int    `json:"id"`
		HostName    string `json:"hostname"`
		Port        int    `json:"port"`
		Database    string `json:"db"`
		Schema      string `json:"schema"`
		Warehouse   string `json:"warehouse"`
		Username    string `json:"user"`
		Password    string `json:"password"`
		WorkspaceID int    `json:"workspaceId"`
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
			"name": &schema.Schema{
				Type:     schema.TypeString,
				Required: true,
			},
			"provision_new_instance": &schema.Schema{
				Type:     schema.TypeBool,
				Optional: true,
				Default:  true,
			},
			"snowflake_db_parameters": &schema.Schema{
				Type:     schema.TypeMap,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"hostname": &schema.Schema{
							Type:     schema.TypeString,
							Required: true,
						},
						"port": &schema.Schema{
							Type:     schema.TypeInt,
							Optional: true,
							Default:  443,
						},
						"database": &schema.Schema{
							Type:     schema.TypeString,
							Optional: true,
						},
						"schema": &schema.Schema{
							Type:     schema.TypeString,
							Optional: true,
						},
						"username": &schema.Schema{
							Type:     schema.TypeString,
							Required: true,
						},
						"password": &schema.Schema{
							Type:     schema.TypeString,
							Required: true,
						},
					},
				},
			},
			"description": &schema.Schema{
				Type:     schema.TypeString,
				Optional: true,
			},
		},
	}
}

func resourceKeboolaSnowflakeWriterCreate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Creating Snowflake Writer in Keboola.")

	client := meta.(*KBCClient)

	createdSnowflakeID, err := createSnowflakeWriterConfiguration(d.Get("name").(string), d.Get("description").(string), client)

	if err != nil {
		return err
	}

	err = createSnowflakeAccessToken(createdSnowflakeID, client)

	if err != nil {
		return err
	}

	snowflakeCredentials := d.Get("snowflake_db_parameters").(map[string]interface{})

	if d.Get("provision_new_instance").(bool) == true {
		provisionedSnowflake, err := provisionSnowflakeInstance(client)

		if err != nil {
			return err
		}

		snowflakeCredentials = map[string]interface{}{
			"hostname": provisionedSnowflake.Credentials.HostName,
			"port":     provisionedSnowflake.Credentials.Port,
			"database": provisionedSnowflake.Credentials.Database,
			"schema":   provisionedSnowflake.Credentials.Schema,
			"username": provisionedSnowflake.Credentials.Username,
			"password": provisionedSnowflake.Credentials.Password,
			"driver":   "snowflake",
		}
	}

	snowflakeCredentialsJSON, err := serialiseSnowflakeCredentials(snowflakeCredentials)

	if err != nil {
		return err
	}

	err = setSnowflakeCredentials(snowflakeCredentialsJSON, createdSnowflakeID, client)

	if err != nil {
		return err
	}

	d.SetId(createdSnowflakeID)

	return resourceKeboolaSnowflakeWriterRead(d, meta)
}

func createSnowflakeWriterConfiguration(name string, description string, client *KBCClient) (createdSnowflakeID string, err error) {
	createWriterForm := url.Values{}
	createWriterForm.Add("name", name)
	createWriterForm.Add("description", description)

	createWriterBuffer := bytes.NewBufferString(createWriterForm.Encode())

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

	createAccessTokenBuffer := bytes.NewBufferString(createAccessTokenForm.Encode())

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

func serialiseSnowflakeCredentials(snowflakeCredentials map[string]interface{}) (creds []byte, err error) {
	snowflakeConfiguration := SnowflakeWriterConfiguration{}

	snowflakeConfiguration.Parameters.Database.HostName = snowflakeCredentials["hostname"].(string)
	snowflakeConfiguration.Parameters.Database.Port = strconv.Itoa(snowflakeCredentials["port"].(int))
	snowflakeConfiguration.Parameters.Database.Database = snowflakeCredentials["database"].(string)
	snowflakeConfiguration.Parameters.Database.Schema = snowflakeCredentials["schema"].(string)
	snowflakeConfiguration.Parameters.Database.Username = snowflakeCredentials["username"].(string)
	snowflakeConfiguration.Parameters.Database.Password = snowflakeCredentials["password"].(string)
	snowflakeConfiguration.Parameters.Database.Driver = snowflakeCredentials["driver"].(string)

	snowflakeConfigurationJSON, err := json.Marshal(snowflakeConfiguration)

	if err != nil {
		return nil, err
	}

	return snowflakeConfigurationJSON, nil
}

func setSnowflakeCredentials(snowflakeCredentials []byte, createdSnowflakeID string, client *KBCClient) error {
	updateCredentialsForm := url.Values{}
	updateCredentialsForm.Add("configuration", string(snowflakeCredentials))
	updateCredentialsForm.Add("changeDescription", "Update credentials")

	updateCredentialsBuffer := bytes.NewBufferString(updateCredentialsForm.Encode())

	updateCredentialsResponse, err := client.PutFormToSyrup(fmt.Sprintf("docker/keboola.wr-db-snowflake/configs/%s", createdSnowflakeID), updateCredentialsBuffer)

	if hasErrors(err, updateCredentialsResponse) {
		return extractError(err, updateCredentialsResponse)
	}

	return nil
}

func resourceKeboolaSnowflakeWriterRead(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Reading Snowflake Writers from Keboola.")

	client := meta.(*KBCClient)
	getResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.wr-db-snowflake/configs/%s", d.Id()))

	if d.Id() == "" {
		return nil
	}

	if hasErrors(err, getResponse) {
		if getResponse.StatusCode == 404 {
			return nil
		}

		return extractError(err, getResponse)
	}

	var snowflakeWriter SnowflakeWriter

	decoder := json.NewDecoder(getResponse.Body)
	err = decoder.Decode(&snowflakeWriter)

	if err != nil {
		return err
	}

	d.Set("id", snowflakeWriter.ID)
	d.Set("name", snowflakeWriter.Name)
	d.Set("description", snowflakeWriter.Description)

	return nil
}

func resourceKeboolaSnowflakeWriterUpdate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Updating Snowflake Writer in Keboola.")

	client := meta.(*KBCClient)

	updateCredentialsForm := url.Values{}
	updateCredentialsForm.Add("name", d.Get("name").(string))
	updateCredentialsForm.Add("description", d.Get("description").(string))

	updateCredentialsBuffer := bytes.NewBufferString(updateCredentialsForm.Encode())

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
