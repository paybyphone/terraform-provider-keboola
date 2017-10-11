package keboola

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/url"

	"github.com/hashicorp/terraform/helper/schema"
)

type PostgreSQLWriterDatabaseParameters struct {
	HostName string `json:"host"`
	Database string `json:"database"`
	Password string `json:"#password"`
	Username string `json:"user"`
	Schema   string `json:"schema"`
	Port     string `json:"port"`
	Driver   string `json:"driver"`
}

type PostgreSQLWriterTableItem struct {
	Name         string `json:"name"`
	DatabaseName string `json:"dbName"`
	Type         string `json:"type"`
	Size         string `json:"size"`
	IsNullable   bool   `json:"nullable"`
	DefaultValue string `json:"default"`
}

type PostgreSQLWriterTable struct {
	DatabaseName string                      `json:"dbName"`
	Export       bool                        `json:"export"`
	Incremental  bool                        `json:"incremental"`
	TableID      string                      `json:"tableId"`
	PrimaryKey   []string                    `json:"primaryKey,omitempty"`
	Items        []PostgreSQLWriterTableItem `json:"items"`
}

type PostgreSQLWriterParameters struct {
	Database PostgreSQLWriterDatabaseParameters `json:"db"`
	Tables   []PostgreSQLWriterTable            `json:"tables,omitempty"`
}

type PostgreSQLWriterStorageTable struct {
	Source      string   `json:"source"`
	Destination string   `json:"destination"`
	Columns     []string `json:"columns"`
}

type PostgreSQLWriterStorage struct {
	Input struct {
		Tables []PostgreSQLWriterStorageTable `json:"tables,omitempty"`
	} `json:"input"`
}

type PostgreSQLWriterConfiguration struct {
	Parameters PostgreSQLWriterParameters `json:"parameters,omitempty"`
	Storage    PostgreSQLWriterStorage    `json:"storage,omitempty"`
}

type PostgreSQLWriter struct {
	ID            string                        `json:"id,omitempty"`
	Name          string                        `json:"name"`
	Description   string                        `json:"description"`
	Configuration PostgreSQLWriterConfiguration `json:"configuration"`
}

func resourceKeboolaPostgreSQLWriter() *schema.Resource {
	return &schema.Resource{
		Create: resourceKeboolaPostgreSQLWriterCreate,
		Read:   resourceKeboolaPostgreSQLWriterRead,
		Update: resourceKeboolaPostgreSQLWriterUpdate,
		Delete: resourceKeboolaPostgreSQLWriterDelete,
		Importer: &schema.ResourceImporter{
			State: schema.ImportStatePassthrough,
		},

		Schema: map[string]*schema.Schema{
			"name": &schema.Schema{
				Type:     schema.TypeString,
				Required: true,
			},
			"description": &schema.Schema{
				Type:     schema.TypeString,
				Optional: true,
			},
			"dbParameters": &schema.Schema{
				Type:     schema.TypeMap,
				Optional: true,
			},
		},
	}
}

func resourceKeboolaPostgreSQLWriterCreate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Creating PostgreSQL Writer in Keboola.")

	client := meta.(*KBCClient)

	createdPostgreSQLID, err := createPostgreSQLWriterConfiguration(d.Get("name").(string), d.Get("description").(string), client)

	if err != nil {
		return err
	}

	params := d.Get("dbParameters").(map[string]interface{})

	if len(params) > 0 {
		err = setPostgreSQLCredentials(createdPostgreSQLID, params, client)

		if err != nil {
			return err
		}
	}

	d.SetId(createdPostgreSQLID)

	return resourceKeboolaPostgreSQLWriterRead(d, meta)
}

func createPostgreSQLWriterConfiguration(name string, description string, client *KBCClient) (createdPostgreSQLID string, err error) {
	createWriterForm := url.Values{}
	createWriterForm.Add("name", name)
	createWriterForm.Add("description", description)

	createWriterBuffer := bytes.NewBufferString(createWriterForm.Encode())

	createResponse, err := client.PostToStorage("storage/components/keboola.wr-db-pgsql/configs", createWriterBuffer)

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

func setPostgreSQLCredentials(createdPostgreSQLID string, params map[string]interface{}, client *KBCClient) error {
	postgresqlCredentials := PostgreSQLWriterConfiguration{}

	postgresqlCredentials.Parameters.Database.HostName = params["host"].(string)
	postgresqlCredentials.Parameters.Database.Port = params["port"].(string)
	postgresqlCredentials.Parameters.Database.Database = params["database"].(string)
	postgresqlCredentials.Parameters.Database.Schema = params["schema"].(string)
	postgresqlCredentials.Parameters.Database.Username = params["username"].(string)
	postgresqlCredentials.Parameters.Database.Password = params["hashedPassword"].(string)
	postgresqlCredentials.Parameters.Database.Driver = "pgsql"

	postgresqlCredentialsJSON, err := json.Marshal(postgresqlCredentials)

	if err != nil {
		return err
	}

	updateCredentialsForm := url.Values{}
	updateCredentialsForm.Add("configuration", string(postgresqlCredentialsJSON))
	updateCredentialsForm.Add("changeDescription", "Update credentials")

	updateCredentialsBuffer := bytes.NewBufferString(updateCredentialsForm.Encode())
	updateCredentialsResponse, err := client.PutToStorage(fmt.Sprintf("storage/components/keboola.wr-db-pgsql/configs/%s", createdPostgreSQLID), updateCredentialsBuffer)

	if hasErrors(err, updateCredentialsResponse) {
		return extractError(err, updateCredentialsResponse)
	}

	return nil
}

func resourceKeboolaPostgreSQLWriterRead(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Reading PostgreSQL Writers from Keboola.")

	client := meta.(*KBCClient)
	getResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.wr-db-pgsql/configs/%s", d.Id()))

	if d.Id() == "" {
		return nil
	}

	if hasErrors(err, getResponse) {
		if getResponse.StatusCode == 404 {
			return nil
		}

		return extractError(err, getResponse)
	}

	var postgresqlWriter PostgreSQLWriter

	decoder := json.NewDecoder(getResponse.Body)
	err = decoder.Decode(&postgresqlWriter)

	if err != nil {
		return err
	}

	d.Set("id", postgresqlWriter.ID)
	d.Set("name", postgresqlWriter.Name)
	d.Set("description", postgresqlWriter.Description)

	return nil
}

func resourceKeboolaPostgreSQLWriterUpdate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Updating PostgreSQL Writer in Keboola.")

	client := meta.(*KBCClient)

	updateWriterForm := url.Values{}
	updateWriterForm.Add("name", d.Get("name").(string))
	updateWriterForm.Add("description", d.Get("description").(string))

	updateWriterBuffer := bytes.NewBufferString(updateWriterForm.Encode())

	updateWriterResponse, err := client.PutToStorage(fmt.Sprintf("storage/components/keboola.wr-db-pgsql/configs/%s", d.Id()), updateWriterBuffer)

	if hasErrors(err, updateWriterResponse) {
		return extractError(err, updateWriterResponse)
	}

	params := d.Get("dbParameters").(map[string]interface{})

	if len(params) > 0 {
		err = setPostgreSQLCredentials(d.Id(), params, client)

		if err != nil {
			return err
		}
	}

	return resourceKeboolaPostgreSQLWriterRead(d, meta)
}

func resourceKeboolaPostgreSQLWriterDelete(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[INFO] Deleting PostgreSQL Writer in Keboola: %s", d.Id())

	client := meta.(*KBCClient)
	destroyResponse, err := client.DeleteFromStorage(fmt.Sprintf("storage/components/keboola.wr-db-pgsql/configs/%s", d.Id()))

	if hasErrors(err, destroyResponse) {
		return extractError(err, destroyResponse)
	}

	d.SetId("")

	return nil
}
