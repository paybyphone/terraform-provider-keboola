package keboola

import (
	"encoding/json"
	"fmt"
	"log"
	"net/url"

	"github.com/hashicorp/terraform/helper/schema"
	"github.com/plmwong/terraform-provider-keboola/plugin/providers/keboola/buffer"
)

func resourceKeboolaPostgreSQLWriterCreate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Creating PostgreSQL Writer in Keboola.")

	client := meta.(*KBCClient)

	createdPostgreSQLID, err := createPostgreSQLWriterConfiguration(d.Get("name").(string), d.Get("description").(string), client)

	if err != nil {
		return err
	}

	postgresqlDatabaseCredentials := d.Get("postgresql_db_parameters").(map[string]interface{})
	err = createPostgreSQLCredentialsConfiguration(postgresqlDatabaseCredentials, createdPostgreSQLID, client)

	d.SetId(createdPostgreSQLID)

	return resourceKeboolaPostgreSQLWriterRead(d, meta)
}

func createPostgreSQLWriterConfiguration(name string, description string, client *KBCClient) (createdPostgreSQLID string, err error) {
	createWriterForm := url.Values{}
	createWriterForm.Add("name", name)
	createWriterForm.Add("description", description)

	createWriterBuffer := buffer.FromForm(createWriterForm)

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

func mapPostgreSQLCredentialsToConfiguration(source map[string]interface{}) PostgreSQLWriterDatabaseParameters {
	databaseParameters := PostgreSQLWriterDatabaseParameters{}

	if val, ok := source["hostname"]; ok { databaseParameters.HostName = val.(string) }
	if val, ok := source["port"]; ok { databaseParameters.Port = val.(string) }
	if val, ok := source["database"]; ok { databaseParameters.Database = val.(string) }
	if val, ok := source["schema"]; ok { databaseParameters.Schema = val.(string) }
	if val, ok := source["username"]; ok { databaseParameters.Username = val.(string) }
	if val, ok := source["hashed_password"]; ok { databaseParameters.EncryptedPassword = val.(string) }

	databaseParameters.Driver = "pgsql"

	return databaseParameters
}

func createPostgreSQLCredentialsConfiguration(params map[string]interface{}, createdPostgreSQLID string, client *KBCClient) error {
	postgresqlCredentials := PostgreSQLWriterConfiguration{}

	postgresqlCredentials.Parameters.Database = mapPostgreSQLCredentialsToConfiguration(params)

	postgresqlCredentialsJSON, err := json.Marshal(postgresqlCredentials)

	if err != nil {
		return err
	}

	updateCredentialsForm := url.Values{}
	updateCredentialsForm.Add("configuration", string(postgresqlCredentialsJSON))
	updateCredentialsForm.Add("changeDescription", "Created database credentials")

	updateCredentialsBuffer := buffer.FromForm(updateCredentialsForm)
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
			d.SetId("")
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

	getWriterResponse, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.wr-db-pgsql/configs/%s", d.Id()))

	if hasErrors(err, getWriterResponse) {
		return extractError(err, getWriterResponse)
	}

	var postgreSQLWriter PostgreSQLWriter

	decoder := json.NewDecoder(getWriterResponse.Body)
	err = decoder.Decode(&postgreSQLWriter)

	if err != nil {
		return err
	}

	postgreSQLCredentials := d.Get("postgresql_db_parameters").(map[string]interface{})
	postgreSQLWriter.Configuration.Parameters.Database = mapPostgreSQLCredentialsToConfiguration(postgreSQLCredentials)
	postgreSQLConfigJSON, err := json.Marshal(postgreSQLWriter.Configuration)

	if err != nil {
		return err
	}

	updateCredentialsForm := url.Values{}
	updateCredentialsForm.Add("name", d.Get("name").(string))
	updateCredentialsForm.Add("description", d.Get("description").(string))
	updateCredentialsForm.Add("configuration", string(postgreSQLConfigJSON))
	updateCredentialsForm.Add("changeDescription", "Updated PostgreSQL Writer configuration via Terraform")

	updateWriterBuffer := buffer.FromForm(updateCredentialsForm)

	updateWriterResponse, err := client.PutToStorage(fmt.Sprintf("storage/components/keboola.wr-db-pgsql/configs/%s", d.Id()), updateWriterBuffer)

	if hasErrors(err, updateWriterResponse) {
		return extractError(err, updateWriterResponse)
	}

	postgresqlDatabaseCredentials := d.Get("postgresql_db_parameters").(map[string]interface{})
	err = createPostgreSQLCredentialsConfiguration(postgresqlDatabaseCredentials, d.Id(), client)

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
