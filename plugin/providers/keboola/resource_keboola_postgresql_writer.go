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

	id, err := createPostgreSQLWriterConfiguration(d.Get("name").(string), d.Get("description").(string), client)

	if err != nil {
		return err
	}

	dbParams := d.Get("postgresql_db_parameters").(map[string]interface{})

	if dbParams != nil {
		err = createPostgreSQLCredentialsConfiguration(dbParams, id, client)

		if err != nil {
			return err
		}
	}

	d.SetId(id)

	return resourceKeboolaPostgreSQLWriterRead(d, meta)
}

func createPostgreSQLWriterConfiguration(name string, description string, client *KBCClient) (createdPostgreSQLID string, err error) {
	form := url.Values{}
	form.Add("name", name)
	form.Add("description", description)

	data := buffer.FromForm(form)

	resp, err := client.PostToStorage("storage/components/keboola.wr-db-pgsql/configs", data)

	if hasErrors(err, resp) {
		return "", extractError(err, resp)
	}

	var res CreateResourceResult

	decoder := json.NewDecoder(resp.Body)
	err = decoder.Decode(&res)

	if err != nil {
		return "", err
	}

	return string(res.ID), nil
}

func mapPostgreSQLCredentialsToConfiguration(source map[string]interface{}) PostgreSQLWriterDatabaseParameters {
	dbParams := PostgreSQLWriterDatabaseParameters{}

	if val, ok := source["hostname"]; ok { dbParams.HostName = val.(string) }
	if val, ok := source["port"]; ok { dbParams.Port = val.(string) }
	if val, ok := source["database"]; ok { dbParams.Database = val.(string) }
	if val, ok := source["schema"]; ok { dbParams.Schema = val.(string) }
	if val, ok := source["username"]; ok { dbParams.Username = val.(string) }
	if val, ok := source["hashed_password"]; ok { dbParams.EncryptedPassword = val.(string) }

	dbParams.Driver = "pgsql"

	return dbParams
}

func createPostgreSQLCredentialsConfiguration(params map[string]interface{}, postgresID string, client *KBCClient) error {
	config := PostgreSQLWriterConfiguration{}

	config.Parameters.Database = mapPostgreSQLCredentialsToConfiguration(params)

	jsonData, err := json.Marshal(config)

	if err != nil {
		return err
	}

	form := url.Values{}
	form.Add("configuration", string(jsonData))
	form.Add("changeDescription", "Created database credentials")

	data := buffer.FromForm(form)
	resp, err := client.PutToStorage(fmt.Sprintf("storage/components/keboola.wr-db-pgsql/configs/%s", postgresID), data)

	if hasErrors(err, resp) {
		return extractError(err, resp)
	}

	return nil
}

func resourceKeboolaPostgreSQLWriterRead(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Reading PostgreSQL Writers from Keboola.")

	client := meta.(*KBCClient)
	resp, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.wr-db-pgsql/configs/%s", d.Id()))

	if d.Id() == "" {
		return nil
	}

	if hasErrors(err, resp) {
		if err != nil {
			return err
		}

		if resp.StatusCode == 404 {
			d.SetId("")
			return nil
		}

		return extractError(err, resp)
	}

	var writer PostgreSQLWriter

	decoder := json.NewDecoder(resp.Body)
	err = decoder.Decode(&writer)

	if err != nil {
		return err
	}

	d.Set("id", writer.ID)
	d.Set("name", writer.Name)
	d.Set("description", writer.Description)

	mapped := mapAPIDatabaseParameters(writer.Configuration.Parameters.Database)

	if len(mapped) > 0 {
		d.Set("postgresql_db_parameters", mapped)
	}

	return nil
}

func mapAPIDatabaseParameters(dbParams PostgreSQLWriterDatabaseParameters) map[string]interface{} {
	mappedDBParams := make(map[string]interface{})

	if len(dbParams.HostName) > 0 {
		mappedDBParams["hostname"] = dbParams.HostName
	}
	
	if len(dbParams.HostName) > 0 {
		mappedDBParams["port"] = dbParams.Port
	}
	
	if len(dbParams.HostName) > 0 {
		mappedDBParams["database"] = dbParams.Database
	}

	if len(dbParams.HostName) > 0 {
		mappedDBParams["schema"] = dbParams.Schema
	}
	
	if len(dbParams.HostName) > 0 {
		mappedDBParams["username"] = dbParams.Username
	}

	if len(dbParams.HostName) > 0 {
		mappedDBParams["hashed_password"] = dbParams.EncryptedPassword
	}

	return mappedDBParams
}

func resourceKeboolaPostgreSQLWriterUpdate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Updating PostgreSQL Writer in Keboola.")

	client := meta.(*KBCClient)

	resp, err := client.GetFromStorage(fmt.Sprintf("storage/components/keboola.wr-db-pgsql/configs/%s", d.Id()))

	if hasErrors(err, resp) {
		return extractError(err, resp)
	}

	var writer PostgreSQLWriter

	decoder := json.NewDecoder(resp.Body)
	err = decoder.Decode(&writer)

	if err != nil {
		return err
	}

	dbParams := d.Get("postgresql_db_parameters").(map[string]interface{})
	writer.Configuration.Parameters.Database = mapPostgreSQLCredentialsToConfiguration(dbParams)
	jsonData, err := json.Marshal(writer.Configuration)

	if err != nil {
		return err
	}

	form := url.Values{}
	form.Add("name", d.Get("name").(string))
	form.Add("description", d.Get("description").(string))
	form.Add("configuration", string(jsonData))
	form.Add("changeDescription", "Updated PostgreSQL Writer configuration via Terraform")

	data := buffer.FromForm(form)

	resp, err = client.PutToStorage(fmt.Sprintf("storage/components/keboola.wr-db-pgsql/configs/%s", d.Id()), data)

	if hasErrors(err, resp) {
		return extractError(err, resp)
	}

	err = createPostgreSQLCredentialsConfiguration(dbParams, d.Id(), client)

	return resourceKeboolaPostgreSQLWriterRead(d, meta)
}

func resourceKeboolaPostgreSQLWriterDelete(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[INFO] Deleting PostgreSQL Writer in Keboola: %s", d.Id())

	client := meta.(*KBCClient)
	resp, err := client.DeleteFromStorage(fmt.Sprintf("storage/components/keboola.wr-db-pgsql/configs/%s", d.Id()))

	if hasErrors(err, resp) {
		return extractError(err, resp)
	}

	d.SetId("")

	return nil
}
