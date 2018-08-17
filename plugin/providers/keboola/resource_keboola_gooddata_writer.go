package keboola

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"strings"
	"time"

	"github.com/hashicorp/terraform/helper/schema"
	"github.com/plmwong/terraform-provider-keboola/plugin/providers/keboola/buffer"
	"net/http"
)

func resourceKeboolaGoodDataWriterCreate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Creating GoodData Writer in Keboola.")

	client := meta.(*KBCClient)

	err := provisionGoodDataProject(d.Get("writer_id").(string), d.Get("description").(string), d.Get("auth_token").(string), client)

	if err != nil {
		return err
	}

	writerID, err := createGoodDataWriterConfiguration(d.Get("writer_id").(string), d.Get("name").(string), d.Get("description").(string), client)

	if err != nil {
		return err
	}

	d.SetId(writerID)

	return resourceKeboolaGoodDataWriterRead(d, meta)
}

func provisionGoodDataProject(writerID string, description string, authToken string, client *KBCClient) error {
	createProject := CreateGoodDataProject{
		WriterID:    writerID,
		Description: description,
		AuthToken:   authToken,
	}

	projectJSON, err := json.Marshal(createProject)
	if err != nil {
		return err
	}

	jsonData := bytes.NewBuffer(projectJSON)
	resp, err := client.PostToSyrup("gooddata-writer/v2", jsonData)

	if hasErrors(err, resp) {
		return extractError(err, resp)
	}

	return waitForJobToFinish(resp, client)
}

func waitForJobToFinish(job *http.Response, client *KBCClient) error {
	status := "waiting"

	var jobStatus StorageJobStatus

	decoder := json.NewDecoder(job.Body)
	err := decoder.Decode(&jobStatus)

	if err != nil {
		return err
	}

	jobURL, err := url.Parse(jobStatus.URL)

	if err != nil {
		return err
	}

	for status != "success" && status != "error" {
		resp, err := client.GetFromSyrup(strings.TrimLeft(jobURL.Path, "/"))

		if hasErrors(err, resp) {
			return extractError(err, resp)
		}

		decoder := json.NewDecoder(resp.Body)
		err = decoder.Decode(&jobStatus)

		if err != nil {
			return err
		}

		time.Sleep(250 * time.Millisecond)
		status = jobStatus.Status
	}

	return nil
}

func createGoodDataWriterConfiguration(writerID string, name string, description string, client *KBCClient) (createdID string, err error) {
	form := url.Values{}
	form.Add("name", name)
	form.Add("description", description)

	formData := buffer.FromForm(form)

	resp, err := client.PutToStorage(fmt.Sprintf("storage/components/gooddata-writer/configs/%s", writerID), formData)

	if err != nil {
		return "", err
	}

	if hasErrors(err, resp) {
		return "", extractError(err, resp)
	}

	var writer CreateResourceResult

	createDecoder := json.NewDecoder(resp.Body)
	err = createDecoder.Decode(&writer)

	if err != nil {
		return "", err
	}

	return string(writer.ID), nil
}

func resourceKeboolaGoodDataWriterRead(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Reading GoodData Writers from Keboola.")

	if d.Id() == "" {
		return nil
	}

	client := meta.(*KBCClient)
	resp, err := client.GetFromStorage(fmt.Sprintf("storage/components/gooddata-writer/configs/%s", d.Id()))

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

	var writer GoodDataWriter

	decoder := json.NewDecoder(resp.Body)
	err = decoder.Decode(&writer)

	if err != nil {
		return err
	}

	d.Set("id", writer.ID)
	d.Set("name", writer.Name)
	d.Set("description", writer.Description)

	return nil
}

func resourceKeboolaGoodDataWriterUpdate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Updating GoodData Writer in Keboola.")

	form := url.Values{}
	form.Add("name", d.Get("name").(string))
	form.Add("description", d.Get("description").(string))

	client := meta.(*KBCClient)

	formData := buffer.FromForm(form)
	resp, err := client.PutToStorage(fmt.Sprintf("storage/components/gooddata-writer/configs/%s", d.Id()), formData)

	if err != nil {
		return err
	}

	if hasErrors(err, resp) {
		return extractError(err, resp)
	}

	return resourceKeboolaGoodDataWriterRead(d, meta)
}

func resourceKeboolaGoodDataWriterDelete(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[INFO] Deleting GoodData Writer in Keboola: %s", d.Id())

	client := meta.(*KBCClient)
	resp, err := client.DeleteFromSyrup(fmt.Sprintf("gooddata-writer/configs/%s", d.Id()))

	if hasErrors(err, resp) {
		return extractError(err, resp)
	}

	resp, err = client.DeleteFromStorage(fmt.Sprintf("storage/components/gooddata-writer/configs/%s", d.Id()))

	if hasErrors(err, resp) {
		return extractError(err, resp)
	}

	d.SetId("")

	return nil
}
