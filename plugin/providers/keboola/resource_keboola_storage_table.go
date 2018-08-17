package keboola

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"mime/multipart"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/terraform/helper/schema"
	"github.com/plmwong/terraform-provider-keboola/plugin/providers/keboola/buffer"
)

// TODO: Break this method up in to its component steps
func resourceKeboolaStorageTableCreate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Creating Storage Table in Keboola.")

	client := meta.(*KBCClient)
	columns := AsStringArray(d.Get("columns").(*schema.Set).List())

	multipartData := &bytes.Buffer{}
	multipartWriter := multipart.NewWriter(multipartData)
	multipartWriter.SetBoundary("----terraform-provider-keboola----")
	multipartWriter.WriteField("name", "from-text-input.csv")
	multipartWriter.WriteField("data", strings.Join(columns, ","))
	multipartWriter.Close()

	resp, err := client.PostToFileImport("upload-file", multipartData)

	if hasErrors(err, resp) {
		return extractError(err, resp)
	}

	var file UploadFileResult

	uploadResponseDecoder := json.NewDecoder(resp.Body)
	err = uploadResponseDecoder.Decode(&file)

	if err != nil {
		return err
	}

	fileID := file.ID

	form := url.Values{}
	form.Add("name", d.Get("name").(string))
	form.Add("primaryKey", strings.Join(AsStringArray(d.Get("primary_key").([]interface{})), ","))
	form.Add("dataFileId", strconv.Itoa(fileID))

	if d.Get("delimiter") != "" {
		form.Add("delimiter", d.Get("delimiter").(string))
	} else {
		form.Add("delimiter", ",")
	}

	if d.Get("enclosure") != "" {
		form.Add("enclosure", d.Get("enclosure").(string))
	} else {
		form.Add("enclosure", "\"")
	}

	formData := buffer.FromForm(form)

	bucketID := d.Get("bucket_id").(string)

	resp, err = client.PostToStorage(fmt.Sprintf("storage/buckets/%s/tables-async", bucketID), formData)

	if hasErrors(err, resp) {
		return extractError(err, resp)
	}

	var storageTable UploadFileResult

	loadTableDecoder := json.NewDecoder(resp.Body)
	err = loadTableDecoder.Decode(&storageTable)

	if err != nil {
		return err
	}

	loadStatus := "waiting"

	var table StorageJobStatus

	for loadStatus != "success" && loadStatus != "error" {
		loadStatusResp, err := client.GetFromStorage(fmt.Sprintf("storage/jobs/%v", storageTable.ID))

		if hasErrors(err, loadStatusResp) {
			return extractError(err, loadStatusResp)
		}

		jobStatusDecoder := json.NewDecoder(loadStatusResp.Body)
		err = jobStatusDecoder.Decode(&table)

		if err != nil {
			return err
		}

		time.Sleep(250 * time.Millisecond)
		loadStatus = table.Status
	}

	indexedOnlyColumns := except(AsStringArray(d.Get("indexed_columns").([]interface{})), AsStringArray(d.Get("primary_key").([]interface{})))

	for _, indexedColumn := range indexedOnlyColumns {
		indexColResp, err := client.PostToStorage(fmt.Sprintf("storage/tables/%s/indexed-columns?name=%s", table.Results.ID, indexedColumn), buffer.Empty())

		if hasErrors(err, indexColResp) {
			return extractError(err, indexColResp)
		}
	}

	d.SetId(table.Results.ID)

	return resourceKeboolaStorageTableRead(d, meta)
}

func except(first []string, second []string) []string {
	var res []string

	for _, firstElem := range first {
		found := false
		for _, secondElem := range second {
			if firstElem == secondElem {
				found = true
				break
			}
		}

		if !found {
			res = append(res, firstElem)
		}
	}

	return res
}

func resourceKeboolaStorageTableRead(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Reading Storage Table from Keboola.")

	if d.Id() == "" {
		return nil
	}

	client := meta.(*KBCClient)
	resp, err := client.GetFromStorage(fmt.Sprintf("storage/tables/%s", d.Id()))

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

	var storageTable StorageTable

	decoder := json.NewDecoder(resp.Body)
	err = decoder.Decode(&storageTable)

	if err != nil {
		return err
	}

	d.Set("name", storageTable.Name)
	d.Set("delimiter", storageTable.Delimiter)
	d.Set("enclosure", storageTable.Enclosure)
	d.Set("transactional", storageTable.Transactional)
	d.Set("primary_key", storageTable.PrimaryKey)
	d.Set("indexed_columns", storageTable.IndexedColumns)
	d.Set("columns", storageTable.Columns)

	return nil
}

func resourceKeboolaStorageTableDelete(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[INFO] Deleting Storage Table in Keboola: %s", d.Id())

	client := meta.(*KBCClient)
	resp, err := client.DeleteFromStorage(fmt.Sprintf("storage/tables/%s", d.Id()))

	if hasErrors(err, resp) {
		return extractError(err, resp)
	}

	d.SetId("")

	return nil
}
