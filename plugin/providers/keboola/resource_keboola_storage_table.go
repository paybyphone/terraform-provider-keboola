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

func resourceKeboolaStorageTableCreate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Creating Storage Table in Keboola.")

	client := meta.(*KBCClient)
	columns := AsStringArray(d.Get("columns").(*schema.Set).List())

	uploadFileBuffer := &bytes.Buffer{}
	uploadFileRequestWriter := multipart.NewWriter(uploadFileBuffer)
	uploadFileRequestWriter.SetBoundary("----terraform-provider-keboola----")
	uploadFileRequestWriter.WriteField("name", "from-text-input.csv")
	uploadFileRequestWriter.WriteField("data", strings.Join(columns, ","))
	uploadFileRequestWriter.Close()

	uploadResponse, err := client.PostToFileImport("upload-file", uploadFileBuffer)

	if hasErrors(err, uploadResponse) {
		return extractError(err, uploadResponse)
	}

	var uploadResult UploadFileResult

	uploadResponseDecoder := json.NewDecoder(uploadResponse.Body)
	err = uploadResponseDecoder.Decode(&uploadResult)

	if err != nil {
		return err
	}

	fileID := uploadResult.ID

	loadTableForm := url.Values{}
	loadTableForm.Add("name", d.Get("name").(string))
	loadTableForm.Add("primaryKey", strings.Join(AsStringArray(d.Get("primary_key").([]interface{})), ","))
	loadTableForm.Add("dataFileId", strconv.Itoa(fileID))

	if d.Get("delimiter") != "" {
		loadTableForm.Add("delimiter", d.Get("delimiter").(string))
	} else {
		loadTableForm.Add("delimiter", ",")
	}

	if d.Get("enclosure") != "" {
		loadTableForm.Add("enclosure", d.Get("enclosure").(string))
	} else {
		loadTableForm.Add("enclosure", "\"")
	}

	loadTableBuffer := buffer.FromForm(loadTableForm)

	bucketID := d.Get("bucket_id").(string)

	loadTableResponse, err := client.PostToStorage(fmt.Sprintf("storage/buckets/%s/tables-async", bucketID), loadTableBuffer)

	if hasErrors(err, loadTableResponse) {
		return extractError(err, loadTableResponse)
	}

	var loadTableResult UploadFileResult

	loadTableDecoder := json.NewDecoder(loadTableResponse.Body)
	err = loadTableDecoder.Decode(&loadTableResult)

	if err != nil {
		return err
	}

	tableLoadStatus := "waiting"

	var tableLoadStatusResult StorageJobStatus

	for tableLoadStatus != "success" && tableLoadStatus != "error" {
		jobStatusResponse, err := client.GetFromStorage(fmt.Sprintf("storage/jobs/%v", loadTableResult.ID))

		if hasErrors(err, jobStatusResponse) {
			return extractError(err, jobStatusResponse)
		}

		jobStatusDecoder := json.NewDecoder(jobStatusResponse.Body)
		err = jobStatusDecoder.Decode(&tableLoadStatusResult)

		if err != nil {
			return err
		}

		time.Sleep(250 * time.Millisecond)
		tableLoadStatus = tableLoadStatusResult.Status
	}

	indexedOnlyColumns := except(AsStringArray(d.Get("indexed_columns").([]interface{})), AsStringArray(d.Get("primary_key").([]interface{})))

	for _, indexedColumn := range indexedOnlyColumns {
		addIndexedColumnResp, err := client.PostToStorage(fmt.Sprintf("storage/tables/%s/indexed-columns?name=%s", tableLoadStatusResult.Results.ID, indexedColumn), buffer.Empty())

		if hasErrors(err, addIndexedColumnResp) {
			return extractError(err, addIndexedColumnResp)
		}
	}

	d.SetId(tableLoadStatusResult.Results.ID)

	return resourceKeboolaStorageTableRead(d, meta)
}

func except(first []string, second []string) []string {
	var result []string

	for _, elementInFirst := range first {
		elementFound := false
		for _, elementInSecond := range second {
			if elementInFirst == elementInSecond {
				elementFound = true
				break
			}
		}

		if !elementFound {
			result = append(result, elementInFirst)
		}
	}

	return result
}

func resourceKeboolaStorageTableRead(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Reading Storage Table from Keboola.")

	if d.Id() == "" {
		return nil
	}

	client := meta.(*KBCClient)
	getResponse, err := client.GetFromStorage(fmt.Sprintf("storage/tables/%s", d.Id()))

	if hasErrors(err, getResponse) {
		if getResponse.StatusCode == 404 {
			d.SetId("")
			return nil
		}

		return extractError(err, getResponse)
	}

	var storageTable StorageTable

	decoder := json.NewDecoder(getResponse.Body)
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
	destroyResponse, err := client.DeleteFromStorage(fmt.Sprintf("storage/tables/%s", d.Id()))

	if hasErrors(err, destroyResponse) {
		return extractError(err, destroyResponse)
	}

	d.SetId("")

	return nil
}
