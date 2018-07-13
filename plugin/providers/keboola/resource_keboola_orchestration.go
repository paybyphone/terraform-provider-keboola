package keboola

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"

	"github.com/hashicorp/terraform/helper/schema"
)

func mapNotifications(d *schema.ResourceData) []OrchestrationNotification {
	notifications := d.Get("notification").([]interface{})
	mappedNotifications := make([]OrchestrationNotification, 0, len(notifications))

	if notifications != nil {
		for _, notificationConfig := range notifications {
			config := notificationConfig.(map[string]interface{})

			mappedNotification := OrchestrationNotification{
				Email:      config["email"].(string),
				Channel:    config["channel"].(string),
				Parameters: config["parameters"].(map[string]interface{}),
			}

			mappedNotifications = append(mappedNotifications, mappedNotification)
		}
	}

	return mappedNotifications
}

func resourceKeboolaOrchestrationCreate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Creating Orchestration in Keboola.")

	orchestrationConfig := Orchestration{
		Name:         d.Get("name").(string),
		ScheduleCRON: d.Get("schedule_cron").(string),
	}

	notifications := mapNotifications(d)
	orchestrationConfig.Notifications = notifications

	orchestrationJSON, err := json.Marshal(orchestrationConfig)

	if err != nil {
		return err
	}

	client := meta.(*KBCClient)

	orchestrationBuffer := bytes.NewBuffer(orchestrationJSON)
	createResponse, err := client.PostToSyrup("orchestrator/orchestrations", orchestrationBuffer)

	if hasErrors(err, createResponse) {
		return extractError(err, createResponse)
	}

	var createResult CreateResourceResult

	decoder := json.NewDecoder(createResponse.Body)
	err = decoder.Decode(&createResult)

	if err != nil {
		return err
	}

	d.SetId(string(createResult.ID))

	return resourceKeboolaOrchestrationRead(d, meta)
}

func resourceKeboolaOrchestrationRead(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Reading Orchestrations from Keboola.")

	if d.Id() == "" {
		return nil
	}

	client := meta.(*KBCClient)
	getResponse, err := client.GetFromSyrup(fmt.Sprintf("orchestrator/orchestrations/%s", d.Id()))

	if hasErrors(err, getResponse) {
		if err != nil {
			return err
		}

		if getResponse.StatusCode == 404 {
			d.SetId("")
			return nil
		}

		return extractError(err, getResponse)
	}

	var orchestration Orchestration

	decoder := json.NewDecoder(getResponse.Body)
	err = decoder.Decode(&orchestration)

	if err != nil {
		return err
	}

	if err != nil {
		d.SetId("")
		return err
	}

	var notifications []map[string]interface{}

	for _, notification := range orchestration.Notifications {

		notificationDetails := map[string]interface{}{
			"email":      notification.Email,
			"channel":    notification.Channel,
			"parameters": notification.Parameters,
		}

		notifications = append(notifications, notificationDetails)
	}

	d.Set("id", orchestration.ID)
	d.Set("name", orchestration.Name)
	d.Set("schedule_cron", orchestration.ScheduleCRON)
	d.Set("notification", notifications)

	return nil
}

func resourceKeboolaOrchestrationUpdate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Updating Orchestration in Keboola.")

	orchestrationConfig := Orchestration{
		Name:         d.Get("name").(string),
		ScheduleCRON: d.Get("schedule_cron").(string),
	}

	notifications := mapNotifications(d)
	orchestrationConfig.Notifications = notifications

	orchestrationJSON, err := json.Marshal(orchestrationConfig)

	if err != nil {
		return err
	}

	client := meta.(*KBCClient)

	orchestrationBuffer := bytes.NewBuffer(orchestrationJSON)
	updateResponse, err := client.PutToSyrup(fmt.Sprintf("orchestrator/orchestrations/%s", d.Id()), orchestrationBuffer)

	if hasErrors(err, updateResponse) {
		return extractError(err, updateResponse)
	}

	return resourceKeboolaOrchestrationRead(d, meta)
}

func resourceKeboolaOrchestrationDelete(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[INFO] Deleting Orchestration in Keboola: %s", d.Id())

	client := meta.(*KBCClient)
	getResponse, err := client.GetFromSyrup(fmt.Sprintf("orchestrator/orchestrations/%s", d.Id()))

	if hasErrors(err, getResponse) {
		if err != nil {
			return err
		}

		if getResponse.StatusCode == 404 {
			d.SetId("")
			return nil
		}

		return extractError(err, getResponse)
	}

	var orchestration Orchestration

	decoder := json.NewDecoder(getResponse.Body)
	err = decoder.Decode(&orchestration)

	if err != nil {
		return err
	}

	tokenID := orchestration.Token.ID

	if err != nil {
		return err
	}

	destroyOrchestrationResponse, err := client.DeleteFromSyrup(fmt.Sprintf("orchestrator/orchestrations/%s", d.Id()))

	if hasErrors(err, destroyOrchestrationResponse) {
		return extractError(err, destroyOrchestrationResponse)
	}

	destroyTokenResponse, err := client.DeleteFromStorage(fmt.Sprintf("storage/tokens/%s", tokenID))

	if hasErrors(err, destroyTokenResponse) {
		return extractError(err, destroyTokenResponse)
	}

	d.SetId("")

	return nil
}
