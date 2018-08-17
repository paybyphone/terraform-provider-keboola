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

	jsonData := bytes.NewBuffer(orchestrationJSON)
	resp, err := client.PostToSyrup("orchestrator/orchestrations", jsonData)

	if hasErrors(err, resp) {
		return extractError(err, resp)
	}

	var orchestration CreateResourceResult

	decoder := json.NewDecoder(resp.Body)
	err = decoder.Decode(&orchestration)

	if err != nil {
		return err
	}

	d.SetId(string(orchestration.ID))

	return resourceKeboolaOrchestrationRead(d, meta)
}

func resourceKeboolaOrchestrationRead(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Reading Orchestrations from Keboola.")

	if d.Id() == "" {
		return nil
	}

	client := meta.(*KBCClient)
	resp, err := client.GetFromSyrup(fmt.Sprintf("orchestrator/orchestrations/%s", d.Id()))

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

	var orchestration Orchestration

	decoder := json.NewDecoder(resp.Body)
	err = decoder.Decode(&orchestration)

	if err != nil {
		d.SetId("")
		return err
	}

	var notifications []map[string]interface{}

	for _, notification := range orchestration.Notifications {

		mappedNotification := map[string]interface{}{
			"email":      notification.Email,
			"channel":    notification.Channel,
			"parameters": notification.Parameters,
		}

		notifications = append(notifications, mappedNotification)
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

	jsonData := bytes.NewBuffer(orchestrationJSON)
	resp, err := client.PutToSyrup(fmt.Sprintf("orchestrator/orchestrations/%s", d.Id()), jsonData)

	if hasErrors(err, resp) {
		return extractError(err, resp)
	}

	return resourceKeboolaOrchestrationRead(d, meta)
}

func resourceKeboolaOrchestrationDelete(d *schema.ResourceData, meta interface{}) error {
	log.Printf("[INFO] Deleting Orchestration in Keboola: %s", d.Id())

	client := meta.(*KBCClient)
	resp, err := client.GetFromSyrup(fmt.Sprintf("orchestrator/orchestrations/%s", d.Id()))

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

	var orchestration Orchestration

	decoder := json.NewDecoder(resp.Body)
	err = decoder.Decode(&orchestration)

	if err != nil {
		return err
	}

	tokenID := orchestration.Token.ID

	if err != nil {
		return err
	}

	resp, err = client.DeleteFromSyrup(fmt.Sprintf("orchestrator/orchestrations/%s", d.Id()))

	if hasErrors(err, resp) {
		return extractError(err, resp)
	}

	resp, err = client.DeleteFromStorage(fmt.Sprintf("storage/tokens/%s", tokenID))

	if hasErrors(err, resp) {
		return extractError(err, resp)
	}

	d.SetId("")

	return nil
}
