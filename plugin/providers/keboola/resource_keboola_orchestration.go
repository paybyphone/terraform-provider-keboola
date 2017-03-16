package keboola

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"

	"github.com/hashicorp/terraform/helper/schema"
)

type OrchestrationNotification struct {
	Email      string                 `json:"email"`
	Channel    string                 `json:"channel"`
	Parameters map[string]interface{} `json:"parameters"`
}

type OrchestrationToken struct {
	ID          string `json:"id"`
	Description string `json:"description"`
}

type Orchestration struct {
	ID            json.Number                 `json:"id,omitempty"`
	Name          string                      `json:"name"`
	ScheduleCRON  string                      `json:"crontabRecord"`
	Token         OrchestrationToken          `json:"token,omitempty"`
	Notifications []OrchestrationNotification `json:"notifications"`
}

func resourceKeboolaOrchestration() *schema.Resource {
	return &schema.Resource{
		Create: resourceKeboolaOrchestrationCreate,
		Read:   resourceKeboolaOrchestrationRead,
		Update: resourceKeboolaOrchestrationUpdate,
		Delete: resourceKeboolaOrchestrationDelete,

		Schema: map[string]*schema.Schema{
			"name": &schema.Schema{
				Type:     schema.TypeString,
				Required: true,
			},
			"scheduleCron": &schema.Schema{
				Type:     schema.TypeString,
				Optional: true,
			},
			"notification": &schema.Schema{
				Type:     schema.TypeList,
				Optional: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"email": &schema.Schema{
							Type:     schema.TypeString,
							Required: true,
						},
						"channel": &schema.Schema{
							Type:         schema.TypeString,
							ValidateFunc: validateOrchestrationNotificationChannel,
							Required:     true,
						},
						"parameters": &schema.Schema{
							Type:     schema.TypeMap,
							Optional: true,
						},
					},
				},
			},
		},
	}
}

func mapNotifications(d *schema.ResourceData, meta interface{}) []OrchestrationNotification {
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
		ScheduleCRON: d.Get("scheduleCron").(string),
	}

	notifications := mapNotifications(d, meta)
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

func getKeboolaOrchestration(d *schema.ResourceData, meta interface{}) (*Orchestration, error) {
	client := meta.(*KBCClient)
	getResponse, err := client.GetFromSyrup(fmt.Sprintf("orchestrator/orchestrations/%s", d.Id()))

	if hasErrors(err, getResponse) {
		if getResponse.StatusCode == 404 {
			return nil, nil
		}

		return nil, extractError(err, getResponse)
	}

	var orchestration Orchestration

	decoder := json.NewDecoder(getResponse.Body)
	err = decoder.Decode(&orchestration)

	if err != nil {
		return nil, err
	}

	return &orchestration, nil
}

func resourceKeboolaOrchestrationRead(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Reading Orchestrations from Keboola.")

	if d.Id() == "" {
		return nil
	}

	orchestrationPtr, err := getKeboolaOrchestration(d, meta)
	orchestration := *orchestrationPtr

	if err != nil {
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
	d.Set("scheduleCron", orchestration.ScheduleCRON)
	d.Set("notification", notifications)

	return nil
}

func resourceKeboolaOrchestrationUpdate(d *schema.ResourceData, meta interface{}) error {
	log.Println("[INFO] Updating Orchestration in Keboola.")

	orchestrationConfig := Orchestration{
		Name:         d.Get("name").(string),
		ScheduleCRON: d.Get("scheduleCron").(string),
	}

	notifications := mapNotifications(d, meta)
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

	orchestration, err := getKeboolaOrchestration(d, meta)
	tokenID := (*orchestration).Token.ID

	if err != nil {
		return err
	}

	client := meta.(*KBCClient)
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
