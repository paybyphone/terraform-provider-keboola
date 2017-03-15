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
	log.Print("[INFO] Creating Orchestration in Keboola.")

	orchConf := Orchestration{
		Name:         d.Get("name").(string),
		ScheduleCRON: d.Get("scheduleCron").(string),
	}

	notifications := mapNotifications(d, meta)
	orchConf.Notifications = notifications

	orchJSON, err := json.Marshal(orchConf)

	if err != nil {
		return err
	}

	client := meta.(*KbcClient)

	orchBuffer := bytes.NewBuffer(orchJSON)
	postResp, err := client.PostToSyrup("orchestrator/orchestrations", orchBuffer)

	if hasErrors(err, postResp) {
		return extractError(err, postResp)
	}

	var createRes CreateResourceResult

	decoder := json.NewDecoder(postResp.Body)
	err = decoder.Decode(&createRes)

	if err != nil {
		return err
	}

	d.SetId(string(createRes.ID))

	return resourceKeboolaOrchestrationRead(d, meta)
}

func getKeboolaOrchestration(d *schema.ResourceData, meta interface{}) (*Orchestration, error) {
	client := meta.(*KbcClient)
	getResp, err := client.GetFromSyrup(fmt.Sprintf("orchestrator/orchestrations/%s", d.Id()))

	if hasErrors(err, getResp) {
		if getResp.StatusCode == 404 {
			return nil, nil
		}

		return nil, extractError(err, getResp)
	}

	var orchestration Orchestration

	decoder := json.NewDecoder(getResp.Body)
	err = decoder.Decode(&orchestration)

	if err != nil {
		return nil, err
	}

	return &orchestration, nil
}

func resourceKeboolaOrchestrationRead(d *schema.ResourceData, meta interface{}) error {
	log.Print("[INFO] Reading Orchestrations from Keboola.")

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
	log.Print("[INFO] Updating Orchestration in Keboola.")

	orchConf := Orchestration{
		Name:         d.Get("name").(string),
		ScheduleCRON: d.Get("scheduleCron").(string),
	}

	notifications := mapNotifications(d, meta)
	orchConf.Notifications = notifications

	orchJSON, err := json.Marshal(orchConf)

	if err != nil {
		return err
	}

	client := meta.(*KbcClient)

	orchBuffer := bytes.NewBuffer(orchJSON)
	putResp, err := client.PutToSyrup(fmt.Sprintf("orchestrator/orchestrations/%s", d.Id()), orchBuffer)

	if hasErrors(err, putResp) {
		return extractError(err, putResp)
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

	client := meta.(*KbcClient)
	delOrchestrationResp, err := client.DeleteFromSyrup(fmt.Sprintf("orchestrator/orchestrations/%s", d.Id()))

	if hasErrors(err, delOrchestrationResp) {
		return extractError(err, delOrchestrationResp)
	}

	delTokenResp, err := client.DeleteFromStorage(fmt.Sprintf("storage/tokens/%s", tokenID))

	if hasErrors(err, delTokenResp) {
		return extractError(err, delTokenResp)
	}

	d.SetId("")

	return nil
}
