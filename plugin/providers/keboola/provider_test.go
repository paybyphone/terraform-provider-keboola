package keboola

import (
	"log"
	"os"
	"testing"

	"github.com/hashicorp/terraform-plugin-sdk/config"
	"github.com/hashicorp/terraform-plugin-sdk/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/terraform"
)

var testAccProviders map[string]terraform.ResourceProvider
var testAccProvider *schema.Provider

func init() {
	testAccProvider = Provider().(*schema.Provider)
	testAccProviders = map[string]terraform.ResourceProvider{
		"keboola": testAccProvider,
	}
}

func TestProvider(t *testing.T) {
	if err := Provider().(*schema.Provider).InternalValidate(); err != nil {
		t.Fatalf("err: %s", err)
	}
}

func TestProvider_ApiKey(t *testing.T) {
	provider := Provider().(*schema.Provider)
	c, _ := config.NewRawConfig(map[string]interface{}{
		"api_key": "abcdefg\n\n\n",
	})

	provider.Configure(terraform.NewResourceConfig(c))

	if provider == nil {
		log.Print("[DEBUG] Unable to read account ID from test provider: empty provider")
	}

	if provider.Meta() == nil {
		log.Print("[DEBUG] Unable to read account ID from test provider: unconfigured provider")
	}

	client, ok := provider.Meta().(*KBCClient)

	if !ok {
		log.Print("[DEBUG] Unable to read account ID from test provider: non-AWS or unconfigured AWS provider")
	}

	if client.APIKey != "abcdefg" {
		t.Fatalf("err: %s", "API key still contains a newline, newlines should be stripped out in the terraform provider")
	}
}

func TestProvider_impl(t *testing.T) {
	var _ terraform.ResourceProvider = Provider()
}

func testAccPreCheck(t *testing.T) {
	if v := os.Getenv("STORAGE_API_KEY"); v == "" {
		t.Fatal("STORAGE_API_KEY must be set for acceptance tests")
	}
}
