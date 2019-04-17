package keboola

import (
	"os"
	"testing"

	"github.com/hashicorp/terraform/helper/schema"
	"github.com/hashicorp/terraform/terraform"
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

	t.Log("hello!")
	t.Logf("%#v", provider)
}

func TestProvider_impl(t *testing.T) {
	var _ terraform.ResourceProvider = Provider()
}

func testAccPreCheck(t *testing.T) {
	if v := os.Getenv("STORAGE_API_KEY"); v == "" {
		t.Fatal("STORAGE_API_KEY must be set for acceptance tests")
	}
}

const testApiKey = `
  provider "keboola" {
	api_key = "abcdefg"
  }`
