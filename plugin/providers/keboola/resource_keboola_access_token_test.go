package keboola

import (
	"fmt"
	"testing"

	"github.com/hashicorp/terraform/helper/resource"
	"github.com/hashicorp/terraform/terraform"
)

func TestAccAccessToken_Basic(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:  func() { testAccPreCheck(t) },
		Providers: testAccProviders,
		CheckDestroy: resource.ComposeTestCheckFunc(
			testAccCheckAccessTokenDestroy,
		),
		Steps: []resource.TestStep{
			resource.TestStep{
				Config: testAccessTokenBasic,
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("keboola_access_token.test_token", "description", "test description"),
					resource.TestCheckResourceAttr("keboola_access_token.test_token", "canManageBuckets", "true"),
					resource.TestCheckResourceAttr("keboola_access_token.test_token", "canManageTokens", "false"),
					resource.TestCheckResourceAttr("keboola_access_token.test_token", "canReadAllFileUploads", "false"),
				),
			},
		},
	})
}

func testAccCheckAccessTokenDestroy(s *terraform.State) error {
	client := testAccProvider.Meta().(*KbcClient)

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "keboola_access_token" {
			continue
		}

		tokenURI := fmt.Sprintf("storage/tokens/%s", rs.Primary.ID)
		fmt.Println(tokenURI)
		getResp, err := client.GetFromStorage(tokenURI)

		if err == nil && getResp.StatusCode == 200 {
			return fmt.Errorf("Access Token still exists")
		}
	}

	return nil
}

const testAccessTokenBasic = `
	resource "keboola_access_token" "test_token" {
		description = "test description"
		canManageBuckets = true
		canManageTokens = false
		canReadAllFileUploads = false
		expiresIn = 10800
    lifecycle {
        ignore_changes = [ "expiresIn" ]
    }
	}`
