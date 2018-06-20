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
			{
				Config: testAccessTokenBasic,
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("keboola_access_token.test_token", "description", "test description"),
					resource.TestCheckResourceAttr("keboola_access_token.test_token", "can_manage_buckets", "true"),
					resource.TestCheckResourceAttr("keboola_access_token.test_token", "can_manage_tokens", "false"),
					resource.TestCheckResourceAttr("keboola_access_token.test_token", "can_read_all_file_uploads", "false"),
					resource.TestCheckResourceAttr("keboola_access_token.test_token", "expires_in", "10800"),
				),
			},
		},
	})
}

func testAccCheckAccessTokenDestroy(s *terraform.State) error {
	client := testAccProvider.Meta().(*KBCClient)

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
		can_manage_buckets = true
		can_manage_tokens = false
		can_read_all_file_uploads = false
		expires_in = 10800
    lifecycle {
        ignore_changes = [ "expires_in" ]
    }
	}`
