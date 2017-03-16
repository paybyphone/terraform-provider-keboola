package keboola

import (
	"fmt"
	"testing"

	"github.com/hashicorp/terraform/helper/resource"
	"github.com/hashicorp/terraform/terraform"
)

func TestAccGoodDataUserManagement_Basic(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:  func() { testAccPreCheck(t) },
		Providers: testAccProviders,
		CheckDestroy: resource.ComposeTestCheckFunc(
			testAccCheckGoodDataUserManagementDestroy,
		),
		Steps: []resource.TestStep{
			resource.TestStep{
				Config: testUserManagementBasic,
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("keboola_gooddata_user_management.test_config", "name", "test name"),
					resource.TestCheckResourceAttr("keboola_gooddata_user_management.test_config", "description", "test description"),
					resource.TestCheckResourceAttr("keboola_gooddata_user_management.test_config", "writer", "testwriter"),
				),
			},
		},
	})
}

func testAccCheckGoodDataUserManagementDestroy(s *terraform.State) error {
	client := testAccProvider.Meta().(*KBCClient)

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "keboola_gooddata_user_management" {
			continue
		}

		gdUserConfigURI := fmt.Sprintf("storage/components/gd-user-mgmt/configs/%s", rs.Primary.ID)
		fmt.Println(gdUserConfigURI)
		getResp, err := client.GetFromStorage(gdUserConfigURI)

		if err == nil && getResp.StatusCode == 200 {
			return fmt.Errorf("GoodData User Management config still exists")
		}
	}

	return nil
}

const testUserManagementBasic = `
	resource "keboola_gooddata_user_management" "test_config" {
		name = "test name"
		description = "test description"
		writer = "testwriter"
	}`
