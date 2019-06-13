package keboola

import (
	"fmt"
	"testing"

	"github.com/hashicorp/terraform/helper/resource"
	"github.com/hashicorp/terraform/terraform"
)

func TestAccGoodDataUserManagement_v2_Basic(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:  func() { testAccPreCheck(t) },
		Providers: testAccProviders,
		CheckDestroy: resource.ComposeTestCheckFunc(
			testAccCheckGoodDataUserManagementV2Destroy,
		),
		Steps: []resource.TestStep{
			{
				Config: testUserManagementV2Basic,
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("keboola_gooddata_user_management_v2.test_config", "name", "test name"),
					resource.TestCheckResourceAttr("keboola_gooddata_user_management_v2.test_config", "description", "test description"),
					resource.TestCheckResourceAttr("keboola_gooddata_user_management_v2.test_config", "project_id", "123"),
					resource.TestCheckResourceAttr("keboola_gooddata_user_management_v2.test_config", "login", "some-login"),
					resource.TestCheckResourceAttr("keboola_gooddata_user_management_v2.test_config", "hashed_password", "pass"),
					resource.TestCheckResourceAttr("keboola_gooddata_user_management_v2.test_config", "custom_domain", "domain"),
				),
			},
		},
	})
}

func testAccCheckGoodDataUserManagementV2Destroy(s *terraform.State) error {
	client := testAccProvider.Meta().(*KBCClient)

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "keboola_gooddata_user_management_v2" {
			continue
		}

		gdUserConfigURI := fmt.Sprintf("storage/components/kds-team.app-gd-user-management/configs/%s", rs.Primary.ID)
		getResp, err := client.GetFromStorage(gdUserConfigURI)

		if err == nil && getResp.StatusCode == 200 {
			return fmt.Errorf("GoodData User Management config still exists")
		}
	}

	return nil
}

const testUserManagementV2Basic = `
	resource "keboola_gooddata_user_management_v2" "test_config" {
		name = "test name"
		description = "test description"
		custom_domain = "domain"
		project_id = "123"
		login = "some-login"
		hashed_password = "pass"
	}`
