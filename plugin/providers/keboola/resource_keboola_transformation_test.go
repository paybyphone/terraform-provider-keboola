package keboola

import (
	"fmt"
	"testing"

	"github.com/hashicorp/terraform/helper/resource"
	"github.com/hashicorp/terraform/terraform"
)

func TestAccTransformation_Basic(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:  func() { testAccPreCheck(t) },
		Providers: testAccProviders,
		CheckDestroy: resource.ComposeTestCheckFunc(
			testAccCheckTransformationBucketDestroy,
			testAccCheckTransformationDestroy,
		),
		Steps: []resource.TestStep{
			{
				Config: testTransformBasic,
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("keboola_transformation.test_transform", "name", "test name"),
					resource.TestCheckResourceAttr("keboola_transformation.test_transform", "description", "test description"),
					resource.TestCheckResourceAttr("keboola_transformation.test_transform", "type", "simple"),
					resource.TestCheckResourceAttr("keboola_transformation.test_transform", "backend", "snowflake"),
				),
			},
		},
	})
}

func testAccCheckTransformationDestroy(s *terraform.State) error {
	client := testAccProvider.Meta().(*KBCClient)

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "keboola_transformation" {
			continue
		}

		transformURI := fmt.Sprintf("storage/components/transformation/configs/%s/rows/%s", rs.Primary.Attributes["bucket_id"], rs.Primary.ID)
		fmt.Println(transformURI)
		getResp, err := client.GetFromStorage(transformURI)

		if err == nil && getResp.StatusCode == 200 {
			return fmt.Errorf("Transformation bucket still exists")
		}
	}

	return nil
}

const testTransformBasic = `
	resource "keboola_transformation_bucket" "test_bucket" {
		name = "test name"
	}

	resource "keboola_transformation" "test_transform" {
		bucket_id = "${keboola_transformation_bucket.test_bucket.id}"
		name = "test name"
		description = "test description"
		type = "simple"
		backend = "snowflake"
	}`
