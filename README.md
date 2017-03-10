# terraform-provider-keboola

[Terraform](https://www.terraform.io) Custom Provider for [Keboola Connection (KBC)](https://www.keboola.com)

[![Build Status](https://travis-ci.org/plmwong/terraform-provider-keboola.svg?branch=master)](https://travis-ci.org/plmwong/terraform-provider-keboola)

## Description

This is a custom terraform provider for managing resources within the Keboola Connection (KBC) platform, such as Transformations, Orchestrations, Writers etc.

## Disclaimer
`terraform-provider-keboola` is early stage software and is provided *"as-is"* under the [Apache 2.0 License](https://www.apache.org/licenses/LICENSE-2.0). Please use proper caution and discretion when using this provider in critical applications.

## Supported Resources

Currently, the following KBC resources are supported (or partially supported) for configuration via Terraform:

* `keboola_access_token`
* `keboola_gooddata_user_management`
* `keboola_gooddata_writer`
* `keboola_orchestration`
* `keboola_orchestration_tasks`
* `keboola_storage_bucket`
* `keboola_storage_table`
* `keboola_transformation_bucket`
* `keboola_transformation`

## Requirement

* [hashicorp/terraform](https://github.com/hashicorp/terraform)


## Usage

### Provider Configuration

#### `keboola`

```
provider "keboola" {
  api_key     =   "${var.storage_api_key}"
}
```

### Resource Configuration

#### `keboola_transformation_bucket`

A transformation bucket is an organisational grouping of transformations.

```

resource "keboola_transformation_bucket" "foo" {
  name              = "Foo"
  description       = "This is a Foo bucket."
}

```

#### `keboola_transformation`

```

resource "keboola_transformation" "bar" {
  bucket_id         = "${keboola_transformation_bucket.terraform_test_bucket.id}"
  name              = "Bar"
  description       = "This is a Bar transformation."
  backend           = "snowflake"
  type              = "simple"
}

```
