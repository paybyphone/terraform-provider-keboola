# terraform-provider-keboola

[Terraform](https://www.terraform.io) Custom Provider for [Keboola Connection (KBC)](https://www.keboola.com)

## Description

This is a custom terraform provider for managing resources within the Keboola Connection (KBC) platform, such as Transformations, Orchestrations, Writers etc.

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
