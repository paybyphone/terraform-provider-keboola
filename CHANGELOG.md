## 0.2.5 (Unreleased)

DEPRECATIONS:

* `keboola_storage_table`: `indexed_columns` has been deprecated and no longer has any effect: http://status.keboola.com/week-in-review-february-12-2018

IMPROVEMENTS:

* `keboola_snowflake_extractor`: Added support for the Snowflake Extractor resource/component in Keboola Connection.

## 0.1.4 (13 May 2019)

IMPROVEMENTS:

* `keboola_storage_bucket`: Supports linking of shared buckets between Keboola projects. Using the new `is_linked`, `source_project_id`, and `source_bucket_id` properties.

## 0.1.3 (18 April 2019)

IMPROVEMENTS:

* `provider`: Whitespace is now trimmed from the API key when it is read from configuration, which will help alleviate issues on first set up of terraform.

FIXES:

* `keboola_snowflake_writer`: `ID` and `workspaceID` on `ProvisionSnowflakeResponse` are now `string` types instead of `int` types.
* `keboola_snowflake_writer_tables`: Data filtering columns (e.g. `where_column`, `changed_since`) are now mapped.
