## 0.3.1 (16 July 2019)

FIXES:

* Fixed a bug in `keboola_gooddata_writer_v3` which caused extra storage tables to be created in the GoodData Writer configuration.

## 0.3.0 (15 July 2019)

IMPROVEMENTS:

* Added `keboola_ftp_extractor` and `keboola_ftp_extractor_file` for managing FTP Extractor for FTP, FTPS and SFTP server integrations.

## 0.2.0 (10 July 2019)

DEPRECATIONS:

* `keboola_storage_table`: `indexed_columns` has been deprecated and no longer has any effect: http://status.keboola.com/week-in-review-february-12-2018
* `keboola_gooddata_writer` and `keboola_gooddata_user_management` have been deprecated and should no longer be used: http://status.keboola.com/weeks-in-review-april-20-2019. They have been replaced with `keboola_gooddata_writer_v3` and `keboola_gooddata_user_management_v2` respectively.

IMPROVEMENTS:

* Added `keboola_gooddata_writer_v3` and `keboola_gooddata_user_management_v2` resources.
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
