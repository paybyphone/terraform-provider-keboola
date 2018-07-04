package keboola

type SnowflakeWriterDatabaseParameters struct {
HostName          string `json:"host"`
Database          string `json:"database"`
Password          string `json:"password,omitempty"`
EncryptedPassword string `json:"#password,omitempty"`
Username          string `json:"user"`
Schema            string `json:"schema"`
Port              string `json:"port"`
Driver            string `json:"driver"`
Warehouse         string `json:"warehouse"`
}

type SnowflakeWriterTableItem struct {
Name         string `json:"name"`
DatabaseName string `json:"dbName"`
Type         string `json:"type"`
Size         string `json:"size"`
IsNullable   bool   `json:"nullable"`
DefaultValue string `json:"default"`
}

type SnowflakeWriterTable struct {
DatabaseName string                     `json:"dbName"`
Export       bool                       `json:"export"`
Incremental  bool                       `json:"incremental"`
TableID      string                     `json:"tableId"`
PrimaryKey   []string                   `json:"primaryKey,omitempty"`
Items        []SnowflakeWriterTableItem `json:"items"`
}

type SnowflakeWriterParameters struct {
Database SnowflakeWriterDatabaseParameters `json:"db"`
Tables   []SnowflakeWriterTable            `json:"tables,omitempty"`
}

type SnowflakeWriterStorageTable struct {
Source      string   `json:"source"`
Destination string   `json:"destination"`
Columns     []string `json:"columns"`
}

type SnowflakeWriterStorage struct {
Input struct {
Tables []SnowflakeWriterStorageTable `json:"tables,omitempty"`
} `json:"input,omitempty"`
}

type SnowflakeWriterConfiguration struct {
Parameters SnowflakeWriterParameters `json:"parameters"`
Storage    SnowflakeWriterStorage    `json:"storage,omitempty"`
}

type ProvisionSnowflakeResponse struct {
Status      string `json:"status"`
Credentials struct {
ID          int    `json:"id"`
HostName    string `json:"hostname"`
Port        int    `json:"port"`
Database    string `json:"db"`
Schema      string `json:"schema"`
Warehouse   string `json:"warehouse"`
Username    string `json:"user"`
Password    string `json:"password"`
WorkspaceID int    `json:"workspaceId"`
} `json:"credentials"`
}

//SnowflakeWriter is the data model for Snowflake Writers within
//the Keboola Storage API.
type SnowflakeWriter struct {
ID            string                       `json:"id,omitempty"`
Name          string                       `json:"name"`
Description   string                       `json:"description"`
Configuration SnowflakeWriterConfiguration `json:"configuration"`
}

