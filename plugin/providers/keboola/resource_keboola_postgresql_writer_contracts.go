package keboola

type PostgreSQLWriterDatabaseParameters struct {
	HostName          string `json:"host"`
	Database          string `json:"database"`
	EncryptedPassword string `json:"#password"`
	Username          string `json:"user"`
	Schema            string `json:"schema"`
	Port              string `json:"port"`
	Driver            string `json:"driver"`
}

type PostgreSQLWriterTableItem struct {
	Name         string `json:"name"`
	DatabaseName string `json:"dbName"`
	Type         string `json:"type"`
	Size         string `json:"size"`
	IsNullable   bool   `json:"nullable"`
	DefaultValue string `json:"default"`
}

type PostgreSQLWriterTable struct {
	DatabaseName string                      `json:"dbName"`
	Export       bool                        `json:"export"`
	Incremental  bool                        `json:"incremental"`
	TableID      string                      `json:"tableId"`
	PrimaryKey   []string                    `json:"primaryKey,omitempty"`
	Items        []PostgreSQLWriterTableItem `json:"items"`
}

type PostgreSQLWriterParameters struct {
	Database PostgreSQLWriterDatabaseParameters `json:"db"`
	Tables   []PostgreSQLWriterTable            `json:"tables,omitempty"`
}

type PostgreSQLWriterStorageTable struct {
	Source      string   `json:"source"`
	Destination string   `json:"destination"`
	Columns     []string `json:"columns"`
}

type PostgreSQLWriterStorage struct {
	Input struct {
		Tables []PostgreSQLWriterStorageTable `json:"tables,omitempty"`
	} `json:"input"`
}

type PostgreSQLWriterConfiguration struct {
	Parameters PostgreSQLWriterParameters `json:"parameters,omitempty"`
	Storage    PostgreSQLWriterStorage    `json:"storage,omitempty"`
}

type PostgreSQLWriter struct {
	ID            string                        `json:"id,omitempty"`
	Name          string                        `json:"name"`
	Description   string                        `json:"description"`
	Configuration PostgreSQLWriterConfiguration `json:"configuration"`
}
