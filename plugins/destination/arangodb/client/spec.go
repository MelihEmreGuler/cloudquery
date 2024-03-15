package client

import (
	_ "embed"
	"fmt"
)

const (
	defaultBatchSize      = 1000
	defaultBatchSizeBytes = 1024 * 1024 * 4 // 4MB
)

type Spec struct {
	// Hostname of the ArangoDB server. Prefix with 'http://' for localhost or 'https://' for remote servers.
	Hostname string `json:"hostname" jsonschema:"required,minLength=1"`

	// Port on which the ArangoDB server is running.
	Port string `json:"port" jsonschema:"required,minimum=1,maximum=65535"`

	// DbName of the database to sync the data to.
	DbName string `json:"dbName" jsonschema:"required,minLength=1"`

	// Username for authenticating with the ArangoDB server.
	Username string `json:"username" jsonschema:"required,minLength=1"`

	// Password for authenticating with the ArangoDB server.
	Password string `json:"password" jsonschema:"required,minLength=1"`

	// Collection Name of the collection to sync the data to.
	Collection string `json:"collection" jsonschema:"required,minLength=1"`

	// BatchSize Maximum number of items that may be grouped together to be written in a single write.
	BatchSize int `json:"batch_size,omitempty" jsonschema:"minimum=1,default=1000"`

	// BatchSizeBytes Maximum size of items that may be grouped together to be written in a single write.
	BatchSizeBytes int `json:"batch_size_bytes,omitempty" jsonschema:"minimum=1,default=4194304"`

	// Protocol to use for connecting to the ArangoDB server. Can be 'http' or 'https'.
	Protocol string `json:"protocol,omitempty" jsonschema:"enum=http,https,default=http"`
}

//go:embed schema.json
var JSONSchema string

func (s *Spec) SetDefaults() {
	if s.BatchSize == 0 {
		s.BatchSize = defaultBatchSize
	}
	if s.BatchSizeBytes == 0 {
		s.BatchSizeBytes = defaultBatchSizeBytes
	}
	if s.Protocol == "" {
		s.Protocol = "http"
	}
}

func (s *Spec) Validate() error {
	if s.Hostname == "" {
		return fmt.Errorf("hostname is required")
	}
	if s.Port == "" {
		return fmt.Errorf("port is required")
	}
	if s.DbName == "" {
		return fmt.Errorf("dbName is required")
	}
	if s.Username == "" {
		return fmt.Errorf("username is required")
	}
	if s.Password == "" {
		return fmt.Errorf("password is required")
	}
	if s.Collection == "" {
		return fmt.Errorf("collection is required")
	}
	return nil
}
