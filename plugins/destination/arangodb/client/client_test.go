package client

import (
	"context"
	"encoding/json"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
	"time"

	"github.com/cloudquery/plugin-sdk/v4/plugin"
	"github.com/cloudquery/plugin-sdk/v4/schema"
)

const (
	defaultHostname   = "localhost"
	defaultPort       = "8529"
	defaultDbName     = "_system"
	defaultUsername   = "root"
	defaultPassword   = "arangopass"
	defaultCollection = "test"
)

func getenv(key, fallback string) string {
	value := os.Getenv(key)
	if len(value) == 0 {
		return fallback
	}
	return value
}

func TestPlugin(t *testing.T) {
	ctx := context.Background()
	p := plugin.NewPlugin("arangodb", "development", New)
	s := &Spec{
		Hostname:   getenv("CQ_DEST_ARANGODB_HOSTNAME", defaultHostname),
		Port:       getenv("CQ_DEST_ARANGODB_PORT", defaultPort),
		DbName:     getenv("CQ_DEST_ARANGODB_DBNAME", defaultDbName),
		Username:   getenv("CQ_DEST_ARANGODB_USERNAME", defaultUsername),
		Password:   getenv("CQ_DEST_ARANGODB_PASSWORD", defaultPassword),
		Collection: getenv("CQ_DEST_ARANGODB_COLLECTION", defaultCollection),
		//Protocol:   "https",
	}
	s.SetDefaults()
	require.NoError(t, s.Validate())

	b, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Init(ctx, b, plugin.NewClientOptions{}); err != nil {
		t.Fatal(err)
	}
	plugin.TestWriterSuiteRunner(t,
		p,
		plugin.WriterTestSuiteTests{
			SkipDeleteRecord: true,
			SkipMigrate:      true,
		},
		plugin.WithTestDataOptions(schema.TestSourceOptions{
			TimePrecision: time.Millisecond,
		}),
	)
}
