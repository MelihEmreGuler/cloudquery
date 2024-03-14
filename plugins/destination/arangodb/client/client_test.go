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
	defaultHostname = "localhost"
	defaultPort     = "8529"
	defaultDbName   = "_system"
	defaultUsername = "root"
	defaultPassword = "arangopass"
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
		Hostname: getenv("CQ_DEST_ARANGODB_HOSTNAME", defaultHostname),
		Port:     getenv("CQ_DEST_ARANGODB_PORT", defaultPort),
		DbName:   getenv("CQ_DEST_ARANGODB_DBNAME", defaultDbName),
		Username: getenv("CQ_DEST_ARANGODB_USERNAME", defaultUsername),
		Password: getenv("CQ_DEST_ARANGODB_PASSWORD", defaultPassword),
	}
	s.SetDefaults()
	require.NoError(t, s.Validate())
	b, err := json.Marshal(s)
	require.NoError(t, err)

	err = p.Init(ctx, b, plugin.NewClientOptions{})
	require.NoError(t, err)

	plugin.TestWriterSuiteRunner(t,
		p,
		plugin.WriterTestSuiteTests{
			// Although we do support migrations, the old data can persist for the tables where PK is changed.
			SkipMigrate:      true,
			SkipDeleteRecord: true,
			SafeMigrations: plugin.SafeMigrations{
				AddColumn:    true,
				RemoveColumn: true,
			},
		},
		plugin.WithTestIgnoreNullsInLists(),
		plugin.WithTestDataOptions(schema.TestSourceOptions{
			TimePrecision: time.Microsecond,
			SkipLists:     true,
		}),
	)
}
