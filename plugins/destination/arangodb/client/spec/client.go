package client

import (
	"context"
	"fmt"
	"github.com/arangodb/go-driver/http"
	"github.com/goccy/go-json"

	"github.com/arangodb/go-driver"
	"github.com/cloudquery/plugin-sdk/v4/plugin"
	"github.com/cloudquery/plugin-sdk/v4/writers/batchwriter"
	"github.com/rs/zerolog"
)

type Client struct {
	plugin.UnimplementedSource
	batchwriter.UnimplementedDeleteRecord
	logger zerolog.Logger
	spec   *Spec
	client driver.Client
	writer *batchwriter.BatchWriter
}

func New(ctx context.Context, logger zerolog.Logger, spec []byte, _ plugin.NewClientOptions) (plugin.Client, error) {
	var err error
	c := &Client{
		logger: logger.With().Str("module", "arangodb").Logger(),
	}
	if err = json.Unmarshal(spec, &c.spec); err != nil {
		return nil, fmt.Errorf("failed to unmarshal ArangoDB spec: %w", err)
	}
	if err = c.spec.Validate(); err != nil {
		return nil, err
	}
	c.spec.SetDefaults()

	// Connect to ArangoDB
	// prefix the hostname with http://, if we conncting to a remote server the prefix will be https://
	conn, err := http.NewConnection(http.ConnectionConfig{
		Endpoints: []string{fmt.Sprintf("http://%s:%d", c.spec.Hostname, c.spec.Port)},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP connection for ArangoDB: %w", err)
	}

	// burası patlarsa pointeri kaldırmayı deneyebilirsin
	c.client, err = driver.NewClient(driver.ClientConfig{
		Connection:     conn,
		Authentication: driver.BasicAuthentication(c.spec.Username, c.spec.Password),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create ArangoDB client: %w", err)
	}

	// BatchWriter nesnesi oluştur
	c.writer, err = batchwriter.New(c, batchwriter.WithBatchSize(c.spec.BatchSize), batchwriter.WithBatchSizeBytes(c.spec.BatchSizeBytes), batchwriter.WithLogger(c.logger))
	if err != nil {
		return nil, fmt.Errorf("failed to create batch writer: %w", err)
	}

	return c, nil
}

func (c *Client) Close(ctx context.Context) error {
	if err := c.writer.Close(ctx); err != nil {
		return fmt.Errorf("failed to close batch writer: %w", err)
	}
	// If you need to close the connection to ArangoDB here, you can add
	return nil
}
