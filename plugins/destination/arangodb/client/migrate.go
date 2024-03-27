package client

import (
	"context"
	"fmt"
	"github.com/arangodb/go-driver"
	"github.com/cloudquery/plugin-sdk/v4/message"
	"github.com/cloudquery/plugin-sdk/v4/schema"
)

type IndexOptions struct {
	Fields []string
	Unique bool
	Sparse bool
}

// MigrateTables creates indexes for the given tables. (comment out when syncing data to multiple collections)
func (c *Client) MigrateTables(ctx context.Context, messages message.WriteMigrateTables) error {
	/*if len(messages) == 0 {
		return nil
	}

	db, err := c.Database(ctx)
	if err != nil {
		return err
	}

	for _, m := range messages {
		col, err := db.Collection(ctx, c.spec.Collection)
		if err != nil {
			return fmt.Errorf("failed to get collection %s: %w", m.Table.Name, err)
		}

		if err = c.tryCreateIndex(ctx, col, m); err != nil {
			return fmt.Errorf("failed to create index for %q: %w", m.Table.Name, err)
		}
	}*/
	return nil
}

// tryCreateIndex creates an index for the given collection if it does not already exist.
// but it should not be used if we want to save data in a single collection.
// if we want to use multiple collections we can use this function to create indexes.
func (c *Client) tryCreateIndex(ctx context.Context, col driver.Collection, migrate *message.WriteMigrateTable) error {
	indexOptions := createIndexOptions(migrate.Table)
	if indexOptions == nil {
		c.logger.Debug().Str("table", migrate.Table.Name).Msg("table has no primary keys, skipping")
		return nil
	}

	// EnsureHashIndex is an example; adjust based on your indexing needs
	_, _, err := col.EnsureHashIndex(ctx, indexOptions.Fields, &driver.EnsureHashIndexOptions{
		Unique: indexOptions.Unique,
		Sparse: indexOptions.Sparse,
	})
	if err != nil {
		c.logger.Err(err).
			Str("table", migrate.Table.Name).
			Msg("failed to ensure index")
		return fmt.Errorf("failed to ensure index for %q: %w", migrate.Table.Name, err)
	}

	return nil
}

func createIndexOptions(table *schema.Table) *IndexOptions {
	pks := table.PrimaryKeys()
	if len(pks) == 0 {
		// no primary keys = no index needed
		return nil
	}

	fields := make([]string, len(pks))
	for i, pk := range pks {
		fields[i] = pk
	}

	return &IndexOptions{
		Fields: fields,
		Unique: true,
		Sparse: false,
	}
}
