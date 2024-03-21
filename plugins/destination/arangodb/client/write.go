package client

import (
	"context"
	"fmt"
	"github.com/arangodb/go-driver"
	"github.com/google/uuid"
	"strings"

	"github.com/cloudquery/plugin-sdk/v4/message"
	"github.com/cloudquery/plugin-sdk/v4/schema"
)

func (c *Client) WriteTableBatch(ctx context.Context, tableName string, msgs message.WriteInserts) error {
	if len(msgs) == 0 {
		return nil
	}

	table, err := schema.NewTableFromArrowSchema(msgs[0].Record.Schema())
	if err != nil {
		return err
	}

	// open a new connection to the database
	db, err := c.Database(ctx)
	if err != nil {
		return err
	}

	rows := make([]map[string]any, 0, len(msgs))
	for i := range msgs {
		transformedRows := transformValues(msgs[i].Record)
		// Add the table name to the row as label
		for _, row := range transformedRows {
			// Add the table name to each row as a label
			row["label"] = tableName

			rows = append(rows, row)
		}
	}
	pks := table.PrimaryKeys()
	if len(pks) == 0 {
		// If no primary keys are defined, use all columns
		pks = table.Columns.Names()
	}

	c.logger.Debug().Any("rows", rows).Msg("Executing statement")

	if err = upsertDocuments(ctx, db, c.spec.Collection, rows, pks); err != nil {
		return err
	}
	return nil
}

func upsertDocuments(ctx context.Context, db driver.Database, collectionName string, docs []map[string]any, keys []string) error {
	for _, doc := range docs {
		// Create key filter and search document for UPSERT query
		keyFilterParts := make([]string, 0, len(keys))
		for _, key := range keys {
			keyFilterParts = append(keyFilterParts, fmt.Sprintf(`"%s": @%s`, key, key))
		}
		keyFilter := "{" + strings.Join(keyFilterParts, ", ") + "}"

		// UPSERT sorgusunu hazırla
		query := fmt.Sprintf(`
			UPSERT %s
			INSERT @doc
			UPDATE @doc
			IN @@collection
		`, keyFilter)

		bindVars := map[string]interface{}{
			"@collection": collectionName,
			"doc":         doc,
		}
		// add primary keys to bindVars
		for _, key := range keys {
			bindVars[key] = doc[key]
		}

		// Run the query
		cur, err := db.Query(ctx, query, bindVars)
		if err != nil {
			return fmt.Errorf("failed to execute upsert query for document %v: %w", doc, err)
		}
		if err = cur.Close(); err != nil {
			return fmt.Errorf("failed to close cursor: %w", err)
		} // Close Cursor
	}
	fmt.Println("Upserted documents successfully")

	return nil
}
func GenerateUUID() string {
	newUUID := uuid.New()
	return newUUID.String()
}

func (c *Client) Write(ctx context.Context, msgs <-chan message.WriteMessage) error {
	if err := c.writer.Write(ctx, msgs); err != nil {
		return err
	}
	if err := c.writer.Flush(ctx); err != nil {
		return fmt.Errorf("failed to flush: %w", err)
	}
	return nil
}
