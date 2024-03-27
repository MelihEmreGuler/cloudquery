package client

import (
	"context"
	"fmt"
	"github.com/arangodb/go-driver"
	"strings"

	"github.com/cloudquery/plugin-sdk/v4/message"
	"github.com/cloudquery/plugin-sdk/v4/schema"
)

func (c *Client) Write(ctx context.Context, msgs <-chan message.WriteMessage) error {
	if err := c.writer.Write(ctx, msgs); err != nil {
		return err
	}
	if err := c.writer.Flush(ctx); err != nil {
		return fmt.Errorf("failed to flush: %w", err)
	}
	return nil
}

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
			// manipulate the key to be used as _key in arangodb
			row["_key"] = row["_cq_id"]
			// remove the _cq_id from the document
			// delete(row, "_cq_id")
			rows = append(rows, row)
		}
	}
	pks := table.PrimaryKeys()
	if len(pks) == 0 {
		// If no primary keys are defined, use all columns
		pks = table.Columns.Names()
	}

	c.logger.Debug().Str("table name:", tableName).Msg("Executing statement")

	if err = upsertDocuments(ctx, db, c.spec.Collection, rows, pks); err != nil {
		return err
	}
	c.logger.Debug().Str("table name:", tableName).Msg("Upserted documents successfully")
	return nil
}

// upsertDocuments checks if the documents already exist in the collection and updates them if they do, otherwise inserts them.
func upsertDocuments(ctx context.Context, db driver.Database, collectionName string, docs []map[string]any, keys []string) error {

	for _, doc := range docs {
		// Create key filter and search document for UPSERT query
		keyFilterParts := make([]string, 0, len(keys))
		//add the _key to the key filter for upsert query
		keys = append(keys, "_key")
		for _, key := range keys {
			keyFilterParts = append(keyFilterParts, fmt.Sprintf(`"%s": @%s`, key, key))
		}
		keyFilter := "{" + strings.Join(keyFilterParts, ", ") + "}"

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
		//remove the _cq_id from the document
		//delete(doc, "_cq_id")

		// Run the query
		cur, err := db.Query(ctx, query, bindVars)
		if err != nil {
			return fmt.Errorf("failed to execute upsert query for document %v: %w", doc, err)
		}
		if err = cur.Close(); err != nil {
			return fmt.Errorf("failed to close cursor: %w", err)
		} // Close Cursor
		fmt.Println(fmt.Sprintf("Upserted table: %v", doc["label"]))
	}
	fmt.Println("Upserted documents successfully")

	return nil
}

// clearCollectionIndexes removes given indexes from the collection.
func clearCollectionIndexes(ctx context.Context, db driver.Database, collectionName string, indexName string) error {
	collection, err := db.Collection(ctx, collectionName)
	if err != nil {
		return fmt.Errorf("failed to get collection: %w", err)
	}

	// Get indexes
	indexes, err := collection.Indexes(ctx)
	if err != nil {
		return fmt.Errorf("failed to get indexes: %w", err)
	}

	for _, index := range indexes {
		fmt.Println("index name: ", index.Name(), "type: ", index.Type())
	}

	// Remove hash indexes
	for _, index := range indexes {
		if string(index.Type()) != indexName {
			continue
		}
		if err = index.Remove(ctx); err != nil {
			fmt.Println("Index removed:", index.Name())
			return fmt.Errorf("failed to drop index %s - %s: %w", index.Name(), index.ID(), err)
		}
	}
	return nil
}
