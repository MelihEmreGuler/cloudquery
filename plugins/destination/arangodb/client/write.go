package client

import (
	"context"
	"fmt"
	"github.com/apache/arrow/go/v15/arrow"
	"github.com/cloudquery/plugin-sdk/v4/message"
	"github.com/cloudquery/plugin-sdk/v4/schema"
)

package client

import (
"context"
"fmt"

"github.com/arangodb/go-driver"
"github.com/cloudquery/plugin-sdk/v4/message"
"github.com/cloudquery/plugin-sdk/v4/schema"
)

func (c *Client) transformValues(r arrow.Record, cqTimeIndex int) []map[string]any {
	results := make([]map[string]any, r.NumRows())

	for i := range results {
		results[i] = make(map[string]any, r.NumCols())
	}
	sc := r.Schema()
	for i := 0; i < int(r.NumCols()); i++ {
		col := r.Column(i)
		transformed := c.transformArr(col, i == cqTimeIndex)
		for l := 0; l < col.Len(); l++ {
			results[l][sc.Field(i).Name] = transformed[l]
		}
	}
	return results
}

func (c *Client) WriteTableBatch(ctx context.Context, tableName string, msgs message.WriteInserts) error {
	if len(msgs) == 0 {
		return nil
	}

	// Veritabanı ve koleksiyonun alınması
	db, err := c.client.Database(ctx, c.spec.DbName)
	if err != nil {
		return fmt.Errorf("failed to get database: %w", err)
	}
	col, err := db.Collection(ctx, tableName)
	if err != nil {
		return fmt.Errorf("failed to get collection: %w", err)
	}


	return nil
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
