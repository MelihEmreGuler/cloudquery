package client

import (
	"context"
	"fmt"
	"time"

	"github.com/cloudquery/plugin-sdk/v4/message"
	"github.com/cloudquery/plugin-sdk/v4/schema"
)

func (c *Client) DeleteStale(ctx context.Context, msgs message.WriteDeleteStales) error {
	for _, msg := range msgs {
		query := fmt.Sprintf(`
			FOR doc IN %s 
			FILTER doc.%s == @sourceName AND doc.%s < @syncTime 
			REMOVE doc IN %s`,
			msg.TableName,
			schema.CqSourceNameColumn.Name,
			schema.CqSyncTimeColumn.Name,
			msg.TableName,
		)

		bindVars := map[string]interface{}{
			"sourceName": msg.SourceName,
			"syncTime":   msg.SyncTime.Truncate(time.Microsecond),
		}

		db, err := c.client.Database(ctx, c.spec.DbName)
		if err != nil {
			return fmt.Errorf("failed to get database: %w", err)
		}
		_, err = db.Query(ctx, query, bindVars)
		if err != nil {
			return fmt.Errorf("failed to execute query for deleting stale documents: %w", err)
		}
	}
	return nil
}
