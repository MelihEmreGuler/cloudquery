package client

import (
	"context"
	"fmt"
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
		rows = append(rows, transformValues(msgs[i].Record)...)
	}
	var sb strings.Builder
	pks := table.PrimaryKeys()
	if len(pks) == 0 {
		// If there is no primary key, direct INSERT is used
		sb.WriteString("FOR row IN @rows INSERT row INTO ")
		sb.WriteString(c.spec.Collection)
	} else {
		// If the primary key exists, we can update or add unique records using UPSERT
		sb.WriteString("FOR row IN @rows UPSERT { ")
		for i, pk := range pks {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(pk + ": row." + pk)
		}
		sb.WriteString(" } INSERT row UPDATE row INTO ")
		sb.WriteString(c.spec.Collection)
	}

	stmt := sb.String()
	c.logger.Debug().Str("stmt", stmt).Any("rows", rows).Msg("Executing statement")
	cursor, err := db.Query(ctx, stmt, map[string]interface{}{"rows": rows})
	if err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
	}
	return cursor.Close()
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
