package client

import (
	"context"
	"fmt"
	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/memory"
	"github.com/arangodb/go-driver"
	"github.com/cloudquery/plugin-sdk/v4/schema"
)

func (c *Client) reverseTransform(table *schema.Table, doc map[string]interface{}) (arrow.Record, error) {
	sc := table.ToArrowSchema()
	bldr := array.NewRecordBuilder(memory.DefaultAllocator, sc)
	for i, f := range sc.Fields() {
		if err := c.reverseTransformField(f, bldr.Field(i), doc[f.Name]); err != nil {
			return nil, err
		}
	}
	rec := bldr.NewRecord()
	return rec, nil
}

func (c *Client) reverseTransformField(f arrow.Field, bldr array.Builder, val interface{}) error {
	if val == nil {
		bldr.AppendNull()
		return nil
	}
	switch b := bldr.(type) {
	case *array.BooleanBuilder:
		b.Append(val.(bool))
	case *array.Int32Builder:
		b.Append(val.(int32))
	case *array.Int64Builder:
		b.Append(val.(int64))
	case *array.Float32Builder:
		b.Append(val.(float32))
	case *array.Float64Builder:
		b.Append(val.(float64))
	case *array.StringBuilder:
		b.Append(val.(string))
	default:
		return fmt.Errorf("unsupported type %T with builder %T and column %s", val, bldr, f.Name)
	}
	return nil
}

func (c *Client) Read(ctx context.Context, table *schema.Table, res chan<- arrow.Record) error {

	db, err := c.client.Database(ctx, c.spec.DbName)
	if err != nil {
		return fmt.Errorf("failed to get database: %w", err)
	}

	query := fmt.Sprintf("FOR d IN %s RETURN d", table.Name)
	cursor, err := db.Query(ctx, query, nil)
	if err != nil {
		return fmt.Errorf("failed to query table %s: %w", table.Name, err)
	}

	defer cursor.Close()
	for {
		var doc map[string]interface{}
		_, err = cursor.ReadDocument(ctx, &doc)
		if driver.IsNoMoreDocuments(err) {
			break
		} else if err != nil {
			return fmt.Errorf("failed to read document from table %s: %w", table.Name, err)
		}

		rec, err := c.reverseTransform(table, doc)
		if err != nil {
			return fmt.Errorf("failed to transform document from table %s: %w", table.Name, err)
		}

		res <- rec
	}

	return nil
}
