// Package cursor exposes a cursor implementation that facilitates easy
// batch reads as well as reading of custom cursor properties like
// resume tokens.
package cursor

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/olekukonko/errors"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// Cursor is like mongo.Cursor, but it exposes documents per batch rather than
// a per-document reader. It also exposes cursor metadata, which facilitates
// things like resumable $natural scans.
type Cursor struct {
	id          int64
	ns          string
	db          *mongo.Database
	curBatch    []bson.Raw
	baseExtra   ExtraMap
	cursorExtra ExtraMap
}

// ExtraMap represents “extra” data points in cursor metadata.
type ExtraMap = map[string]bson.RawValue

// GetCurrentBatch returns the Cursor’s current batch of documents.
func (c Cursor) GetCurrentBatch() []bson.Raw {
	return slices.Clone(c.curBatch)
}

// GetExtra returns the current response’s “extra” metadata.
func (c Cursor) GetExtra() ExtraMap {
	return maps.Clone(c.baseExtra)
}

// GetCursorExtra returns the current cursor batch’s “extra” metadata.
func (c Cursor) GetCursorExtra() ExtraMap {
	return maps.Clone(c.cursorExtra)
}

// IsFinished indicates whether the present batch is the final one.
func (c Cursor) IsFinished() bool {
	return c.id == 0
}

// GetNext fetches the next batch of responses from the server and caches it
// for access via GetCurrentBatch().
//
// extraPieces are things you want to add to the underlying `getMore`
// server call, such as `batchSize`.
func (c *Cursor) GetNext(ctx context.Context, extraPieces ...bson.E) error {
	if c.IsFinished() {
		panic("internal error: cursor already finished!")
	}

	nsDB, nsColl, found := strings.Cut(c.ns, ".")
	if !found {
		panic("Malformed namespace from cursor (expect a dot): " + c.ns)
	}
	if nsDB != c.db.Name() {
		panic(fmt.Sprintf("db from cursor (%s) mismatches db struct (%s)", nsDB, c.db.Name()))
	}

	cmd := bson.D{
		{"getMore", c.id},
		{"collection", nsColl},
	}

	cmd = append(cmd, extraPieces...)

	resp := c.db.RunCommand(ctx, cmd)

	raw, err := resp.Raw()
	if err != nil {
		return fmt.Errorf("iterating %#q’s cursor: %w", c.ns, err)
	}

	baseResp := baseResponse{}

	err = bson.Unmarshal(raw, &baseResp)
	if err != nil {
		return fmt.Errorf("decoding %#q’s iterated cursor to %T: %w", c.ns, baseResp, err)
	}

	c.curBatch = baseResp.Cursor.NextBatch
	c.baseExtra = baseResp.Extra
	c.cursorExtra = baseResp.Cursor.Extra
	c.id = baseResp.Cursor.ID

	return nil
}

type cursorResponse struct {
	ID         int64
	Ns         string
	FirstBatch []bson.Raw
	NextBatch  []bson.Raw
	Extra      ExtraMap `bson:",inline"`
}

type baseResponse struct {
	Cursor cursorResponse
	Extra  ExtraMap `bson:",inline"`
}

// New creates a Cursor from the response of a cursor-returning command
// like `find` or `bulkWrite`.
//
// Use this control (rather than the Go driver’s cursor implementation)
// to extract parts of the cursor responses that the driver’s API doesn’t
// expose. This is useful, e.g., to do a resumable $natural scan by
// extracting resume tokens from `find` responses.
func New(
	db *mongo.Database,
	resp *mongo.SingleResult,
) (*Cursor, error) {
	raw, err := resp.Raw()
	if err != nil {
		return nil, errors.Wrapf(err, "cursor open failed")
	}

	baseResp := baseResponse{}

	err = bson.Unmarshal(raw, &baseResp)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to decode cursor-open response to %T", baseResp)
	}

	return &Cursor{
		db:          db,
		id:          baseResp.Cursor.ID,
		ns:          baseResp.Cursor.Ns,
		curBatch:    baseResp.Cursor.FirstBatch,
		baseExtra:   baseResp.Extra,
		cursorExtra: baseResp.Cursor.Extra,
	}, nil
}
