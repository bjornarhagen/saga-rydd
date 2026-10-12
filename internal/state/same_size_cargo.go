package state

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"math"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const SameSizeCargoPolicy = "cargo_default_target_saved_layout_v1"
const sameSizeCargoAncestorLimit = 256

// Private output roles select original SQL values, never recognition authority.
const (
	sameSizeCargoMemberRole int64 = 0
	sameSizeCargoLayoutRole int64 = 1
)

// SameSizeOptions selects only a saved-page visibility filter. It conveys no
// content, regeneration, current-state or cleanup authority.
type SameSizeOptions struct {
	ExcludeCargoDefaultTarget bool
}

type SameSizeCargoTargetExclusion struct {
	Policy                              string `json:"policy"`
	Recognition                         string `json:"recognition"`
	EligibleRows                        int    `json:"eligible_rows"`
	RowsWithTargetAncestor              int    `json:"rows_with_target_ancestor"`
	ExcludedRows                        int    `json:"excluded_rows"`
	RetainedUnrecognizedRows            int    `json:"retained_unrecognized_rows"`
	RetainedUnknownRows                 int    `json:"retained_unknown_rows"`
	RetainedAncestorLimitRows           int    `json:"retained_ancestor_limit_rows"`
	RootQueryCalls                      int    `json:"root_query_calls"`
	RootEvidenceRequests                int    `json:"root_evidence_requests"`
	ReusedProofRows                     int    `json:"reused_proof_rows"`
	MembershipBatchQueryCalls           int    `json:"membership_batch_query_calls"`
	LayoutBatchQueryCalls               int    `json:"layout_batch_query_calls"`
	MarkerQualificationBatchQueryCalls  int    `json:"marker_qualification_batch_query_calls"`
	LeafEvidenceRequests                int    `json:"leaf_evidence_requests"`
	AncestorEvidenceRequests            int    `json:"ancestor_evidence_requests"`
	LayoutMarkerEvidenceRequests        int    `json:"layout_marker_evidence_requests"`
	MarkerQualificationEvidenceRequests int    `json:"marker_qualification_evidence_requests"`
}

type sameSizeCargoCursor struct {
	Version      int    `json:"version"`
	InventoryID  string `json:"inventory_id"`
	MinimumBytes int64  `json:"minimum_bytes"`
	Policy       string `json:"policy"`
	Size         int64  `json:"size"`
	ID           int64  `json:"id"`
}

func decodeSameSizeCargoCursor(token string) (sameSizeCursor, error) {
	if token == "" {
		return sameSizeCursor{}, nil
	}
	if len(token) > sameSizeCursorLimit {
		return sameSizeCursor{}, ErrReportCursor
	}
	body, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return sameSizeCursor{}, ErrReportCursor
	}
	var c sameSizeCargoCursor
	d := json.NewDecoder(bytes.NewReader(body))
	d.DisallowUnknownFields()
	if d.Decode(&c) != nil || d.Decode(new(any)) != io.EOF || c.Version != 2 || c.Policy != SameSizeCargoPolicy ||
		!sameSizeInventoryID(c.InventoryID) || c.MinimumBytes <= 0 || c.Size < c.MinimumBytes || c.ID <= 0 {
		return sameSizeCursor{}, ErrReportCursor
	}
	canonical, _ := json.Marshal(c)
	if base64.RawURLEncoding.EncodeToString(canonical) != token {
		return sameSizeCursor{}, ErrReportCursor
	}
	return sameSizeCursor{Version: 2, InventoryID: c.InventoryID, MinimumBytes: c.MinimumBytes, Size: c.Size, ID: c.ID}, nil
}

// False delegates to the unchanged original API, including its V1 cursor and
// diagnostics. True uses one existing inventory snapshot and never refills a
// page after exclusions. No source/configuration path is opened.
func (s *Store) SameSizeCandidatesWithOptions(ctx context.Context, limit int, token string, minimumBytes int64, options SameSizeOptions) (SameSizeReport, error) {
	if !options.ExcludeCargoDefaultTarget {
		return s.SameSizeCandidates(ctx, limit, token, minimumBytes)
	}
	return s.sameSizeCargoCandidates(ctx, limit, token, minimumBytes, sameSizeCargoHooks{})
}

type sameSizeCargoHooks struct {
	afterSnapshot       func()
	beforeCommit        func()
	afterRawPage        func()
	beforeExcluded      func(*sameSizeCargoIdentities, *BuildOutputFinding)
	closeEntryStatement func(*sql.Stmt) error
	closeRootStatement  func(*sql.Stmt) error
	wrapCursor          func(sameSizeCargoStage, sameSizeCargoRows) sameSizeCargoRows
	disableProofReuse   bool
}

// Integer conversion must not turn a corrupt SQLite text/real scalar into a
// usable saved identity or pass counter.
func sameSizeCargoInt(v any) (int64, error) {
	if n, ok := v.(int64); ok {
		return n, nil
	}
	return 0, errors.New("saved Cargo filter scalar is not an integer")
}

func readSameSizeCargoRows(ctx context.Context, tx *sql.Tx, query string, args ...any) (result []sameSizeRawFile, err error) {
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = errors.Join(err, rows.Close())
		if err != nil {
			result = nil
		}
	}()
	for rows.Next() {
		var e sameSizeRawFile
		var numbers [11]any
		f := &e.file
		if err = rows.Scan(&numbers[0], &numbers[1], &e.relative, &e.root, &numbers[2], &numbers[3], &numbers[4],
			&numbers[5], &numbers[6], &numbers[7], &numbers[8], &f.Device, &f.Inode, &numbers[9], &f.ParentPass, &numbers[10]); err != nil {
			return nil, err
		}
		var n [11]int64
		for i, v := range numbers {
			if n[i], err = sameSizeCargoInt(v); err != nil {
				return nil, err
			}
		}
		f.ID, f.RootID, f.Size, f.Allocated, f.ChangedNS, f.Generation = n[0], n[1], n[3], n[4], n[7], n[8]
		e.enabled, e.skipped, e.compacted = int(n[2]), int(n[9]), int(n[10])
		f.Path = filepath.Join(string(e.root), string(e.relative))
		f.PathBytes = []byte(f.Path)
		f.ObservedAt, f.ModifiedAt = time.Unix(0, n[5]).UTC(), time.Unix(0, n[6]).UTC()
		result = append(result, e)
	}
	return result, rows.Err()
}

// One exact entries key and bounded joins; a SQL call can perform several
// internal index/table accesses. These counters do not bound SQLite/OS I/O.
const sameSizeCargoEntryQuery = `SELECT e.id,
 CASE WHEN ?3=1 THEN substr(e.parent,1,4097) ELSE NULL END AS c02,
 CASE WHEN ?3=1 THEN substr(e.kind,1,33) ELSE NULL END AS c03,
 CASE WHEN ?3=1 THEN substr(e.device,1,1025) ELSE NULL END AS c04,
 CASE WHEN ?3=1 THEN substr(e.inode,1,1025) ELSE NULL END AS c05,
 e.mtime_ns,
 e.ctime_ns,
 e.generation,
 e.observed_at_ns,
 CASE WHEN ?3=1 THEN substr(e.skip_reason,1,4097) ELSE NULL END AS c10,
 COALESCE(p.generation,0),
 COALESCE(p.complete,0),
 CASE WHEN ?3=1 THEN substr(COALESCE(p.last_error,''),1,4097) ELSE NULL END AS c13,
 CASE WHEN ?3=0 THEN CASE
 WHEN length(COALESCE(substr(CAST(e.parent AS BLOB),1,4097),X''))>4096 THEN 0
 WHEN COALESCE(substr(CAST(e.parent AS BLOB),1,4097),X'')=CAST(?4 AS BLOB) THEN 2
 ELSE 1 END ELSE NULL END AS c14,
 CASE WHEN ?3=0 THEN substr(CAST(e.kind AS BLOB),1,33) ELSE NULL END AS c15,
 CASE WHEN ?3=0 THEN substr(CAST(e.device AS BLOB),1,21) ELSE NULL END AS c16,
 CASE WHEN ?3=0 THEN substr(CAST(e.inode AS BLOB),1,21) ELSE NULL END AS c17,
 CASE WHEN ?3=0 THEN e.size ELSE NULL END AS c18,
 CASE WHEN ?3=0 THEN e.allocated ELSE NULL END AS c19,
 CASE WHEN ?3=0 THEN CASE WHEN e.skip_reason='' THEN 0 ELSE 1 END ELSE NULL END AS c20,
 CASE WHEN ?3=0 THEN COALESCE(p.checked_at_ns,0) ELSE NULL END AS c21,
 CASE WHEN ?3=0 THEN CASE WHEN COALESCE(p.last_error,'')='' THEN 0 ELSE 1 END ELSE NULL END AS c22,
 CASE WHEN ?3=0 THEN COALESCE(o.generation,0) ELSE NULL END AS c23,
 CASE WHEN ?3=0 THEN COALESCE(o.complete,0) ELSE NULL END AS c24,
 CASE WHEN ?3=0 THEN COALESCE(o.checked_at_ns,0) ELSE NULL END AS c25,
 CASE WHEN ?3=0 THEN CASE WHEN COALESCE(o.last_error,'')='' THEN 0 ELSE 1 END ELSE NULL END AS c26 FROM entries e LEFT JOIN directories p ON p.root_id=e.root_id AND p.path=e.parent
 LEFT JOIN directories o ON o.root_id=e.root_id AND o.path=e.path WHERE e.root_id=?1 AND e.path=?2`

const sameSizeCargoRootQuery = `SELECT substr(CAST(r.path AS BLOB),1,4097),r.enabled,CASE WHEN r.last_error='' THEN 0 ELSE 1 END,
 CASE WHEN COALESCE(m.skip_reason,'')!='' OR COALESCE(l.skip_reason,'')!='' THEN 1 ELSE 0 END
 FROM roots r LEFT JOIN entries m ON m.root_id=r.id AND m.path=?
 LEFT JOIN entries l ON l.root_id=r.id AND l.path=? WHERE r.id=?`

type sameSizeCargoEntry struct {
	id, size, allocated, modified, changed, generation, observed          int64
	skipped, parentGeneration, parentComplete, parentChecked, parentError int64
	ownGeneration, complete, checked, ownError, parentOutcome             int64
	kind, device, inode                                                   []byte
}

// Added cursors and the one most-recent successful shared proof belong only
// to this page's transaction. Every leaf is read again; one cursor can be open.
type sameSizeCargoStage uint8

const (
	sameSizeCargoRootStage sameSizeCargoStage = iota
	sameSizeCargoMembershipStage
	sameSizeCargoCommonStage
	sameSizeCargoDebugStage
	sameSizeCargoReleaseStage
	sameSizeCargoQualificationStage
)

type sameSizeCargoRows interface {
	Next() bool
	Scan(...any) error
	Err() error
	Close() error
}
type sameSizeCargoScanner interface{ Scan(...any) error }
type sameSizeCargoDiscard struct{}

func (*sameSizeCargoDiscard) Scan(any) error { return nil }

type sameSizeCargoRequest struct {
	path, parent string
	role         int64
}
type sameSizeCargoActiveCursor struct {
	rows                    sameSizeCargoRows
	stage                   sameSizeCargoStage
	rootID                  int64
	count, reached, columns int
}
type sameSizeCargoEntryReader struct {
	tx                                 *sql.Tx
	ctx                                context.Context
	statements                         [6]*sql.Stmt
	active                             *sameSizeCargoActiveCursor
	plan                               [sameSizeCargoAncestorLimit + 1]sameSizeCargoRequest
	sealedCount                        int
	counters                           *SameSizeCargoTargetExclusion
	closeStatement, closeRootStatement func(*sql.Stmt) error
	wrapCursor                         func(sameSizeCargoStage, sameSizeCargoRows) sameSizeCargoRows
	layoutTarget                       string
	lastStage                          sameSizeCargoStage
	proof                              *sameSizeCargoProof
}

func sameSizeCargoStageSize(stage sameSizeCargoStage) int {
	switch stage {
	case sameSizeCargoRootStage:
		return 1
	case sameSizeCargoMembershipStage:
		return sameSizeCargoAncestorLimit + 1
	case sameSizeCargoCommonStage:
		return 3
	case sameSizeCargoDebugStage, sameSizeCargoReleaseStage:
		return 4
	case sameSizeCargoQualificationStage:
		return 7
	}
	return 0
}
func sameSizeCargoStageRole(stage sameSizeCargoStage) int64 {
	if stage == sameSizeCargoCommonStage || stage == sameSizeCargoDebugStage || stage == sameSizeCargoReleaseStage {
		return sameSizeCargoLayoutRole
	}
	return sameSizeCargoMemberRole
}

// Request expressions act only on bounded caller-owned arguments. All saved
// payload expressions below retain the original scalar projection and types.
// The largest shape has 517 arguments, below the portable 999-argument bound.
func sameSizeCargoBatchQuery(stage sameSizeCargoStage) (string, error) {
	if sameSizeCargoStageSize(stage) == 0 {
		return "", errors.New("invalid Cargo cursor stage")
	}
	if stage == sameSizeCargoRootStage {
		return `WITH request(ordinal,root_id) AS (VALUES(0,?3))
 SELECT request.ordinal,CASE WHEN r.id IS NULL THEN 0 ELSE 1 END,
 substr(CAST(r.path AS BLOB),1,4097),r.enabled,CASE WHEN r.last_error='' THEN 0 ELSE 1 END,
 CASE WHEN COALESCE(m.skip_reason,'')!='' OR COALESCE(l.skip_reason,'')!='' THEN 1 ELSE 0 END
 FROM request LEFT JOIN roots r ON r.id=request.root_id
 LEFT JOIN entries m ON m.root_id=r.id AND m.path=?1
 LEFT JOIN entries l ON l.root_id=r.id AND l.path=?2 ORDER BY request.ordinal`, nil
	}
	var query strings.Builder
	if stage == sameSizeCargoMembershipStage {
		query.WriteString("WITH base(root_id,leaf,count) AS (VALUES(?1,?2,?3)),geometry(ordinal,path_end,expected_parent) AS (VALUES ")
		for i := 0; i < sameSizeCargoStageSize(stage); i++ {
			if i != 0 {
				query.WriteByte(',')
			}
			query.WriteString("(" + strconv.Itoa(i) + ",?" + strconv.Itoa(4+2*i) + ",?" + strconv.Itoa(5+2*i) + ")")
		}
		query.WriteString("), requests AS (SELECT g.ordinal,CASE WHEN g.path_end=-1 THEN CAST('.' AS BLOB) ELSE substr(b.leaf,1,g.path_end) END AS path,g.expected_parent,0 AS role FROM geometry g,base b WHERE g.ordinal<b.count) ")
	} else {
		query.WriteString("WITH requests(ordinal,path,expected_parent,role) AS (VALUES ")
		for i := 0; i < sameSizeCargoStageSize(stage); i++ {
			if i != 0 {
				query.WriteByte(',')
			}
			query.WriteString("(" + strconv.Itoa(i) + ",?" + strconv.Itoa(3+2*i) + ",?" + strconv.Itoa(4+2*i) + "," + strconv.FormatInt(sameSizeCargoStageRole(stage), 10) + ")")
		}
		query.WriteString(") ")
	}
	projection := sameSizeCargoEntryQuery[len("SELECT "):strings.Index(sameSizeCargoEntryQuery, " FROM entries e")]
	projection = strings.ReplaceAll(strings.ReplaceAll(projection, "?3", "r.role"), "?4", "r.expected_parent")
	// A matched corrupt payload ID must still reach its original decoder. The
	// equality key distinguishes absence without treating NULL IDs as missing.
	query.WriteString("SELECT r.ordinal,CASE WHEN e.path IS NULL THEN 0 ELSE 1 END," + projection + " FROM requests r LEFT JOIN entries e ON e.root_id=?1 AND e.path=r.path LEFT JOIN directories p ON p.root_id=e.root_id AND p.path=e.parent LEFT JOIN directories o ON o.root_id=e.root_id AND o.path=e.path")
	if stage != sameSizeCargoMembershipStage {
		query.WriteString(" WHERE r.ordinal<?2")
	}
	query.WriteString(" ORDER BY r.ordinal")
	return query.String(), nil
}

func closeSameSizeCargoStatement(owner **sql.Stmt, closeStatement func(*sql.Stmt) error) error {
	stmt := *owner
	*owner = nil
	if stmt == nil {
		return nil
	}
	if closeStatement != nil {
		return closeStatement(stmt)
	}
	return stmt.Close()
}
func (reader *sameSizeCargoEntryReader) statement(ctx context.Context, stage sameSizeCargoStage) (*sql.Stmt, error) {
	if sameSizeCargoStageSize(stage) == 0 {
		return nil, errors.New("invalid Cargo cursor stage")
	}
	if reader.statements[stage] == nil {
		query, err := sameSizeCargoBatchQuery(stage)
		if err != nil {
			return nil, err
		}
		stmt, err := reader.tx.PrepareContext(ctx, query)
		if err != nil {
			return nil, err
		}
		reader.statements[stage] = stmt
	}
	return reader.statements[stage], nil
}
func (reader *sameSizeCargoEntryReader) rootStatement(ctx context.Context) (*sql.Stmt, error) {
	return reader.statement(ctx, sameSizeCargoRootStage)
}
func (reader *sameSizeCargoEntryReader) closeCursor(ctx context.Context) error {
	active := reader.active
	reader.active = nil
	for i := 0; i < reader.sealedCount; i++ {
		reader.plan[i] = sameSizeCargoRequest{}
	}
	reader.sealedCount = 0
	if active == nil {
		if ctx != nil {
			return ctx.Err()
		}
		return nil
	}
	reader.lastStage = active.stage
	// Never drain later headers or decode evidence after a logical refusal.
	observed := active.rows.Err()
	closed := active.rows.Close()
	var contextErr error
	if ctx != nil {
		contextErr = ctx.Err()
	}
	return errors.Join(observed, closed, contextErr)
}
func (reader *sameSizeCargoEntryReader) close() error {
	ctx := reader.ctx
	reader.proof = nil
	err := reader.closeCursor(ctx)
	for i := range reader.statements {
		closeStatement := reader.closeStatement
		if sameSizeCargoStage(i) == sameSizeCargoRootStage {
			closeStatement = reader.closeRootStatement
		}
		err = errors.Join(err, closeSameSizeCargoStatement(&reader.statements[i], closeStatement))
	}
	if ctx != nil {
		err = errors.Join(err, ctx.Err())
	}
	return err
}
func (reader *sameSizeCargoEntryReader) open(ctx context.Context, stage sameSizeCargoStage, rootID int64, leaf []byte, requests []sameSizeCargoRequest) error {
	if reader.active != nil {
		return errors.Join(errors.New("Cargo cursor crosses a stage boundary"), reader.closeCursor(ctx))
	}
	maximum := sameSizeCargoStageSize(stage)
	if maximum == 0 || len(requests) == 0 || len(requests) > maximum {
		return errors.New("invalid Cargo request count")
	}
	role := sameSizeCargoStageRole(stage)
	for _, request := range requests {
		if request.role != role || len(request.path) == 0 || len(request.path) > 4096 || len(request.parent) > 4096 {
			return errors.New("invalid Cargo request correspondence")
		}
		if stage == sameSizeCargoRootStage && (request.path != "." || request.parent != "") {
			return errors.New("invalid Cargo root request")
		}
		if stage == sameSizeCargoMembershipStage && request.path != "." && (len(request.path) > len(leaf) || request.path != string(leaf[:len(request.path)])) {
			return errors.New("invalid Cargo membership prefix")
		}
	}
	if len(leaf) > 4096 {
		return errors.New("invalid Cargo request key bound")
	}
	// Seal caller-owned correspondence before preparation or opening. Both the
	// SQL relation and the consumed-row checker use this one owned plan.
	copy(reader.plan[:], requests)
	reader.sealedCount = len(requests)
	defer func() {
		if reader.active == nil {
			for i := 0; i < reader.sealedCount; i++ {
				reader.plan[i] = sameSizeCargoRequest{}
			}
			reader.sealedCount = 0
		}
	}()
	stmt, err := reader.statement(ctx, stage)
	if err != nil {
		return err
	}
	var args []any
	switch stage {
	case sameSizeCargoRootStage:
		args = []any{[]byte(filepath.Join(filepath.Dir(string(leaf)), "Cargo.toml")), []byte(filepath.Join(filepath.Dir(string(leaf)), "Cargo.lock")), rootID}
	case sameSizeCargoMembershipStage:
		args = make([]any, 3+2*maximum)
		args[0], args[1], args[2] = rootID, leaf, reader.sealedCount
		for i := 0; i < maximum; i++ {
			end, parent := 0, ""
			if i < reader.sealedCount {
				end, parent = len(reader.plan[i].path), reader.plan[i].parent
				if reader.plan[i].path == "." {
					end = -1
				}
			}
			args[3+2*i], args[4+2*i] = end, parent
		}
	default:
		args = make([]any, 2+2*maximum)
		args[0], args[1] = rootID, reader.sealedCount
		for i := 0; i < maximum; i++ {
			path, parent := []byte{}, ""
			if i < reader.sealedCount {
				path, parent = []byte(reader.plan[i].path), reader.plan[i].parent
			}
			args[2+2*i], args[3+2*i] = path, parent
		}
	}
	if reader.counters != nil {
		switch stage {
		case sameSizeCargoRootStage:
			reader.counters.RootQueryCalls++
		case sameSizeCargoMembershipStage:
			reader.counters.MembershipBatchQueryCalls++
		case sameSizeCargoQualificationStage:
			reader.counters.MarkerQualificationBatchQueryCalls++
		default:
			reader.counters.LayoutBatchQueryCalls++
		}
	}
	// Count exactly this physical QueryContext attempt, not request rows or preparation.
	rows, err := stmt.QueryContext(ctx, args...)
	if err != nil {
		return err
	}
	var cursor sameSizeCargoRows = rows
	if reader.wrapCursor != nil {
		cursor = reader.wrapCursor(stage, cursor)
	}
	if cursor == nil {
		return errors.Join(errors.New("Cargo cursor wrapper returned nil"), rows.Close(), ctx.Err())
	}
	columns := 26
	if stage == sameSizeCargoRootStage {
		columns = 4
	}
	reader.ctx = ctx
	reader.active = &sameSizeCargoActiveCursor{rows: cursor, stage: stage, rootID: rootID, count: reader.sealedCount, columns: columns}
	return nil
}
func (reader *sameSizeCargoEntryReader) next(path string, role int64, parent string) (sameSizeCargoScanner, bool, error) {
	active := reader.active
	if active == nil {
		return nil, false, errors.New("Cargo request outside sealed plan")
	}
	if active.reached >= active.count {
		return nil, false, errors.Join(errors.New("Cargo request outside sealed plan"), reader.closeCursor(reader.ctx))
	}
	request := reader.plan[active.reached]
	if request.path != path || request.role != role || request.parent != parent {
		return nil, false, errors.Join(errors.New("Cargo callback differs from sealed request"), reader.closeCursor(reader.ctx))
	}
	if !active.rows.Next() {
		return nil, false, errors.Join(errors.New("Cargo transport row missing"), reader.closeCursor(reader.ctx))
	}
	var ordinal, presence any
	var discard sameSizeCargoDiscard
	var destinations [28]any
	destinations[0], destinations[1] = &ordinal, &presence
	for i := 0; i < active.columns; i++ {
		destinations[2+i] = &discard
	}
	if err := active.rows.Scan(destinations[:active.columns+2]...); err != nil {
		return nil, false, errors.Join(err, reader.closeCursor(reader.ctx))
	}
	order, ok := ordinal.(int64)
	flag, valid := presence.(int64)
	if !ok || !valid || order != int64(active.reached) || (flag != 0 && flag != 1) {
		return nil, false, errors.Join(errors.New("invalid Cargo transport header"), reader.closeCursor(reader.ctx))
	}
	active.reached++
	return sameSizeCargoPayloadRow{rows: active.rows, columns: active.columns}, flag == 1, nil
}

type sameSizeCargoPayloadRow struct {
	rows    sameSizeCargoRows
	columns int
}

func (row sameSizeCargoPayloadRow) Scan(destinations ...any) error {
	if len(destinations) > row.columns {
		return errors.New("invalid Cargo payload width")
	}
	var values [28]any
	var discard sameSizeCargoDiscard
	values[0], values[1] = &discard, &discard
	copy(values[2:], destinations)
	for i := len(destinations); i < row.columns; i++ {
		values[2+i] = &discard
	}
	return row.rows.Scan(values[:row.columns+2]...)
}
func (reader *sameSizeCargoEntryReader) finish(ctx context.Context) error {
	active := reader.active
	if active == nil {
		return errors.New("Cargo cursor missing at full consumption")
	}
	if active.reached != active.count {
		return errors.Join(errors.New("Cargo plan not fully consumed"), reader.closeCursor(ctx))
	}
	var err error
	if active.rows.Next() {
		err = errors.New("extra Cargo transport row")
	}
	return errors.Join(err, reader.closeCursor(ctx))
}
func sameSizeCargoExpectedParent(path string) string {
	if path == "." {
		return ""
	}
	return filepath.Dir(path)
}
func (reader *sameSizeCargoEntryReader) readRoot(ctx context.Context, rootID int64, target string) ([]byte, any, any, any, bool, error) {
	request := sameSizeCargoRequest{path: ".", role: sameSizeCargoMemberRole}
	if err := reader.open(ctx, sameSizeCargoRootStage, rootID, []byte(target), []sameSizeCargoRequest{request}); err != nil {
		return nil, nil, nil, nil, false, err
	}
	row, exists, err := reader.next(".", sameSizeCargoMemberRole, "")
	if err != nil {
		return nil, nil, nil, nil, false, err
	}
	if !exists {
		return nil, nil, nil, nil, false, reader.closeCursor(ctx)
	}
	var root []byte
	var enabled, bad, skipped any
	if err = row.Scan(&root, &enabled, &bad, &skipped); err != nil {
		return nil, nil, nil, nil, false, errors.Join(err, reader.closeCursor(ctx))
	}
	err = reader.finish(ctx)
	return root, enabled, bad, skipped, err == nil, err
}
func (reader *sameSizeCargoEntryReader) readLayout(ctx context.Context, _ *sql.Tx, rootID int64, path string) (buildOutputEntry, bool, error) {
	singleton := reader.layoutTarget == ""
	if reader.active != nil && reader.active.rootID != rootID {
		return buildOutputEntry{}, false, errors.Join(errors.New("Cargo callback root differs from sealed request"), reader.closeCursor(ctx))
	}
	if reader.active == nil {
		stage := sameSizeCargoCommonStage
		var requests [7]sameSizeCargoRequest
		count := 1
		requests[0] = sameSizeCargoRequest{path: path, parent: sameSizeCargoExpectedParent(path), role: sameSizeCargoLayoutRole}
		if !singleton {
			switch reader.lastStage {
			case sameSizeCargoMembershipStage:
				keys := []string{reader.layoutTarget, filepath.Join(filepath.Dir(reader.layoutTarget), "Cargo.toml"), filepath.Join(filepath.Dir(reader.layoutTarget), "Cargo.lock")}
				count = len(keys)
				for i, key := range keys {
					requests[i] = sameSizeCargoRequest{path: key, parent: sameSizeCargoExpectedParent(key), role: sameSizeCargoLayoutRole}
				}
			case sameSizeCargoCommonStage, sameSizeCargoDebugStage:
				profile := "debug"
				stage = sameSizeCargoDebugStage
				if reader.lastStage == sameSizeCargoDebugStage {
					profile = "release"
					stage = sameSizeCargoReleaseStage
				}
				prefix := filepath.Join(reader.layoutTarget, profile)
				keys := []string{prefix, filepath.Join(prefix, ".cargo-lock"), filepath.Join(prefix, "deps"), filepath.Join(prefix, ".fingerprint")}
				count = len(keys)
				for i, key := range keys {
					requests[i] = sameSizeCargoRequest{path: key, parent: sameSizeCargoExpectedParent(key), role: sameSizeCargoLayoutRole}
				}
			default:
				return buildOutputEntry{}, false, errors.New("Cargo layout stage out of order")
			}
		}
		if err := reader.open(ctx, stage, rootID, nil, requests[:count]); err != nil {
			return buildOutputEntry{}, false, err
		}
	}
	if reader.counters != nil {
		reader.counters.LayoutMarkerEvidenceRequests++
	}
	row, exists, err := reader.next(path, sameSizeCargoLayoutRole, sameSizeCargoExpectedParent(path))
	if err != nil {
		return buildOutputEntry{}, false, err
	}
	var entry buildOutputEntry
	if exists {
		entry, exists, err = scanBuildOutputEntry(row)
	}
	if err != nil {
		return entry, false, errors.Join(err, reader.closeCursor(ctx))
	}
	// All three/four callbacks are requested before the unchanged recognizer
	// evaluates that complete group's evidence. No branch is prefetched.
	if singleton && !exists {
		err = reader.closeCursor(ctx)
	} else if reader.active.reached == reader.active.count {
		err = reader.finish(ctx)
	}
	return entry, exists && err == nil, err
}

// These destinations validate transport headers before any member payload
// conversion. They never coerce stored text, real, NULL or boolean values.
type sameSizeCargoOrdinalScanner struct {
	expected int64
	admitted bool
}

func (header *sameSizeCargoOrdinalScanner) Scan(value any) error {
	ordinal, ok := value.(int64)
	if !ok || ordinal != header.expected {
		return errors.New("invalid Cargo transport header")
	}
	header.admitted = true
	return nil
}

type sameSizeCargoPresenceScanner struct {
	stop             *sameSizeCargoAbsenceStop
	admitted, absent bool
}

func (header *sameSizeCargoPresenceScanner) Scan(value any) error {
	presence, ok := value.(int64)
	if !ok || (presence != 0 && presence != 1) {
		return errors.New("invalid Cargo transport header")
	}
	header.admitted = true
	if presence == 0 {
		header.absent = true
		return header.stop
	}
	return nil
}

// Nonzero size gives each row its own stop identity. No global ErrNoRows or
// foreign stop can normalize an unrelated Scan error into logical absence.
type sameSizeCargoAbsenceStop struct{ marker byte }

func (*sameSizeCargoAbsenceStop) Error() string { return "saved Cargo entry absent" }
func sameSizeCargoOnlyAbsenceStop(err error, stop *sameSizeCargoAbsenceStop) bool {
	if stop == nil {
		return false
	}
	// database/sql adds one wrapping link. Refuse unexpected deep or cyclic
	// chains conservatively; do not call errors.Is or parse driver strings.
	for depth := 0; depth < 16 && err != nil; depth++ {
		if err == stop {
			return true
		}
		if _, joined := err.(interface{ Unwrap() []error }); joined {
			return false
		}
		wrapper, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = wrapper.Unwrap()
	}
	return false
}

type sameSizeCargoMemberRow struct {
	reader            *sameSizeCargoEntryReader
	path, parent      string
	stop              sameSizeCargoAbsenceStop
	normalizedAbsence bool
}

func (row *sameSizeCargoMemberRow) Scan(destinations ...any) error {
	reader := row.reader
	active := reader.active
	if active == nil {
		return errors.New("Cargo request outside sealed plan")
	}
	if (active.stage != sameSizeCargoMembershipStage && active.stage != sameSizeCargoQualificationStage) || active.reached >= active.count || len(destinations) != 26 {
		return errors.Join(errors.New("invalid Cargo member cursor scope"), reader.closeCursor(reader.ctx))
	}
	request := reader.plan[active.reached]
	if request.path != row.path || request.role != sameSizeCargoMemberRole || request.parent != row.parent {
		return errors.Join(errors.New("Cargo callback differs from sealed request"), reader.closeCursor(reader.ctx))
	}
	if !active.rows.Next() {
		return errors.Join(errors.New("Cargo transport row missing"), reader.closeCursor(reader.ctx))
	}
	ordinal := sameSizeCargoOrdinalScanner{expected: int64(active.reached)}
	presence := sameSizeCargoPresenceScanner{stop: &row.stop}
	var values [28]any
	values[0], values[1] = &ordinal, &presence
	copy(values[2:], destinations)
	// This is the only production Scan for this member/qualification row.
	// The original payload destinations run only after both strict headers.
	err := active.rows.Scan(values[:]...)
	if ordinal.admitted && presence.admitted {
		active.reached++
	}
	if ordinal.admitted && presence.admitted && presence.absent && sameSizeCargoOnlyAbsenceStop(err, &row.stop) {
		row.normalizedAbsence = true
		return sql.ErrNoRows
	}
	return err
}

func readSameSizeCargoEntry(ctx context.Context, reader *sameSizeCargoEntryReader, rootID int64, path string) (sameSizeCargoEntry, bool, error) {
	singleton := reader.active == nil
	if reader.active != nil && reader.active.rootID != rootID {
		return sameSizeCargoEntry{}, false, errors.Join(errors.New("Cargo callback root differs from sealed request"), reader.closeCursor(ctx))
	}
	expectedParent := sameSizeCargoExpectedParent(path)
	if singleton {
		request := sameSizeCargoRequest{path: path, parent: expectedParent, role: sameSizeCargoMemberRole}
		if err := reader.open(ctx, sameSizeCargoMembershipStage, rootID, []byte(path), []sameSizeCargoRequest{request}); err != nil {
			return sameSizeCargoEntry{}, false, err
		}
	}
	row := sameSizeCargoMemberRow{reader: reader, path: path, parent: expectedParent}
	entry, err := scanSameSizeCargoEntry(&row)
	exists := err == nil
	if row.normalizedAbsence && err == sql.ErrNoRows {
		err = nil
		exists = false
	}
	if err != nil {
		return entry, false, errors.Join(err, reader.closeCursor(ctx))
	}
	if singleton {
		if exists {
			err = reader.finish(ctx)
		} else {
			err = reader.closeCursor(ctx)
		}
	}
	return entry, exists && err == nil, err
}
func scanSameSizeCargoEntry(row sameSizeCargoScanner) (sameSizeCargoEntry, error) {
	var e sameSizeCargoEntry
	var values [16]any
	var parentOutcome any
	var unused [6]any
	err := row.Scan(&values[0], &unused[0], &unused[1], &unused[2], &unused[3],
		&values[3], &values[4], &values[5], &values[6], &unused[4], &values[8], &values[9], &unused[5],
		&parentOutcome, &e.kind, &e.device, &e.inode, &values[1], &values[2], &values[7], &values[10], &values[11],
		&values[12], &values[13], &values[14], &values[15])
	if err != nil {
		return e, err
	}
	fields := []*int64{&e.id, &e.size, &e.allocated, &e.modified, &e.changed, &e.generation, &e.observed, &e.skipped,
		&e.parentGeneration, &e.parentComplete, &e.parentChecked, &e.parentError, &e.ownGeneration, &e.complete, &e.checked, &e.ownError}
	for i, value := range values {
		if *fields[i], err = sameSizeCargoInt(value); err != nil {
			return e, err
		}
	}
	if e.parentOutcome, err = sameSizeCargoInt(parentOutcome); err != nil {
		return e, err
	}
	if e.parentOutcome < 0 || e.parentOutcome > 2 {
		return e, errors.New("invalid saved Cargo parent outcome")
	}
	return e, nil
}

type sameSizeCargoIdentity struct {
	device, inode                uint64
	changed, modified, allocated int64
	kind                         string
}

type sameSizeCargoIdentities struct {
	values [sameSizeCargoAncestorLimit + 8]sameSizeCargoIdentity
	count  int
}

func (seen *sameSizeCargoIdentities) add(e sameSizeCargoEntry) bool {
	if !sameSizeIdentityNumber(string(e.device), false) || !sameSizeIdentityNumber(string(e.inode), true) || e.changed <= 0 || e.generation <= 0 || e.size < 0 || e.allocated < 0 {
		return false
	}
	d, _ := strconv.ParseUint(string(e.device), 10, 64)
	i, _ := strconv.ParseUint(string(e.inode), 10, 64)
	x := sameSizeCargoIdentity{d, i, e.changed, e.modified, e.allocated, string(e.kind)}
	for _, prior := range seen.values[:seen.count] {
		if prior.device == d && prior.inode == i {
			return prior == x
		}
	}
	if seen.count == len(seen.values) {
		return false
	}
	seen.values[seen.count], seen.count = x, seen.count+1
	return true
}

// One successful proof can be reused only inside its reader's snapshot. The
// shared identities exclude the cold leaf; no later leaf is added to the proof.
type sameSizeCargoProof struct {
	rootID                                                  int64
	root, parent, target                                    string
	shared                                                  sameSizeCargoIdentities
	firstDemand                                             [sameSizeCargoAncestorLimit + 8]uint16
	finding                                                 BuildOutputFinding
	ancestorRequests, layoutRequests, qualificationRequests int
}

// The saved-layout recognizer creates each PathBytes from its exact Path
// string. Keep that immutable string once in the proof, without a duplicate
// byte body. Marker and action containers remain independently owned. The
// recognizer supplies no measurement or reclaimable pointers.
func sameSizeCargoFreezeFinding(finding BuildOutputFinding) BuildOutputFinding {
	frozen := finding
	frozen.PathBytes = nil
	frozen.Markers = make([]BuildOutputMarker, len(finding.Markers))
	copy(frozen.Markers, finding.Markers)
	for i := range frozen.Markers {
		frozen.Markers[i].PathBytes = nil
	}
	if finding.Actions != nil {
		frozen.Actions = make([]string, len(finding.Actions))
		copy(frozen.Actions, finding.Actions)
	}
	return frozen
}

// Each observer gets separate mutable bytes for the finding and every marker.
// String-to-byte conversion preserves raw path bytes without text encoding.
func sameSizeCargoThawFinding(frozen BuildOutputFinding) BuildOutputFinding {
	working := sameSizeCargoFreezeFinding(frozen)
	working.PathBytes = []byte(frozen.Path)
	for i := range working.Markers {
		working.Markers[i].PathBytes = []byte(frozen.Markers[i].Path)
	}
	return working
}

// add never appends a repeated device/inode pair, so the copied shared proof
// contains unique pairs. Each replay starts with only this row's fresh leaf.
// Earlier shared tuples cannot collide with later ones; compare each tuple
// only with that leaf, preserving identity order and the first failed demand.
func sameSizeCargoReplayIdentities(seen *sameSizeCargoIdentities, proof *sameSizeCargoProof) (uint16, bool) {
	leaf := seen.values[0]
	for index, identity := range proof.shared.values[:proof.shared.count] {
		if leaf.device == identity.device && leaf.inode == identity.inode {
			if leaf != identity {
				return proof.firstDemand[index], false
			}
			continue
		}
		if seen.count == len(seen.values) {
			return proof.firstDemand[index], false
		}
		seen.values[seen.count], seen.count = identity, seen.count+1
	}
	return 0, true
}

func sameSizeCargoReplayProof(ctx context.Context, entries *sameSizeCargoEntryReader, raw sameSizeRawFile, proof *sameSizeCargoProof, counters *SameSizeCargoTargetExclusion, hooks sameSizeCargoHooks) (bool, error) {
	leafPath := string(raw.relative)
	request := sameSizeCargoRequest{path: leafPath, parent: sameSizeCargoExpectedParent(leafPath), role: sameSizeCargoMemberRole}
	if err := entries.open(ctx, sameSizeCargoMembershipStage, raw.file.RootID, raw.relative, []sameSizeCargoRequest{request}); err != nil {
		return false, err
	}
	var seen sameSizeCargoIdentities
	counters.LeafEvidenceRequests++
	leaf, exists, err := readSameSizeCargoEntry(ctx, entries, raw.file.RootID, leafPath)
	if err != nil {
		return false, err
	}
	f := raw.file
	if !exists || !sameSizeCargoMembership(leaf, leafPath, "file", &seen) || leaf.id != f.ID || leaf.size != f.Size || leaf.allocated != f.Allocated || leaf.changed != f.ChangedNS || leaf.generation != f.Generation || leaf.observed != f.ObservedAt.UnixNano() || leaf.modified != f.ModifiedAt.UnixNano() || string(leaf.device) != f.Device || string(leaf.inode) != f.Inode {
		return false, nil
	}
	if err = entries.finish(ctx); err != nil {
		return false, err
	}
	if err = ctx.Err(); err != nil {
		return false, err
	}
	demand, matches := sameSizeCargoReplayIdentities(&seen, proof)
	if !matches {
		if demand <= sameSizeCargoAncestorLimit {
			counters.AncestorEvidenceRequests += int(demand)
		} else {
			counters.AncestorEvidenceRequests += proof.ancestorRequests
			counters.LayoutMarkerEvidenceRequests += proof.layoutRequests
			counters.MarkerQualificationEvidenceRequests += int(demand) - sameSizeCargoAncestorLimit
		}
		return false, ctx.Err()
	}
	counters.AncestorEvidenceRequests += proof.ancestorRequests
	counters.LayoutMarkerEvidenceRequests += proof.layoutRequests
	counters.MarkerQualificationEvidenceRequests += proof.qualificationRequests
	counters.ReusedProofRows++
	if err = ctx.Err(); err != nil {
		return false, err
	}
	if hooks.beforeExcluded != nil {
		finding := sameSizeCargoThawFinding(proof.finding)
		hooks.beforeExcluded(&seen, &finding)
	}
	if err = ctx.Err(); err != nil {
		return false, err
	}
	counters.ExcludedRows++
	return true, nil
}

func sameSizeCargoMembership(e sameSizeCargoEntry, relative, kind string, seen *sameSizeCargoIdentities) bool {
	if e.id <= 0 || string(e.kind) != kind || e.parentOutcome == 0 || e.skipped != 0 || e.observed <= 0 || !seen.add(e) {
		return false
	}
	if relative == "." {
		if e.parentOutcome != 2 {
			return false
		}
	} else if e.parentOutcome != 2 || e.parentGeneration <= 0 || e.generation != e.parentGeneration || e.parentComplete != 1 || e.parentError != 0 || e.parentChecked <= 0 || e.observed > e.parentChecked {
		return false
	}
	return kind != "directory" || e.ownGeneration > 0 && e.complete == 1 && e.ownError == 0 && e.checked > 0 && e.observed <= e.checked
}

func sameSizeCargoTarget(root, relative []byte) (string, bool) {
	parent := filepath.Join(string(root), filepath.Dir(string(relative)))
	parts := strings.Split(parent, string(filepath.Separator))
	for i, part := range parts {
		if part != "target" {
			continue
		}
		absolute := string(filepath.Separator) + filepath.Join(parts[:i+1]...)
		target, err := filepath.Rel(string(root), absolute)
		if err != nil || target == "." || !validRelative([]byte(target)) {
			return "", true
		}
		return target, true
	}
	return "", false
}

func sameSizeCargoMarkerPaths(root []byte, target string) bool {
	if !validRelative([]byte(target)) || target == "." {
		return false
	}
	for _, suffix := range []string{"", "../Cargo.toml", "../Cargo.lock", "debug", "debug/.cargo-lock", "debug/deps", "debug/.fingerprint", "release", "release/.cargo-lock", "release/deps", "release/.fingerprint"} {
		rel := filepath.Join(target, suffix)
		if !validRelative([]byte(rel)) || len(filepath.Join(string(root), rel)) > 4096 {
			return false
		}
	}
	return true
}

// A negative outcome keeps the ordinary row. Missing or uncertain saved
// evidence is not a source-access request or a new cleanup classification.
func sameSizeCargoExcluded(ctx context.Context, tx *sql.Tx, entries *sameSizeCargoEntryReader, raw sameSizeRawFile, counters *SameSizeCargoTargetExclusion, hooks sameSizeCargoHooks) (excluded bool, err error) {
	defer func() {
		err = errors.Join(err, entries.closeCursor(ctx))
		entries.layoutTarget = ""
		if err != nil {
			excluded = false
		}
	}()
	counters.EligibleRows++
	target, found := sameSizeCargoTarget(raw.root, raw.relative)
	if !found {
		return false, nil
	}
	counters.RowsWithTargetAncestor++
	unrecognized := func() (bool, error) { counters.RetainedUnrecognizedRows++; return false, nil }
	unknown := func() (bool, error) { counters.RetainedUnknownRows++; return false, nil }
	if target == "" {
		return unrecognized()
	}
	if !sameSizeCargoMarkerPaths(raw.root, target) {
		return unknown()
	}
	counters.RootEvidenceRequests++
	if proof := entries.proof; !hooks.disableProofReuse && proof != nil && proof.rootID == raw.file.RootID && proof.root == string(raw.root) && proof.parent == filepath.Dir(string(raw.relative)) && proof.target == target {
		reused, readErr := sameSizeCargoReplayProof(ctx, entries, raw, proof, counters, hooks)
		if readErr != nil {
			return false, readErr
		}
		if !reused {
			return unknown()
		}
		return true, nil
	}
	ancestorStart, layoutStart, qualificationStart := counters.AncestorEvidenceRequests, counters.LayoutMarkerEvidenceRequests, counters.MarkerQualificationEvidenceRequests
	root, enabled, bad, skippedCommon, exists, err := entries.readRoot(ctx, raw.file.RootID, target)
	if err != nil {
		return false, err
	}
	if !exists {
		return unknown()
	}
	en, err := sameSizeCargoInt(enabled)
	if err != nil {
		return false, err
	}
	rootBad, err := sameSizeCargoInt(bad)
	if err != nil {
		return false, err
	}
	commonSkipped, err := sameSizeCargoInt(skippedCommon)
	if err != nil {
		return false, err
	}
	if en != 1 || rootBad != 0 || !bytes.Equal(root, raw.root) || !canonicalPath(root) {
		return unknown()
	}
	// Every ancestor key is a prefix view of one leaf string. The dot key is
	// explicit; no full ancestor path copy or decoded cache is retained.
	var requests [sameSizeCargoAncestorLimit + 1]sameSizeCargoRequest
	leafPath := string(raw.relative)
	requests[0] = sameSizeCargoRequest{path: leafPath, parent: sameSizeCargoExpectedParent(leafPath), role: sameSizeCargoMemberRole}
	count := 1
	relative := filepath.Dir(leafPath)
	for count <= sameSizeCargoAncestorLimit {
		requests[count] = sameSizeCargoRequest{path: relative, parent: sameSizeCargoExpectedParent(relative), role: sameSizeCargoMemberRole}
		count++
		if relative == "." {
			break
		}
		relative = filepath.Dir(relative)
	}
	if err = entries.open(ctx, sameSizeCargoMembershipStage, raw.file.RootID, raw.relative, requests[:count]); err != nil {
		return false, err
	}
	var seen sameSizeCargoIdentities
	var firstDemand [sameSizeCargoAncestorLimit + 8]uint16
	counters.LeafEvidenceRequests++
	leaf, exists, err := readSameSizeCargoEntry(ctx, entries, raw.file.RootID, leafPath)
	if err != nil {
		return false, err
	}
	f := raw.file
	if !exists || !sameSizeCargoMembership(leaf, leafPath, "file", &seen) || leaf.id != f.ID || leaf.size != f.Size || leaf.allocated != f.Allocated || leaf.changed != f.ChangedNS || leaf.generation != f.Generation || leaf.observed != f.ObservedAt.UnixNano() || leaf.modified != f.ModifiedAt.UnixNano() || string(leaf.device) != f.Device || string(leaf.inode) != f.Inode {
		return unknown()
	}
	var targetID int64
	relative = filepath.Dir(leafPath)
	for depth := 0; ; depth++ {
		if depth == sameSizeCargoAncestorLimit {
			counters.RetainedAncestorLimitRows++
			return false, nil
		}
		counters.AncestorEvidenceRequests++
		e, ok, readErr := readSameSizeCargoEntry(ctx, entries, f.RootID, relative)
		if readErr != nil {
			return false, readErr
		}
		previousCount := seen.count
		if !ok || !sameSizeCargoMembership(e, relative, "directory", &seen) {
			return unknown()
		}
		if seen.count > previousCount {
			firstDemand[previousCount] = uint16(depth + 1)
		}
		if relative == target {
			targetID = e.id
		}
		if relative == "." {
			break
		}
		relative = filepath.Dir(relative)
	}
	if err = entries.finish(ctx); err != nil {
		return false, err
	}
	if targetID <= 0 {
		return unknown()
	}
	entries.layoutTarget = target
	finding, code, err := readBuildOutputFindingWithReader(ctx, tx, f.RootID, targetID, root, []byte(target), math.MaxInt64, entries.readLayout)
	if err != nil {
		return false, err
	}
	// Root's common-skip observation has already been strictly decoded. Layout
	// evidence counters are incremented at callbacks, never inferred from code.
	_ = commonSkipped
	if code != buildSelected {
		switch code {
		case buildSkipped, buildUnsupportedPath, buildParentIncomplete, buildParentUnconfirmed, buildTimestampUnknown:
			return unknown()
		}
		return unrecognized()
	}
	if len(finding.Markers) != 7 {
		return unknown()
	}
	var markers [7]sameSizeCargoRequest
	for i, marker := range finding.Markers {
		rel, relErr := filepath.Rel(string(root), string(marker.PathBytes))
		if relErr != nil || !validRelative([]byte(rel)) || len(marker.PathBytes) > 4096 {
			return unknown()
		}
		markers[i] = sameSizeCargoRequest{path: rel, parent: sameSizeCargoExpectedParent(rel), role: sameSizeCargoMemberRole}
	}
	if err = entries.open(ctx, sameSizeCargoQualificationStage, f.RootID, nil, markers[:]); err != nil {
		return false, err
	}
	for i, marker := range finding.Markers {
		rel := markers[i].path
		counters.MarkerQualificationEvidenceRequests++
		e, ok, readErr := readSameSizeCargoEntry(ctx, entries, f.RootID, rel)
		if readErr != nil {
			return false, readErr
		}
		previousCount := seen.count
		if !ok || !sameSizeCargoMembership(e, rel, marker.Kind, &seen) || e.id != marker.EntryID || string(e.device) != marker.Device || string(e.inode) != marker.Inode || e.changed != marker.CtimeNS || e.generation != marker.Generation || e.parentGeneration != marker.ParentGeneration || e.modified != marker.ModifiedAt.UnixNano() || e.observed != marker.ObservedAt.UnixNano() {
			return unknown()
		}
		if seen.count > previousCount {
			firstDemand[previousCount] = uint16(sameSizeCargoAncestorLimit + i + 1)
		}
	}
	if err = entries.finish(ctx); err != nil {
		return false, err
	}
	if err = ctx.Err(); err != nil {
		return false, err
	}
	// Removing the first identity is safe only if it belongs solely to the
	// cold leaf. Ancestor aliases cannot qualify with the leaf's file kind;
	// explicitly rule out the seven marker aliases before caching this proof.
	reusable := true
	for _, marker := range finding.Markers {
		if marker.Device == string(leaf.device) && marker.Inode == string(leaf.inode) {
			reusable = false
			break
		}
	}
	if reusable && !hooks.disableProofReuse {
		proof := &sameSizeCargoProof{rootID: f.RootID, root: string(raw.root), parent: filepath.Dir(leafPath), target: target,
			finding: sameSizeCargoFreezeFinding(finding), ancestorRequests: counters.AncestorEvidenceRequests - ancestorStart,
			layoutRequests: counters.LayoutMarkerEvidenceRequests - layoutStart, qualificationRequests: counters.MarkerQualificationEvidenceRequests - qualificationStart}
		proof.shared.count = seen.count - 1
		copy(proof.shared.values[:], seen.values[1:seen.count])
		copy(proof.firstDemand[:], firstDemand[1:seen.count])
		// Publish before the checkpoint so the retained cache is included in
		// its heap observation. The hook receives separate working storage.
		entries.proof = proof
	}
	if hooks.beforeExcluded != nil {
		hooks.beforeExcluded(&seen, &finding)
	}
	counters.ExcludedRows++
	return true, nil
}

func (s *Store) sameSizeCargoCandidates(ctx context.Context, limit int, token string, minimumBytes int64, hooks sameSizeCargoHooks) (result SameSizeReport, err error) {
	defer func() {
		if err != nil {
			result = SameSizeReport{}
		}
	}()
	r := SameSizeReport{GeneratedAt: time.Now().UTC(), Source: "saved_inventory", MinimumBytes: minimumBytes, Limit: limit,
		Bands: []SameSizeBand{}, PageCoverage: "saved_entries_exhausted", Diagnostics: sameSizeDiagnostics(), Notes: []string{
			"Same-size files can have different contents. No file contents, samples or hashes have been read; this report cannot establish duplicates or authorize cleanup.",
			"Only ordinary saved regular files at or above the minimum are examined. Disabled roots, saved skips, node_modules trees and compacted parents are excluded. Exclusion diagnostics count one first-match outcome per examined entry.",
			"Each page uses one saved inventory snapshot. Scans can change sizes, ordering and membership between pages; the cursor binds the inventory incarnation and minimum, not a frozen export.",
			"Bands and object counts cover this page only. Continuation flags describe raw size bands before exclusions; a boundary singleton is retained because its size band crosses a page boundary. Complete singleton bands are omitted.",
			"Known objects use canonical saved device/inode numbers with positive ctime and scan generation. Repeated saved objects are additional paths to that saved identity. Differing saved ctime, modification time or allocation marks an identity conflict; scan generations alone do not identify inode incarnations.",
			"Saved parent-pass labels describe direct-parent observations only. Current availability, ancestor freshness and file contents have not been verified.",
			"Logical sizes, allocation and saved aliases do not establish storage you can free. Hardlinks, clones and snapshots can retain storage. Do not sum counts or sizes across pages.",
			"Exhausted saved entries do not prove scanning is complete or that no duplicates exist. Files below the minimum and complete singleton size bands are outside this report.",
		}}
	if limit < 1 || limit > 200 {
		return r, ErrSameSizeLimit
	}
	if minimumBytes <= 0 {
		return r, ErrSameSizeMinimum
	}
	if s.schema < 9 {
		return r, ErrSameSizeSchema
	}
	r.CargoTargetExclusion = &SameSizeCargoTargetExclusion{Policy: SameSizeCargoPolicy, Recognition: "saved_layout_metadata_only"}
	r.Diagnostics = append(r.Diagnostics, SelectionDiagnostic{Code: "cargo_default_target_excluded", Explanation: "Path is below a supported saved Cargo default target layout; unique or manual files can be hidden."})
	r.Notes = append(r.Notes, "Opt-in Cargo filter uses saved layout metadata only, with no age cutoff. It can hide unique or manually edited files; it does not prove generated-only content, producer, regeneration, current state or safe cleanup. Empty pages can continue with the same filter.")
	cursor, err := decodeSameSizeCargoCursor(token)
	if err != nil {
		return r, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return r, err
	}
	defer func() {
		if closeErr := tx.Rollback(); closeErr != nil && !errors.Is(closeErr, sql.ErrTxDone) {
			err = errors.Join(err, closeErr)
		}
		if err != nil {
			result = SameSizeReport{}
		}
	}()
	entries := sameSizeCargoEntryReader{tx: tx, ctx: ctx, counters: r.CargoTargetExclusion, closeStatement: hooks.closeEntryStatement,
		closeRootStatement: hooks.closeRootStatement, wrapCursor: hooks.wrapCursor}
	// Registered after rollback so every early return closes all statements
	// first. A close or cancellation error vetoes the complete report.
	defer func() {
		err = errors.Join(err, entries.close(), ctx.Err())
		if err != nil {
			result = SameSizeReport{}
		}
	}()
	if err = tx.QueryRowContext(ctx, "SELECT token FROM inventory_identity WHERE singleton=1").Scan(&r.InventoryID); err != nil {
		return r, err
	}
	if !sameSizeInventoryID(r.InventoryID) {
		return r, errors.New("saved inventory incarnation is invalid")
	}
	if token != "" && (cursor.InventoryID != r.InventoryID || cursor.MinimumBytes != minimumBytes) {
		return r, ErrReportCursor
	}
	if hooks.afterSnapshot != nil {
		hooks.afterSnapshot()
	}
	var raw []sameSizeRawFile
	if token == "" {
		raw, err = readSameSizeCargoRows(ctx, tx, sameSizeInitialQuery, minimumBytes, limit+1)
	} else {
		// A row-value (size,id) range may seek only size and walk all consumed
		// IDs again. Equality on size enables the existing index's rowid seek.
		raw, err = readSameSizeCargoRows(ctx, tx, sameSizeContinueQuery, cursor.Size, cursor.ID, limit+1)
		if err == nil && len(raw) < limit+1 {
			var lower []sameSizeRawFile
			lower, err = readSameSizeCargoRows(ctx, tx, sameSizeLowerQuery, minimumBytes, cursor.Size, limit+1-len(raw))
			raw = append(raw, lower...)
		}
	}
	if err != nil {
		return r, err
	}
	processed := raw
	if len(raw) > limit {
		processed = raw[:limit]
		last := processed[len(processed)-1].file
		payload, _ := json.Marshal(sameSizeCargoCursor{Version: 2, Policy: SameSizeCargoPolicy, InventoryID: r.InventoryID, MinimumBytes: minimumBytes, Size: last.Size, ID: last.ID})
		r.NextCursor = base64.RawURLEncoding.EncodeToString(payload)
		r.PageCoverage = "more_saved_entries"
	}
	r.EntriesExamined = len(processed)
	if hooks.afterRawPage != nil {
		hooks.afterRawPage()
	}
	var bands []SameSizeBand
	for _, entry := range processed {
		outcome := 5
		switch {
		case entry.enabled != 1:
			outcome = 0
		case entry.skipped != 0:
			outcome = 1
		case sameSizeGeneratedParent(entry.root, entry.relative):
			outcome = 2
		case entry.compacted != 0:
			outcome = 3
		case !canonicalPath(entry.root) || !validRelative(entry.relative) || string(entry.relative) == "." || len(entry.file.PathBytes) > 4096:
			outcome = 4
		}
		if outcome == 5 {
			excluded, checkErr := sameSizeCargoExcluded(ctx, tx, &entries, entry, r.CargoTargetExclusion, hooks)
			if checkErr != nil {
				return SameSizeReport{}, checkErr
			}
			if excluded {
				outcome = 6
			}
		}
		r.Diagnostics[outcome].Count++
		if outcome != 5 {
			continue
		}
		if len(bands) == 0 || bands[len(bands)-1].LogicalBytes != entry.file.Size {
			bands = append(bands, SameSizeBand{LogicalBytes: entry.file.Size, Files: []SameSizeFile{},
				ContinuesBefore: token != "" && cursor.Size == entry.file.Size,
				ContinuesAfter:  len(raw) > limit && raw[limit].file.Size == entry.file.Size})
		}
		last := &bands[len(bands)-1]
		last.Files = append(last.Files, entry.file)
	}
	for _, band := range bands {
		if len(band.Files) >= 2 || band.ContinuesBefore || band.ContinuesAfter {
			countSameSizeObjects(&band)
			r.Bands = append(r.Bands, band)
		}
	}
	// Commit must not hide a statement-close failure or cancellation during
	// close. The deferred fallback is now an idempotent no-op.
	if err = errors.Join(entries.close(), ctx.Err()); err != nil {
		return SameSizeReport{}, err
	}
	if hooks.beforeCommit != nil {
		hooks.beforeCommit()
	}
	if err = ctx.Err(); err != nil {
		return SameSizeReport{}, err
	}
	if err = tx.Commit(); err != nil {
		return SameSizeReport{}, err
	}
	if err = ctx.Err(); err != nil {
		return SameSizeReport{}, err
	}
	return r, nil
}
