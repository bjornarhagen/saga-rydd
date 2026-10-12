package state

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"time"

	"github.com/bjornarhagen/saga-rydd/internal/localfs"
)

const manualRefreshKey = "inventory.manual_refresh.v1"
const manualRefreshContract = "inventory_manual_refresh_v1"
const manualRefreshSlotLimit = 8192
const manualRefreshStatementLimit = 16

var (
	ErrManualRefreshInvalid = errors.New("invalid manual refresh evidence")
	ErrManualRefreshScope   = errors.New("manual refresh scope changed")
	ErrManualRefreshUnknown = errors.New("manual refresh activation lineage is unknown")
)

// ManualRefreshReceipt describes saved historical metadata work. It grants no
// content-read or cleanup permission, and root EOF alone is not completion.
type ManualRefreshReceipt struct {
	Contract               string `json:"contract"`
	InventoryIncarnationID string `json:"inventory_incarnation_id"`
	RootID                 int64  `json:"root_id"`
	RequestGeneration      int64  `json:"request_generation"`
	CompactMode            bool   `json:"compact_mode"`
	Phase                  string `json:"phase"`
	ActivatedRootJobID     int64  `json:"activated_root_job_id"`
	ObservedRootGeneration int64  `json:"observed_root_generation"`
	RootListingEOFWitness  bool   `json:"root_listing_eof_witness"`
	TerminalQualification  string `json:"terminal_qualification"`
}

type manualRefreshSlot struct {
	ManualRefreshReceipt
	RawRootPath []byte `json:"raw_root_path"`
}

// ManualRefreshSession is invocation-local. Its private inputs and receipt do
// not alias caller data. A new session cannot inherit an activation proof.
type ManualRefreshSession struct {
	self              *ManualRefreshSession
	store             *Store
	lock              *localfs.Lock
	path              []byte
	incarnation       string
	rootID            int64
	mode              bool
	preflightSlot     []byte
	last              *manualRefreshSlot
	requested         bool
	discoveredUnknown bool
	fresh             *ManualRefreshActivation
	hooks             *manualRefreshHooks
}

// ManualRefreshActivation cannot be reconstructed from a receipt, job ID or
// generation. Only the invocation that created the root job receives it.
type ManualRefreshActivation struct {
	self              *ManualRefreshActivation
	session           *ManualRefreshSession
	requestGeneration int64
	jobID             int64
	sealed            bool
}

// These nil-default selected-only hooks exercise acknowledgment boundaries and
// count additional statements. Legacy CommitScan never enters this wrapper.
type manualRefreshHooks struct {
	beforeCommit func(string)
	afterCommit  func(string)
	statement    func()
}

type manualRefreshTx struct {
	tx         *sql.Tx
	ctx        context.Context
	statements int
	hooks      *manualRefreshHooks
}

func (m *manualRefreshTx) admit() error {
	if err := m.ctx.Err(); err != nil {
		return err
	}
	if m.statements >= manualRefreshStatementLimit {
		return ErrManualRefreshInvalid
	}
	m.statements++
	if m.hooks != nil && m.hooks.statement != nil {
		m.hooks.statement()
	}
	return nil
}

func (m *manualRefreshTx) scan(query string, destinations []any, args ...any) error {
	if err := m.admit(); err != nil {
		return err
	}
	return m.tx.QueryRowContext(m.ctx, query, args...).Scan(destinations...)
}

func (m *manualRefreshTx) exec(query string, args ...any) (sql.Result, error) {
	if err := m.admit(); err != nil {
		return nil, err
	}
	return m.tx.ExecContext(m.ctx, query, args...)
}

func (m *manualRefreshTx) commit(operation string) error {
	if m.hooks != nil && m.hooks.beforeCommit != nil {
		m.hooks.beforeCommit(operation)
	}
	if err := m.ctx.Err(); err != nil {
		return err
	}
	if err := m.tx.Commit(); err != nil {
		return err
	}
	if m.hooks != nil && m.hooks.afterCommit != nil {
		m.hooks.afterCommit(operation)
	}
	return nil
}

func manualRefreshContext(parent context.Context) (context.Context, context.CancelFunc, error) {
	if parent == nil {
		return nil, nil, ErrManualRefreshInvalid
	}
	if err := parent.Err(); err != nil {
		return nil, nil, err
	}
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	return ctx, cancel, nil
}

func (s *Store) manualRefreshBegin(ctx context.Context, session *ManualRefreshSession) (*manualRefreshTx, error) {
	if s == nil || s.db == nil || s.readOnly || s.lock == nil {
		return nil, ErrManualRefreshScope
	}
	if session != nil && (session.self != session || session.store != s || session.lock != s.lock) {
		return nil, ErrManualRefreshScope
	}
	// Close leaves Store's pointers intact. Entering the DB is also required;
	// pointer equality alone never makes a closed Store usable.
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	m := &manualRefreshTx{tx: tx, ctx: ctx}
	if session != nil {
		m.hooks = session.hooks
	}
	return m, nil
}

func manualRefreshHex(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, c := range value {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

func manualRefreshValid(slot manualRefreshSlot) bool {
	r := slot.ManualRefreshReceipt
	if r.Contract != manualRefreshContract || !manualRefreshHex(r.InventoryIncarnationID) || r.RootID <= 0 || r.RequestGeneration <= 0 || !canonicalAdmissionPath(slot.RawRootPath) || r.ObservedRootGeneration < 0 {
		return false
	}
	if r.RootListingEOFWitness && r.ObservedRootGeneration == 0 {
		return false
	}
	switch r.Phase {
	case "pending":
		return r.ActivatedRootJobID == 0 && r.ObservedRootGeneration == 0 && !r.RootListingEOFWitness && r.TerminalQualification == "pending_work"
	case "active":
		return r.ActivatedRootJobID > 0 && ((!r.RootListingEOFWitness && r.TerminalQualification == "historical_root_listing_pending") || (r.RootListingEOFWitness && r.TerminalQualification == "historical_root_listing"))
	case "completed":
		return r.ActivatedRootJobID > 0 && r.RootListingEOFWitness && r.TerminalQualification == "historical_pass_completed"
	case "unknown":
		return r.ActivatedRootJobID > 0 && r.TerminalQualification == "activation_lineage_unknown"
	default:
		return false
	}
}

func manualRefreshEncode(slot manualRefreshSlot) ([]byte, error) {
	if !manualRefreshValid(slot) {
		return nil, ErrManualRefreshInvalid
	}
	raw, err := json.Marshal(slot)
	if err != nil || len(raw) > manualRefreshSlotLimit {
		return nil, ErrManualRefreshInvalid
	}
	projection, err := json.Marshal(slot.ManualRefreshReceipt)
	if err != nil || len(projection) > 1024 {
		return nil, ErrManualRefreshInvalid
	}
	return raw, nil
}

func manualRefreshDecode(raw []byte) (*manualRefreshSlot, error) {
	if len(raw) == 0 || len(raw) > manualRefreshSlotLimit {
		return nil, ErrManualRefreshInvalid
	}
	var slot manualRefreshSlot
	if err := json.Unmarshal(raw, &slot); err != nil {
		return nil, ErrManualRefreshInvalid
	}
	canonical, err := manualRefreshEncode(slot)
	// Exact canonical fixed shape rejects duplicate, unknown and missing fields,
	// alternate scalar types/encodings and overflow, without a permissive decode.
	if err != nil || !bytes.Equal(raw, canonical) {
		return nil, ErrManualRefreshInvalid
	}
	return &slot, nil
}

func manualRefreshReceipt(slot *manualRefreshSlot) *ManualRefreshReceipt {
	if slot == nil {
		return nil
	}
	r := slot.ManualRefreshReceipt
	return &r
}

func manualRefreshRemember(session *ManualRefreshSession, slot *manualRefreshSlot) {
	if slot == nil {
		session.last = nil
		return
	}
	copySlot := *slot
	copySlot.RawRootPath = append([]byte(nil), slot.RawRootPath...)
	session.last = &copySlot
}

type manualRefreshScope struct {
	incarnation string
	rootID      int64
	mode        bool
	slot        *manualRefreshSlot
	rawSlot     []byte
}

// Four selected queries. CASE/length/typeof prevent oversized values crossing
// into Go. LIMIT 2 captures the selected root and detects any other root,
// including disabled roots. Index/page work remains cooperatively bounded.
func manualRefreshRead(m *manualRefreshTx, path []byte, allowEmpty bool) (manualRefreshScope, error) {
	var scope manualRefreshScope
	var kind string
	var length int64
	err := m.scan(`SELECT typeof(value),length(value),CASE WHEN typeof(value)='blob' AND length(value) BETWEEN 1 AND 8192 THEN substr(value,1,8192) ELSE NULL END FROM settings WHERE key=?`, []any{&kind, &length, &scope.rawSlot}, manualRefreshKey)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return scope, err
	}
	if err == nil {
		if kind != "blob" || length <= 0 || length > manualRefreshSlotLimit {
			return scope, ErrManualRefreshInvalid
		}
		scope.slot, err = manualRefreshDecode(scope.rawSlot)
		if err != nil {
			return scope, err
		}
	}
	var incarnation []byte
	err = m.scan(`SELECT typeof(token),length(CAST(token AS BLOB)),CASE WHEN typeof(token)='text' AND length(CAST(token AS BLOB))=64 THEN substr(CAST(token AS BLOB),1,64) ELSE NULL END FROM inventory_identity WHERE singleton=1`, []any{&kind, &length, &incarnation})
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return scope, ErrManualRefreshInvalid
		}
		return scope, err
	}
	if kind != "text" || length != 64 || !manualRefreshHex(string(incarnation)) {
		return scope, ErrManualRefreshInvalid
	}
	scope.incarnation = string(incarnation)
	var mode []byte
	err = m.scan(`SELECT typeof(value),length(value),CASE WHEN typeof(value)='blob' AND length(value)=1 THEN substr(value,1,1) ELSE NULL END FROM settings WHERE key='inventory.compact'`, []any{&kind, &length, &mode})
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return scope, err
	}
	if err == nil {
		if kind != "blob" || length != 1 || (string(mode) != "0" && string(mode) != "1") {
			return scope, ErrManualRefreshInvalid
		}
		scope.mode = string(mode) == "1"
	}
	if err = m.admit(); err != nil {
		return scope, err
	}
	rows, err := m.tx.QueryContext(m.ctx, "SELECT "+rootAdmissionProjection+" FROM roots ORDER BY id LIMIT 2")
	if err != nil {
		return scope, err
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		count++
		r, scanErr := rootAdmissionScan(rows)
		if scanErr != nil {
			return scope, ErrManualRefreshInvalid
		}
		if count != 1 || r.enabled != 1 || !bytes.Equal(r.path, path) {
			return scope, ErrManualRefreshScope
		}
		scope.rootID = r.id
	}
	if err = rows.Err(); err != nil {
		return scope, err
	}
	if err = rows.Close(); err != nil {
		return scope, err
	}
	if count == 0 && (!allowEmpty || scope.slot != nil) {
		return scope, ErrManualRefreshScope
	}
	return scope, nil
}

func manualRefreshSlotScope(slot *manualRefreshSlot, scope manualRefreshScope, path []byte) bool {
	return slot == nil || (slot.InventoryIncarnationID == scope.incarnation && slot.RootID == scope.rootID && bytes.Equal(slot.RawRootPath, path) && slot.CompactMode == scope.mode)
}

func manualRefreshSessionScope(session *ManualRefreshSession, scope manualRefreshScope) bool {
	return scope.incarnation == session.incarnation && scope.mode == session.mode && (session.rootID == 0 || scope.rootID == session.rootID)
}

func manualRefreshSameRequest(a, b *manualRefreshSlot) bool {
	return a != nil && b != nil && a.InventoryIncarnationID == b.InventoryIncarnationID && a.RootID == b.RootID && a.RequestGeneration == b.RequestGeneration && a.CompactMode == b.CompactMode && bytes.Equal(a.RawRootPath, b.RawRootPath)
}

func manualRefreshDrained(m *manualRefreshTx, rootID int64) (bool, error) {
	var unfinished bool
	err := m.scan(`SELECT EXISTS(SELECT 1 FROM jobs WHERE root_id=?)
 OR EXISTS(SELECT 1 FROM compact_retirement WHERE root_id=?)
 OR EXISTS(SELECT 1 FROM subtree_reconcile WHERE root_id=?)
 OR EXISTS(SELECT 1 FROM subtree_retirement WHERE root_id=?)
 OR EXISTS(SELECT 1 FROM allocation_cache c LEFT JOIN allocation_revisions v ON v.root_id=c.root_id
 WHERE c.root_id=? AND (v.revision IS NULL OR c.revision!=v.revision OR c.phase!='done'))`, []any{&unfinished}, rootID, rootID, rootID, rootID, rootID)
	return !unfinished, err
}

func manualRefreshWrite(m *manualRefreshTx, slot manualRefreshSlot) error {
	raw, err := manualRefreshEncode(slot)
	if err != nil {
		return err
	}
	_, err = m.exec("INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", manualRefreshKey, raw)
	return err
}

func manualRefreshLoseLineage(m *manualRefreshTx, session *ManualRefreshSession, slot *manualRefreshSlot) error {
	copySlot := *slot
	copySlot.Phase, copySlot.TerminalQualification = "unknown", "activation_lineage_unknown"
	if err := manualRefreshWrite(m, copySlot); err != nil {
		return err
	}
	if err := m.commit("request_unknown"); err != nil {
		return err
	}
	manualRefreshRemember(session, &copySlot)
	session.requested, session.discoveredUnknown = true, true
	if err := m.ctx.Err(); err != nil {
		return err
	}
	return ErrManualRefreshUnknown
}

// PreflightManualRefresh must precede ordinary mode/root setup. It never writes.
func (s *Store) PreflightManualRefresh(parent context.Context, rootPath []byte, requestedCompact *bool) (*ManualRefreshSession, *ManualRefreshReceipt, error) {
	if !canonicalAdmissionPath(rootPath) {
		return nil, nil, ErrManualRefreshInvalid
	}
	path := append([]byte(nil), rootPath...)
	var requested *bool
	if requestedCompact != nil {
		value := *requestedCompact
		requested = &value
	}
	ctx, cancel, err := manualRefreshContext(parent)
	if err != nil {
		return nil, nil, err
	}
	defer cancel()
	m, err := s.manualRefreshBegin(ctx, nil)
	if err != nil {
		return nil, nil, err
	}
	defer m.tx.Rollback()
	scope, err := manualRefreshRead(m, path, true)
	if err != nil {
		return nil, manualRefreshReceipt(scope.slot), err
	}
	if scope.slot != nil && (scope.slot.InventoryIncarnationID != scope.incarnation || scope.slot.RootID != scope.rootID || !bytes.Equal(scope.slot.RawRootPath, path) || ((scope.slot.Phase == "pending" || scope.slot.Phase == "active") && scope.slot.CompactMode != scope.mode)) {
		return nil, manualRefreshReceipt(scope.slot), ErrManualRefreshScope
	}
	mode := scope.mode
	if requested != nil {
		mode = *requested
		if scope.slot != nil && (scope.slot.Phase == "pending" || scope.slot.Phase == "active") && mode != scope.mode {
			return nil, manualRefreshReceipt(scope.slot), ErrManualRefreshScope
		}
		if mode != scope.mode && scope.rootID > 0 {
			drained, drainErr := manualRefreshDrained(m, scope.rootID)
			if drainErr != nil {
				return nil, manualRefreshReceipt(scope.slot), drainErr
			}
			if !drained {
				return nil, manualRefreshReceipt(scope.slot), ErrManualRefreshScope
			}
		}
	}
	if err = ctx.Err(); err != nil {
		return nil, manualRefreshReceipt(scope.slot), err
	}
	session := &ManualRefreshSession{store: s, lock: s.lock, path: path, incarnation: scope.incarnation, rootID: scope.rootID, mode: mode, preflightSlot: append([]byte(nil), scope.rawSlot...)}
	session.self = session
	manualRefreshRemember(session, scope.slot)
	return session, manualRefreshReceipt(session.last), nil
}

func (s *Store) RequestManualRefresh(parent context.Context, session *ManualRefreshSession) (*ManualRefreshReceipt, error) {
	if session == nil {
		return nil, ErrManualRefreshInvalid
	}
	ctx, cancel, err := manualRefreshContext(parent)
	if err != nil {
		return manualRefreshReceipt(session.last), err
	}
	defer cancel()
	m, err := s.manualRefreshBegin(ctx, session)
	if err != nil {
		return manualRefreshReceipt(session.last), err
	}
	defer m.tx.Rollback()
	scope, err := manualRefreshRead(m, session.path, false)
	if err != nil {
		return manualRefreshReceipt(session.last), err
	}
	if !manualRefreshSessionScope(session, scope) {
		return manualRefreshReceipt(session.last), ErrManualRefreshScope
	}
	if session.discoveredUnknown {
		return manualRefreshReceipt(session.last), ErrManualRefreshUnknown
	}
	if session.requested {
		if !manualRefreshSameRequest(session.last, scope.slot) {
			return manualRefreshReceipt(session.last), ErrManualRefreshScope
		}
		manualRefreshRemember(session, scope.slot)
		if scope.slot.Phase == "active" && !scope.slot.RootListingEOFWitness && session.fresh == nil {
			err = manualRefreshLoseLineage(m, session, scope.slot)
			return manualRefreshReceipt(session.last), err
		}
		if scope.slot.Phase == "unknown" {
			session.discoveredUnknown = true
			return manualRefreshReceipt(session.last), ErrManualRefreshUnknown
		}
		return manualRefreshReceipt(session.last), ctx.Err()
	}
	if !bytes.Equal(scope.rawSlot, session.preflightSlot) {
		return manualRefreshReceipt(session.last), ErrManualRefreshScope
	}
	// Terminal records may retain the previous mode after the separately admitted
	// explicit mode setup. Nonterminal records must retain their exact mode.
	if scope.slot != nil && (scope.slot.InventoryIncarnationID != scope.incarnation || scope.slot.RootID != scope.rootID || !bytes.Equal(scope.slot.RawRootPath, session.path) || ((scope.slot.Phase == "pending" || scope.slot.Phase == "active") && scope.slot.CompactMode != scope.mode)) {
		return manualRefreshReceipt(scope.slot), ErrManualRefreshScope
	}
	slot := scope.slot
	if slot != nil && slot.Phase == "active" && !slot.RootListingEOFWitness {
		err = manualRefreshLoseLineage(m, session, slot)
		return manualRefreshReceipt(session.last), err
	}
	if slot != nil && (slot.Phase == "pending" || slot.Phase == "active") {
		manualRefreshRemember(session, slot)
		session.requested = true
		return manualRefreshReceipt(session.last), ctx.Err()
	}
	if slot != nil && slot.Phase == "unknown" {
		drained, drainErr := manualRefreshDrained(m, scope.rootID)
		if drainErr != nil {
			return manualRefreshReceipt(slot), drainErr
		}
		if !drained {
			manualRefreshRemember(session, slot)
			session.discoveredUnknown = true
			return manualRefreshReceipt(slot), ErrManualRefreshUnknown
		}
	}
	generation := int64(1)
	if slot != nil {
		if slot.RequestGeneration == math.MaxInt64 {
			return manualRefreshReceipt(slot), ErrManualRefreshInvalid
		}
		generation = slot.RequestGeneration + 1
	}
	next := manualRefreshSlot{ManualRefreshReceipt: ManualRefreshReceipt{Contract: manualRefreshContract, InventoryIncarnationID: scope.incarnation, RootID: scope.rootID, RequestGeneration: generation, CompactMode: scope.mode, Phase: "pending", TerminalQualification: "pending_work"}, RawRootPath: append([]byte(nil), session.path...)}
	if err = manualRefreshWrite(m, next); err != nil {
		return manualRefreshReceipt(session.last), err
	}
	if err = m.commit("request"); err != nil {
		return manualRefreshReceipt(session.last), err
	}
	session.rootID, session.requested = scope.rootID, true
	manualRefreshRemember(session, &next)
	return manualRefreshReceipt(session.last), ctx.Err()
}

func manualRefreshCurrent(m *manualRefreshTx, session *ManualRefreshSession) (*manualRefreshSlot, error) {
	scope, err := manualRefreshRead(m, session.path, false)
	if err != nil {
		return nil, err
	}
	if !manualRefreshSessionScope(session, scope) || !manualRefreshSlotScope(scope.slot, scope, session.path) || !session.requested || !manualRefreshSameRequest(session.last, scope.slot) {
		return nil, ErrManualRefreshScope
	}
	manualRefreshRemember(session, scope.slot)
	if session.discoveredUnknown || scope.slot.Phase == "unknown" {
		session.discoveredUnknown = true
		return scope.slot, ErrManualRefreshUnknown
	}
	if scope.slot.Phase == "active" && !scope.slot.RootListingEOFWitness && session.fresh == nil {
		return scope.slot, manualRefreshLoseLineage(m, session, scope.slot)
	}
	return scope.slot, nil
}

func (s *Store) TryActivateManualRefresh(parent context.Context, session *ManualRefreshSession, now time.Time) (*ManualRefreshReceipt, *ManualRefreshActivation, error) {
	if session == nil {
		return nil, nil, ErrManualRefreshInvalid
	}
	ctx, cancel, err := manualRefreshContext(parent)
	if err != nil {
		return manualRefreshReceipt(session.last), nil, err
	}
	defer cancel()
	m, err := s.manualRefreshBegin(ctx, session)
	if err != nil {
		return manualRefreshReceipt(session.last), nil, err
	}
	defer m.tx.Rollback()
	slot, err := manualRefreshCurrent(m, session)
	if err != nil {
		return manualRefreshReceipt(session.last), nil, err
	}
	manualRefreshRemember(session, slot)
	if slot.Phase != "pending" {
		var activation *ManualRefreshActivation
		if slot.Phase == "active" && !slot.RootListingEOFWitness {
			activation = session.fresh
		}
		return manualRefreshReceipt(session.last), activation, ctx.Err()
	}
	n := now.UnixNano()
	if n <= 0 || !time.Unix(0, n).Equal(now) {
		return manualRefreshReceipt(session.last), nil, ErrManualRefreshInvalid
	}
	drained, err := manualRefreshDrained(m, slot.RootID)
	if err != nil {
		return manualRefreshReceipt(session.last), nil, err
	}
	if !drained {
		return manualRefreshReceipt(session.last), nil, ctx.Err()
	}
	result, err := m.exec(`INSERT INTO jobs(root_id,kind,path,due_at_ns,inventory_claimed) VALUES(?,?,X'2e',?,0)`, slot.RootID, ScanKind, n)
	if err != nil {
		return manualRefreshReceipt(session.last), nil, err
	}
	id, err := result.LastInsertId()
	if err != nil || id <= 0 {
		if err == nil {
			err = ErrManualRefreshInvalid
		}
		return manualRefreshReceipt(session.last), nil, err
	}
	next := *slot
	next.Phase, next.TerminalQualification, next.ActivatedRootJobID = "active", "historical_root_listing_pending", id
	if err = manualRefreshWrite(m, next); err != nil {
		return manualRefreshReceipt(session.last), nil, err
	}
	if err = m.commit("activate"); err != nil {
		return manualRefreshReceipt(session.last), nil, err
	}
	manualRefreshRemember(session, &next)
	activation := &ManualRefreshActivation{session: session, requestGeneration: next.RequestGeneration, jobID: id}
	activation.self = activation
	session.fresh = activation
	return manualRefreshReceipt(session.last), activation, ctx.Err()
}

type manualRefreshWitness struct {
	activation *ManualRefreshActivation
	m          *manualRefreshTx
	slot       *manualRefreshSlot
	next       *manualRefreshSlot
}

func (w *manualRefreshWitness) prepare(s *Store, ctx context.Context, tx *sql.Tx, j Job, b ScanBatch) error {
	a := w.activation
	if a == nil || a.self != a || a.session == nil || a.session.self != a.session || a.session.store != s || a.session.lock != s.lock || s.lock == nil || a.session.fresh != a || a.sealed {
		return ErrManualRefreshScope
	}
	w.m = &manualRefreshTx{tx: tx, ctx: ctx, hooks: a.session.hooks}
	slot, err := manualRefreshCurrent(w.m, a.session)
	if err != nil {
		return err
	}
	if slot.Phase != "active" || slot.RequestGeneration != a.requestGeneration || slot.ActivatedRootJobID != a.jobID || slot.RootListingEOFWitness || j.ID != a.jobID || j.RootID != slot.RootID || j.Kind != ScanKind || !bytes.Equal(j.Path, []byte(".")) || !bytes.Equal(j.RootPath, a.session.path) {
		return ErrManualRefreshScope
	}
	if b.Fault == "" && slot.ObservedRootGeneration != 0 && slot.ObservedRootGeneration != b.Generation {
		return ErrManualRefreshUnknown
	}
	w.slot = slot
	return nil
}

func (w *manualRefreshWitness) record(b ScanBatch) error {
	next := *w.slot
	if next.ObservedRootGeneration == 0 {
		next.ObservedRootGeneration = b.Generation
	}
	if b.Complete {
		next.RootListingEOFWitness = true
		next.TerminalQualification = "historical_root_listing"
	}
	if err := manualRefreshWrite(w.m, next); err != nil {
		return err
	}
	w.next = &next
	return nil
}

func (w *manualRefreshWitness) commit() error {
	if err := w.m.commit("witness"); err != nil {
		return err
	}
	if w.next != nil {
		manualRefreshRemember(w.activation.session, w.next)
		w.activation.sealed = w.next.RootListingEOFWitness
	}
	return w.m.ctx.Err()
}

// CommitManualRefreshScan adds witnesses only for the freshly activated root
// job. Remaining child work and reopened durable-EOF work use CommitScan.
func (s *Store) CommitManualRefreshScan(parent context.Context, activation *ManualRefreshActivation, j Job, b ScanBatch) (*ManualRefreshReceipt, error) {
	if activation == nil || activation.session == nil {
		return nil, ErrManualRefreshInvalid
	}
	session := activation.session
	if s == nil || s.db == nil || s.readOnly || s.lock == nil || activation.self != activation || session.self != session || session.store != s || session.lock != s.lock {
		return manualRefreshReceipt(session.last), ErrManualRefreshScope
	}
	ctx, cancel, err := manualRefreshContext(parent)
	if err != nil {
		return manualRefreshReceipt(session.last), err
	}
	defer cancel()
	witness := &manualRefreshWitness{activation: activation}
	err = s.commitScanWithManualRefresh(ctx, j, b, nil, time.Now(), witness)
	return manualRefreshReceipt(session.last), err
}

func (s *Store) FinalizeManualRefresh(parent context.Context, session *ManualRefreshSession) (*ManualRefreshReceipt, error) {
	if session == nil {
		return nil, ErrManualRefreshInvalid
	}
	ctx, cancel, err := manualRefreshContext(parent)
	if err != nil {
		return manualRefreshReceipt(session.last), err
	}
	defer cancel()
	m, err := s.manualRefreshBegin(ctx, session)
	if err != nil {
		return manualRefreshReceipt(session.last), err
	}
	defer m.tx.Rollback()
	slot, err := manualRefreshCurrent(m, session)
	if err != nil {
		return manualRefreshReceipt(session.last), err
	}
	manualRefreshRemember(session, slot)
	if slot.Phase != "active" {
		return manualRefreshReceipt(session.last), ctx.Err()
	}
	drained, err := manualRefreshDrained(m, slot.RootID)
	if err != nil {
		return manualRefreshReceipt(session.last), err
	}
	if !drained {
		return manualRefreshReceipt(session.last), ctx.Err()
	}
	var generation int64
	var complete bool
	err = m.scan(`SELECT CASE WHEN typeof(generation)='integer' THEN generation ELSE 0 END,complete=1 AND last_error='' FROM directories WHERE root_id=? AND path=X'2e'`, []any{&generation, &complete}, slot.RootID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return manualRefreshReceipt(session.last), err
	}
	next := *slot
	if !slot.RootListingEOFWitness || slot.ObservedRootGeneration <= 0 || err != nil || generation != slot.ObservedRootGeneration || !complete {
		next.Phase, next.TerminalQualification = "unknown", "activation_lineage_unknown"
	} else {
		next.Phase, next.TerminalQualification = "completed", "historical_pass_completed"
	}
	if err = manualRefreshWrite(m, next); err != nil {
		return manualRefreshReceipt(session.last), err
	}
	if err = m.commit("finalize"); err != nil {
		return manualRefreshReceipt(session.last), err
	}
	manualRefreshRemember(session, &next)
	if next.Phase == "unknown" {
		session.discoveredUnknown = true
		if err = ctx.Err(); err != nil {
			return manualRefreshReceipt(session.last), err
		}
		return manualRefreshReceipt(session.last), ErrManualRefreshUnknown
	}
	return manualRefreshReceipt(session.last), ctx.Err()
}
