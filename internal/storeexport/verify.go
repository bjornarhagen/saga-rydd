//go:build darwin || linux

package storeexport

import (
	"context"
	"errors"
	"path/filepath"

	"golang.org/x/sys/unix"
)

func readOnly(raw any, e engineEnv) error {
	source, ok := raw.(interface{ IsReadOnly(string) (bool, error) })
	if !ok {
		return ErrProfile
	}
	if err := e.fence(); err != nil {
		return err
	}
	ro, err := source.IsReadOnly("main")
	if err != nil {
		return err
	}
	if !ro {
		return ErrProfile
	}
	return e.fence()
}

func inspectScratch(scratch *heldDir, pin unix.Stat_t, want profile, g *guard) (closed bool, err error) {
	// No scratch engine has been opened if the standalone header refuses.
	if err = rollbackHeader(scratch, pin, g); err != nil {
		return true, phaseError("scratch rollback header", err)
	}
	admit := func() error {
		if err := scratch.check(); err != nil {
			return err
		}
		actual, err := scratch.stat("plans.sqlite3")
		if err != nil {
			return err
		}
		if !sameFile(pin, actual) {
			return ErrChanged
		}
		return nil
	}
	e := engineEnv{g, admit}
	db, conn, err := openEngine(filepath.Join(scratch.path, "plans.sqlite3"), e)
	if err != nil {
		return false, phaseError("scratch engine open", err)
	}
	got, inspectionErr := readProfile(conn, e)
	inspectionErr = phaseError("scratch profile", inspectionErr)
	if inspectionErr == nil && got != want {
		inspectionErr = ErrProfile
	}
	if inspectionErr == nil {
		inspectionErr = phaseError("scratch integrity", integrity(conn, e))
	}
	closed, closeErr := consumeConnection(conn, db, func(raw any) error { return readOnly(raw, e) })
	err = errors.Join(inspectionErr, closeErr, e.fence())
	if err == nil {
		err = phaseError("scratch engine members", scratch.exact(g, "plans.sqlite3"))
	}
	return
}

// VerifyPlanStoreExport reads an explicitly selected artifact and writes one
// exclusive scratch child. SQLite opens only that copy. An error can retain
// this child's exact path; the function never prunes an earlier child.
func VerifyPlanStoreExport(ctx context.Context, artifact, scratchParent string) (result VerificationResult, err error) {
	result.Contract = Contract
	if ctx == nil {
		return result, ErrProfile
	}
	if err = cleanPath(artifact); err != nil {
		return
	}
	if err = cleanPath(scratchParent); err != nil {
		return
	}
	if beneath(artifact, scratchParent) || len(scratchParent)+80 > maxPathBytes {
		return result, ErrProfile
	}
	work, cancel := context.WithTimeout(ctx, wholeLimit)
	defer cancel()
	g := newGuard(work)
	defer func() { joined(&err, g.check()) }()
	if err = g.check(); err != nil {
		return
	}
	input, err := openDir(artifact)
	if err != nil {
		return
	}
	defer func() { joined(&err, input.close()) }()
	if err = input.exact(g, "plans.sqlite3", "manifest.json"); err != nil {
		return
	}
	_, body, manifestPin, err := streamFile(input, "manifest.json", nil, maxManifestBytes, g, nil, true)
	if err != nil {
		return
	}
	m, err := parseManifest(body)
	if err != nil {
		return
	}
	result.ExportID = m.ExportID
	digest, _, dataPin, err := streamFile(input, "plans.sqlite3", nil, maxDatabaseBytes, g, nil, false)
	if err != nil {
		return
	}
	if dataPin.Size != m.DataBytes || digest != m.DataSHA256 || dataPin.Mode&0777 != 0600 || manifestPin.Mode&0777 != 0600 {
		return result, ErrProfile
	}
	parent, err := openDir(scratchParent)
	if err != nil {
		return
	}
	defer func() { joined(&err, parent.close()) }()
	scratch, err := createStage(parent, g)
	retainScratchStage(&result, scratch, err)
	if err != nil {
		return
	}
	defer func() { joined(&err, scratch.close()) }()
	copied, pin, err := copyFile(input, "plans.sqlite3", dataPin, scratch, g)
	if err != nil {
		return
	}
	if copied != digest {
		return result, ErrChanged
	}
	if err = scratch.exact(g, "plans.sqlite3"); err != nil {
		return
	}
	result.EngineClosureKnown, err = inspectScratch(scratch, pin, m.Profile, g)
	if err != nil {
		return
	}
	after, _, afterPin, err := streamFile(scratch, "plans.sqlite3", &pin, maxDatabaseBytes, g, nil, false)
	if err != nil {
		return
	}
	if after != digest {
		return result, ErrChanged
	}
	if err = removeScratch(scratch, afterPin, g); err != nil {
		return
	}
	result.ScratchRetained = false
	if err = input.exact(g, "plans.sqlite3", "manifest.json"); err != nil {
		return
	}
	after, _, _, err = streamFile(input, "plans.sqlite3", &dataPin, maxDatabaseBytes, g, nil, false)
	if err != nil {
		return
	}
	if after != digest {
		return result, ErrChanged
	}
	_, afterManifest, _, err := streamFile(input, "manifest.json", &manifestPin, maxManifestBytes, g, nil, true)
	if err != nil {
		return
	}
	if string(afterManifest) != string(body) {
		return result, ErrChanged
	}
	if err = g.check(); err != nil {
		return
	}
	result.Verified = true
	return
}
