//go:build darwin || linux

package storeexport

import (
	"context"
	"errors"
	"path/filepath"

	"github.com/bjornarhagen/saga-rydd/internal/localfs"
	"golang.org/x/sys/unix"
)

// ExportPlanStore exports only committed history in the fixed existing plans
// store. The artifact is private and can contain private history and unused-page
// remnants. It is not a sanitized report or an executable restore record.
func ExportPlanStore(ctx context.Context, base, destination string) (result ExportResult, err error) {
	result.Contract = Contract
	result.Destination = destination
	if ctx == nil {
		return result, ErrProfile
	}
	parentPath, destinationName, err := outputPath(base, destination)
	if err != nil {
		return
	}
	work, cancel := context.WithTimeout(ctx, wholeLimit)
	defer cancel()
	g := newGuard(work)
	defer func() { joined(&err, g.check()) }()
	if err = g.check(); err != nil {
		return
	}
	baseDir, err := openDir(base)
	if err != nil {
		return
	}
	defer func() { joined(&err, baseDir.close()) }()
	source, err := baseDir.child("plans")
	if err != nil {
		return
	}
	defer func() { joined(&err, source.close()) }()
	if !privateDir(source.stamp) {
		return result, ErrProfile
	}
	lock, err := localfs.AcquireExistingLock(work, source.path)
	if err != nil {
		return
	}
	defer func() { joined(&err, lock.Close()) }()
	before, err := sourceMetadata(source)
	if err != nil {
		return
	}
	if err = g.check(); err != nil {
		return
	}
	parent, err := openDir(parentPath)
	if err != nil {
		return
	}
	defer func() { joined(&err, parent.close()) }()
	var named unix.Stat_t
	if err = unix.Fstatat(parent.fd(), destinationName, &named, unix.AT_SYMLINK_NOFOLLOW); !errors.Is(err, unix.ENOENT) {
		if err == nil {
			err = unix.EEXIST
		}
		return
	}
	err = nil
	stage, err := createStage(parent, g)
	retainCandidateStage(&result, stage, err)
	if err != nil {
		return
	}
	defer func() { joined(&err, stage.close()) }()
	creation, candidateIdentity, err := createFile(stage, "plans.sqlite3")
	if err != nil {
		return
	}
	if err = creation.Close(); err != nil {
		return
	}
	admit := func() error {
		if err := lock.Check(work); err != nil {
			return err
		}
		if _, err := sourceMetadata(source); err != nil {
			return err
		}
		if err := stage.check(); err != nil {
			return err
		}
		candidate, err := stage.stat("plans.sqlite3")
		if err != nil {
			return err
		}
		if !sameObject(candidateIdentity, candidate) || candidate.Size > maxDatabaseBytes || candidate.Mode&0777 != 0600 {
			return ErrChanged
		}
		return nil
	}
	e := engineEnv{g, admit}
	db, conn, err := openEngine(filepath.Join(source.path, "plans.sqlite3"), e)
	if err != nil {
		err = phaseError("export source open", err)
		return
	}
	p, inspectionErr := readProfile(conn, e)
	inspectionErr = phaseError("export source profile", inspectionErr)
	destinationClosed := inspectionErr != nil // No backup destination was opened in this branch.
	sourceClosed, closeErr := consumeConnection(conn, db, func(raw any) error {
		if inspectionErr != nil {
			return nil
		}
		var copyErr error
		destinationClosed, copyErr = transfer(raw, filepath.Join(stage.path, "plans.sqlite3"), p, e, startBackup)
		return phaseError("export backup", copyErr)
	})
	result.EngineClosureKnown = sourceClosed && destinationClosed
	if err = errors.Join(inspectionErr, closeErr, e.fence()); err != nil {
		return
	}
	afterSource, err := sourceMetadata(source)
	if err != nil {
		err = phaseError("export source metadata after backup", err)
		return
	}
	if !unchangedSource(before, afterSource) {
		return result, ErrChanged
	}
	if err = stage.exact(g, "plans.sqlite3"); err != nil {
		err = phaseError("export candidate members", err)
		return
	}
	digest, _, candidatePin, err := streamFile(stage, "plans.sqlite3", nil, maxDatabaseBytes, g, nil, false)
	if err != nil {
		err = phaseError("export candidate body", err)
		return
	}
	if err = rollbackHeader(stage, candidatePin, g); err != nil {
		err = phaseError("export candidate rollback header", err)
		return
	}
	if err = g.check(); err != nil {
		return
	}
	if err = unix.Mkdirat(stage.fd(), ".verify", 0700); err != nil {
		return
	}
	scratch, err := stage.child(".verify")
	if err != nil {
		return
	}
	defer func() { joined(&err, scratch.close()) }()
	if !privateDir(scratch.stamp) || scratch.stamp.Mode&0777 != 0700 {
		return result, ErrProfile
	}
	if err = stage.exact(g, "plans.sqlite3", ".verify"); err != nil {
		err = phaseError("export candidate scratch members", err)
		return
	}
	copied, scratchPin, err := copyFile(stage, "plans.sqlite3", candidatePin, scratch, g)
	if err != nil {
		err = phaseError("export scratch copy", err)
		return
	}
	if copied != digest {
		return result, ErrChanged
	}
	result.EngineClosureKnown, err = inspectScratch(scratch, scratchPin, p, g)
	if err != nil {
		return
	}
	verified, _, verifiedPin, err := streamFile(scratch, "plans.sqlite3", &scratchPin, maxDatabaseBytes, g, nil, false)
	if err != nil {
		err = phaseError("export checked scratch body", err)
		return
	}
	if verified != digest {
		return result, ErrChanged
	}
	if err = removeScratch(scratch, verifiedPin, g); err != nil {
		err = phaseError("export scratch cleanup", err)
		return
	}
	if err = scratch.close(); err != nil {
		return
	}
	if err = stage.exact(g, "plans.sqlite3"); err != nil {
		err = phaseError("export candidate members", err)
		return
	}
	verified, _, _, err = streamFile(stage, "plans.sqlite3", &candidatePin, maxDatabaseBytes, g, nil, false)
	if err != nil {
		err = phaseError("export checked candidate body", err)
		return
	}
	if verified != digest {
		return result, ErrChanged
	}
	result.Verified = true
	m := manifest{Version: 1, Contract: Contract, CreatedAt: g.wallHigh, SourcePathBytes: []byte(filepath.Join(source.path, "plans.sqlite3")), DataFile: "plans.sqlite3", DataBytes: candidatePin.Size, DataSHA256: digest, Profile: p}
	if m.ExportID, err = manifestID(m); err != nil {
		return
	}
	result.ExportID = m.ExportID
	body, err := encodeManifest(m)
	if err != nil {
		return
	}
	manifestPin, err := writeFile(stage, "manifest.json", body, g)
	if err != nil {
		err = phaseError("export manifest write", err)
		return
	}
	if err = g.check(); err != nil {
		return
	}
	f, _, err := openFile(stage, "plans.sqlite3", &candidatePin)
	if err != nil {
		return
	}
	if err = g.check(); err != nil {
		return result, errors.Join(err, f.Close())
	}
	syncErr := f.Sync()
	fileCloseErr := f.Close()
	if err = errors.Join(syncErr, fileCloseErr, g.check()); err != nil {
		return
	}
	if err = stage.file.Sync(); err != nil {
		return
	}
	if err = e.fence(); err != nil {
		return
	}
	afterSource, err = sourceMetadata(source)
	if err != nil {
		err = phaseError("export source metadata before publication", err)
		return
	}
	if !unchangedSource(before, afterSource) {
		return result, ErrChanged
	}
	if err = stage.exact(g, "plans.sqlite3", "manifest.json"); err != nil {
		err = phaseError("export final candidate members", err)
		return
	}
	for _, item := range []struct {
		name string
		pin  unix.Stat_t
	}{{"plans.sqlite3", candidatePin}, {"manifest.json", manifestPin}} {
		if err = g.check(); err != nil {
			return
		}
		f, _, openErr := openFile(stage, item.name, &item.pin)
		if openErr != nil {
			return result, openErr
		}
		actual, checkErr := createdOutputPin(stage, item.name, f, item.pin, item.pin.Size)
		if checkErr == nil && !sameFile(item.pin, actual) {
			checkErr = ErrChanged
		}
		if err = errors.Join(checkErr, f.Close(), g.check()); err != nil {
			return
		}
	}
	if _, err = parent.names(g, maxParentNames+1); err != nil {
		err = phaseError("export publication parent names", err)
		return
	}
	if err = stage.check(); err != nil {
		return
	}
	if err = g.check(); err != nil {
		return
	}
	if err = publishNoReplace(parent.fd(), stage.name, destinationName); err != nil {
		return
	}
	result.Published = true
	result.CandidateRetained = false
	stage.name = destinationName
	stage.path = destination
	if err = stage.check(); err != nil {
		return
	}
	if err = g.check(); err != nil {
		return
	}
	if err = parent.file.Sync(); err != nil {
		return
	}
	if err = g.check(); err != nil {
		return
	}
	result.SyncCompleted = true
	return
}
