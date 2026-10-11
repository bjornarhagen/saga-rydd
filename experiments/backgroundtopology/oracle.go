package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

var errOracle = errors.New("generated topology oracle refused unexpected evidence")

type topology struct {
	shape string
	files int
}

func (t topology) levels() int {
	if t.shape == "deep" {
		return 64
	}
	if t.shape == "healthy" {
		return 32
	}
	return 1
}
func (t topology) path(root string, n int) string {
	if t.shape == "healthy" {
		return filepath.Join(root, fmt.Sprintf("branch%02d", n/16), fmt.Sprintf("f%07d", n))
	}
	level := n / (t.files / t.levels())
	parts := []string{root}
	for i := 1; i <= level; i++ {
		parts = append(parts, fmt.Sprintf("d%02d", i))
	}
	return filepath.Join(append(parts, fmt.Sprintf("f%07d", n))...)
}
func (t topology) body(n int) []byte {
	if n%1024 != 0 && n%(t.files/t.levels()) != 0 {
		return nil
	}
	b := make([]byte, 32)
	copy(b, "Rydd generated topology sentinel")
	binary.BigEndian.PutUint64(b[24:], uint64(n))
	return b
}
func (t topology) directoryAllowed(root, dir string) bool {
	rel, err := filepath.Rel(root, dir)
	if err != nil {
		return false
	}
	if rel == "." {
		return true
	}
	switch t.shape {
	case "wide":
		return false
	case "healthy":
		for i := 0; i < 32; i++ {
			if rel == fmt.Sprintf("branch%02d", i) {
				return true
			}
		}
		return false
	case "deep":
		parts := strings.Split(rel, string(filepath.Separator))
		if len(parts) > 63 {
			return false
		}
		for i, p := range parts {
			if p != fmt.Sprintf("d%02d", i+1) {
				return false
			}
		}
		return true
	}
	return false
}
func (t topology) directoryCount() int {
	if t.shape == "healthy" {
		return 33
	}
	return t.levels()
}

func generate(ctx context.Context, root string, t topology) error {
	if (t.shape != "wide" && t.shape != "deep" && t.shape != "healthy") || t.files < t.levels() || t.files > 1000000 || t.files%t.levels() != 0 {
		return errOracle
	}
	if t.shape == "healthy" && t.files != 512 {
		return errOracle
	}
	if err := os.Mkdir(root, 0700); err != nil {
		return err
	}
	for n := 0; n < t.files; n++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		path := t.path(root, n)
		if n%(t.files/t.levels()) == 0 && filepath.Dir(path) != root {
			if err := os.Mkdir(filepath.Dir(path), 0700); err != nil {
				return err
			}
		}
		f, e := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if e != nil {
			return e
		}
		_, e = f.Write(t.body(n))
		ce := f.Close()
		if e != nil {
			return e
		}
		if ce != nil {
			return ce
		}
	}
	return nil
}

type oracle struct {
	Files, Directories, Logical, Allocated, DirectoryAllocated int64
	BodiesSHA256                                               string
}

// The hooks are a private deterministic fixture seam. Production passes the
// zero value; no arguments, environment or generated profile can select them.
type oracleLeafHooks struct {
	beforeOpen func()
	afterRead  func()
}

func sameOracleStat(a, b unix.Stat_t) bool {
	return a.Dev == b.Dev && a.Ino == b.Ino && a.Mode == b.Mode &&
		a.Uid == b.Uid && a.Gid == b.Gid && a.Nlink == b.Nlink &&
		a.Size == b.Size && a.Blocks == b.Blocks &&
		a.Mtim == b.Mtim && a.Ctim == b.Ctim
}

func oracleStatMatchesInfo(a unix.Stat_t, info os.FileInfo) bool {
	b, ok := info.Sys().(*syscall.Stat_t)
	return ok && uint64(a.Dev) == uint64(b.Dev) && uint64(a.Ino) == uint64(b.Ino) &&
		uint32(a.Mode) == uint32(b.Mode) && a.Uid == b.Uid && a.Gid == b.Gid &&
		uint64(a.Nlink) == uint64(b.Nlink) && a.Size == info.Size() &&
		a.Blocks == b.Blocks && a.Mtim.Nano() == info.ModTime().UnixNano()
}

func oracleDirectoryNamedUnchanged(path string, expected unix.Stat_t) error {
	var named unix.Stat_t
	if err := unix.Lstat(path, &named); err != nil {
		return errors.Join(errOracle, err)
	}
	if named.Mode&unix.S_IFMT != unix.S_IFDIR || !sameOracleStat(expected, named) {
		return errOracle
	}
	return nil
}

func oracleDirectoryUnchanged(parent *os.File, path string, expected unix.Stat_t) error {
	var held unix.Stat_t
	if err := unix.Fstat(int(parent.Fd()), &held); err != nil {
		return errors.Join(errOracle, err)
	}
	if held.Mode&unix.S_IFMT != unix.S_IFDIR || !sameOracleStat(expected, held) {
		return errOracle
	}
	return oracleDirectoryNamedUnchanged(path, expected)
}

func openOracleDirectory(path string, info os.FileInfo) (*os.File, unix.Stat_t, error) {
	var expected unix.Stat_t
	if err := unix.Lstat(path, &expected); err != nil {
		return nil, expected, errors.Join(errOracle, err)
	}
	if expected.Mode&unix.S_IFMT != unix.S_IFDIR || !oracleStatMatchesInfo(expected, info) {
		return nil, expected, errOracle
	}
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, expected, errors.Join(errOracle, err)
	}
	f := os.NewFile(uintptr(fd), path)
	if err = oracleDirectoryUnchanged(f, path, expected); err != nil {
		return nil, expected, errors.Join(err, f.Close())
	}
	return f, expected, nil
}

func oracleLeafNamedUnchanged(parent *os.File, name string, expected unix.Stat_t) error {
	var named unix.Stat_t
	if err := unix.Fstatat(int(parent.Fd()), name, &named, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return errors.Join(errOracle, err)
	}
	if named.Mode&unix.S_IFMT != unix.S_IFREG || named.Nlink != 1 || !sameOracleStat(expected, named) {
		return errOracle
	}
	return nil
}

func readOracleLeaf(ctx context.Context, parent *os.File, parentPath, name string, parentStamp unix.Stat_t, info os.FileInfo, want []byte) ([]byte, error) {
	return readOracleLeafWithHooks(ctx, parent, parentPath, name, parentStamp, info, want, oracleLeafHooks{})
}

func readOracleLeafWithHooks(ctx context.Context, parent *os.File, parentPath, name string, parentStamp unix.Stat_t, info os.FileInfo, want []byte, hooks oracleLeafHooks) (data []byte, err error) {
	if name == "." || name == ".." || filepath.Base(name) != name || (len(want) != 0 && len(want) != 32) {
		return nil, errOracle
	}
	if err = ctx.Err(); err != nil {
		return nil, errors.Join(errOracle, err)
	}
	if err = oracleDirectoryUnchanged(parent, parentPath, parentStamp); err != nil {
		return nil, err
	}
	var expected unix.Stat_t
	if err = unix.Fstatat(int(parent.Fd()), name, &expected, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return nil, errors.Join(errOracle, err)
	}
	if expected.Mode&unix.S_IFMT != unix.S_IFREG || expected.Nlink != 1 ||
		expected.Size != int64(len(want)) || !oracleStatMatchesInfo(expected, info) {
		return nil, errOracle
	}
	if hooks.beforeOpen != nil {
		hooks.beforeOpen()
	}
	fd, openErr := unix.Openat(int(parent.Fd()), name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if openErr != nil {
		return nil, errors.Join(errOracle, openErr, ctx.Err())
	}
	body := os.NewFile(uintptr(fd), name)
	defer func() {
		// Attempt close and both name/parent proofs independently. A refusal
		// returns no body even when a bounded read already completed.
		closeErr := body.Close()
		nameErr := oracleLeafNamedUnchanged(parent, name, expected)
		parentErr := oracleDirectoryUnchanged(parent, parentPath, parentStamp)
		contextErr := ctx.Err()
		if closeErr != nil || nameErr != nil || parentErr != nil || contextErr != nil {
			err = errors.Join(err, errOracle, closeErr, nameErr, parentErr, contextErr)
		}
		if err != nil {
			data = nil
		}
	}()
	var held unix.Stat_t
	if err = unix.Fstat(fd, &held); err != nil {
		return nil, errors.Join(errOracle, err)
	}
	if held.Mode&unix.S_IFMT != unix.S_IFREG || held.Nlink != 1 || !sameOracleStat(expected, held) {
		return nil, errOracle
	}
	if err = ctx.Err(); err != nil {
		return nil, errors.Join(errOracle, err)
	}
	data, err = io.ReadAll(io.LimitReader(body, 33))
	if err != nil || !bytes.Equal(data, want) {
		return nil, errors.Join(errOracle, err)
	}
	// For an admitted zero- or 32-byte body, ReadAll must have observed EOF
	// before its 33-byte ceiling; an extra byte refuses the whole observation.
	if hooks.afterRead != nil {
		hooks.afterRead()
	}
	if err = unix.Fstat(fd, &held); err != nil {
		return nil, errors.Join(errOracle, err)
	}
	if !sameOracleStat(expected, held) {
		return nil, errOracle
	}
	if err = oracleLeafNamedUnchanged(parent, name, expected); err != nil {
		return nil, err
	}
	if err = oracleDirectoryUnchanged(parent, parentPath, parentStamp); err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, errors.Join(errOracle, err)
	}
	return data, nil
}

func inspect(ctx context.Context, root, database string, t topology) (oracle, error) {
	var result oracle
	db, err := sql.Open("sqlite", database)
	if err != nil {
		return result, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err = db.ExecContext(ctx, `PRAGMA cache_size=-4096; PRAGMA mmap_size=0; PRAGMA temp_store=FILE;
CREATE TABLE directory_observations(path BLOB PRIMARY KEY,device TEXT,inode TEXT,allocated INTEGER,UNIQUE(device,inode));
CREATE TABLE observations(ordinal INTEGER PRIMARY KEY,path BLOB UNIQUE,device TEXT,inode TEXT,body BLOB,logical INTEGER,allocated INTEGER,UNIQUE(device,inode));`); err != nil {
		return result, err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return result, err
	}
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()
	var walk func(string) error
	walk = func(dir string) (walkErr error) {
		if err := ctx.Err(); err != nil {
			return err
		}
		if result.Directories >= int64(t.directoryCount()) || !t.directoryAllowed(root, dir) {
			return errOracle
		}
		info, err := os.Lstat(dir)
		if err != nil || !info.IsDir() {
			return errOracle
		}
		result.Directories++
		rawDir, ok := info.Sys().(*syscall.Stat_t)
		if !ok || rawDir.Blocks < 0 || rawDir.Blocks > int64(^uint64(0)>>1)/512 {
			return errOracle
		}
		relative, e := filepath.Rel(root, dir)
		if e != nil {
			return e
		}
		if _, e = tx.ExecContext(ctx, "INSERT INTO directory_observations VALUES(?,?,?,?)", []byte(relative), fmt.Sprint(rawDir.Dev), fmt.Sprint(rawDir.Ino), rawDir.Blocks*512); e != nil {
			return errOracle
		}
		result.DirectoryAllocated += rawDir.Blocks * 512
		f, parentStamp, err := openOracleDirectory(dir, info)
		if err != nil {
			return err
		}
		defer func() {
			closeErr := f.Close()
			nameErr := oracleDirectoryNamedUnchanged(dir, parentStamp)
			contextErr := ctx.Err()
			if closeErr != nil || nameErr != nil || contextErr != nil {
				walkErr = errors.Join(walkErr, errOracle, closeErr, nameErr, contextErr)
			}
		}()
		for {
			names, readErr := f.Readdirnames(128)
			for _, name := range names {
				if err := ctx.Err(); err != nil {
					return err
				}
				p := filepath.Join(dir, name)
				st, e := os.Lstat(p)
				if e != nil {
					return e
				}
				if st.IsDir() {
					if e = walk(p); e != nil {
						return e
					}
					continue
				}
				if !st.Mode().IsRegular() || !strings.HasPrefix(name, "f") {
					return errOracle
				}
				n, e := strconv.Atoi(strings.TrimPrefix(name, "f"))
				if e != nil || n < 0 || n >= t.files || t.path(root, n) != p {
					return errOracle
				}
				raw, ok := st.Sys().(*syscall.Stat_t)
				if !ok || raw.Nlink != 1 {
					return errOracle
				}
				data, e := readOracleLeaf(ctx, f, dir, name, parentStamp, st, t.body(n))
				if e != nil {
					return e
				}
				if raw.Blocks < 0 || raw.Blocks > int64(^uint64(0)>>1)/512 {
					return errOracle
				}
				allocated := raw.Blocks * 512
				digest := sha256.Sum256(data)
				rel, e := filepath.Rel(root, p)
				if e != nil {
					return e
				}
				if _, e = tx.ExecContext(ctx, "INSERT INTO observations VALUES(?,?,?,?,?,?,?)", n, []byte(rel), fmt.Sprint(raw.Dev), fmt.Sprint(raw.Ino), digest[:], st.Size(), allocated); e != nil {
					return errOracle
				}
				result.Files++
				result.Logical += st.Size()
				result.Allocated += allocated
				if result.Files%128 == 0 {
					if e = tx.Commit(); e != nil {
						return e
					}
					tx, e = db.BeginTx(ctx, nil)
					if e != nil {
						return e
					}
				}
			}
			if errors.Is(readErr, io.EOF) {
				break
			}
			if readErr != nil {
				return readErr
			}
		}
		if err := oracleDirectoryUnchanged(f, dir, parentStamp); err != nil {
			return err
		}
		return ctx.Err()
	}
	if err = walk(root); err != nil {
		return result, err
	}
	expectedDirs := t.levels()
	if t.shape == "healthy" {
		expectedDirs++
	}
	if result.Files != int64(t.files) || result.Directories != int64(expectedDirs) {
		return result, errOracle
	}
	if err = tx.Commit(); err != nil {
		return result, err
	}
	tx = nil
	rows, err := db.QueryContext(ctx, "SELECT ordinal,path,body FROM observations ORDER BY ordinal")
	if err != nil {
		return result, err
	}
	defer rows.Close()
	hash := sha256.New()
	next := 0
	for rows.Next() {
		var n int
		var p, b []byte
		if err = rows.Scan(&n, &p, &b); err != nil {
			return result, err
		}
		if n != next {
			return result, errOracle
		}
		next++
		var order [8]byte
		binary.BigEndian.PutUint64(order[:], uint64(n))
		hash.Write(order[:])
		hash.Write(p)
		hash.Write([]byte{0})
		hash.Write(b)
	}
	if rows.Err() != nil || next != t.files {
		return result, errOracle
	}
	result.BodiesSHA256 = hex.EncodeToString(hash.Sum(nil))
	return result, nil
}
func sameOracle(ctx context.Context, before, after string) error {
	db, err := sql.Open("sqlite", before)
	if err != nil {
		return err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err = db.ExecContext(ctx, "PRAGMA cache_size=-2048; PRAGMA mmap_size=0; PRAGMA temp_store=FILE"); err != nil {
		return err
	}
	if _, err = db.ExecContext(ctx, "ATTACH DATABASE ? AS after", after); err != nil {
		return err
	}
	if _, err = db.ExecContext(ctx, "PRAGMA after.cache_size=-2048; PRAGMA after.mmap_size=0"); err != nil {
		return err
	}
	var changed int64
	err = db.QueryRowContext(ctx, `SELECT count(*) FROM (SELECT * FROM (SELECT * FROM observations EXCEPT SELECT * FROM after.observations) UNION ALL SELECT * FROM (SELECT * FROM after.observations EXCEPT SELECT * FROM observations))`).Scan(&changed)
	if err != nil {
		return err
	}
	if changed != 0 {
		return errOracle
	}
	err = db.QueryRowContext(ctx, `SELECT count(*) FROM (SELECT * FROM (SELECT * FROM directory_observations EXCEPT SELECT * FROM after.directory_observations) UNION ALL SELECT * FROM (SELECT * FROM after.directory_observations EXCEPT SELECT * FROM directory_observations))`).Scan(&changed)
	if err != nil {
		return err
	}
	if changed != 0 {
		return errOracle
	}
	return nil
}
