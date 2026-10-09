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
	walk = func(dir string) error {
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
		f, err := os.Open(dir)
		if err != nil {
			return err
		}
		defer f.Close()
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
				body, e := os.Open(p)
				if e != nil {
					return e
				}
				data, e := io.ReadAll(io.LimitReader(body, 33))
				ce := body.Close()
				if e != nil || ce != nil || !bytes.Equal(data, t.body(n)) {
					return errOracle
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
		return nil
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
