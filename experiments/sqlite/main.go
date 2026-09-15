// This isolated experiment never reads user files. All rows are synthetic.
package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"time"
)

func openDB(path string) (*sql.DB, error) {
	db, err := sql.Open(driverName, path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	for _, pragma := range []string{"journal_mode=WAL", "synchronous=FULL", "cache_size=-4096", "busy_timeout=1000", "wal_autocheckpoint=1000"} {
		if _, err := db.Exec("PRAGMA " + pragma); err != nil {
			db.Close()
			return nil, err
		}
	}
	return db, nil
}

func main() {
	rows := flag.Int("rows", 100000, "synthetic entries to insert")
	flag.Parse()
	if *rows < 1 {
		panic("rows must be positive")
	}
	if err := run(*rows); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(count int) error {
	dir, err := os.MkdirTemp("", "rydd-sqlite-experiment-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "inventory.sqlite3")
	db, err := openDB(path)
	if err != nil {
		return err
	}
	defer db.Close()
	_, err = db.Exec(`CREATE TABLE entries (
 id INTEGER PRIMARY KEY, path BLOB NOT NULL UNIQUE, parent INTEGER NOT NULL,
 size INTEGER NOT NULL, allocated INTEGER NOT NULL, mtime INTEGER NOT NULL,
 device INTEGER NOT NULL, inode INTEGER NOT NULL, generation INTEGER NOT NULL);
 CREATE INDEX entries_size ON entries(size); CREATE INDEX entries_parent ON entries(parent);`)
	if err != nil {
		return err
	}
	var sqliteVersion string
	if err := db.QueryRow("SELECT sqlite_version()").Scan(&sqliteVersion); err != nil {
		return err
	}
	started := time.Now()
	for base := 0; base < count; base += 256 {
		tx, err := db.Begin()
		if err != nil {
			return err
		}
		stmt, err := tx.Prepare("INSERT INTO entries VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
		if err != nil {
			tx.Rollback()
			return err
		}
		for i := base; i < min(base+256, count); i++ {
			path := []byte(fmt.Sprintf("/synthetic/development/project-%06d/generated/dependencies/package-%08d/file.dat", i/100, i))
			_, err = stmt.Exec(i+1, path, i/100, (i%8192+1)*4096, (i%8192+1)*4096, 1700000000000000000+i, 1, i+1, 1)
			if err != nil {
				stmt.Close()
				tx.Rollback()
				return err
			}
		}
		stmt.Close()
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	insertSeconds := time.Since(started).Seconds()
	queryStarted := time.Now()
	var groups int
	if err := db.QueryRow("SELECT count(*) FROM (SELECT size FROM entries GROUP BY size HAVING count(*) > 1)").Scan(&groups); err != nil {
		return err
	}
	queryMS := float64(time.Since(queryStarted).Microseconds()) / 1000
	if _, err := db.Exec("PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		return err
	}
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return err
	}
	rss := int64(usage.Maxrss)
	if runtime.GOOS != "darwin" {
		rss *= 1024
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{
		"driver": driverName, "sqlite": sqliteVersion, "go": runtime.Version(),
		"platform": runtime.GOOS + "/" + runtime.GOARCH, "rows": count, "batch": 256,
		"insert_seconds": insertSeconds, "size_groups_query_ms": queryMS, "size_groups": groups,
		"database_bytes": info.Size(), "peak_rss_bytes": rss,
	})
}
