//go:build sqlite_cgo

package main

import _ "github.com/mattn/go-sqlite3"

const driverName = "sqlite3"
