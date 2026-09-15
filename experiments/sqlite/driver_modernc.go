//go:build !sqlite_cgo

package main

import _ "modernc.org/sqlite"

const driverName = "sqlite"
