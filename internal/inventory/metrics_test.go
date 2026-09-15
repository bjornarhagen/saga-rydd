package inventory

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestMetricsIncludeTraversalFailuresAndRevalidation(t *testing.T) {
	s, j, root := scannerFixture(t)
	before := s.Metrics()
	if before.StatCalls == 0 || before.PathResolutionCalls == 0 {
		t.Fatal("startup validation missing", before)
	}
	write(t, filepath.Join(root, "file"))
	batch := next(t, s, j)
	after := s.Metrics()
	// One child observation entails both traversal and final path validation.
	if len(batch.Entries) != 1 || after.StatCalls-before.StatCalls <= 1 || after.DirectoryOpenCalls <= before.DirectoryOpenCalls || after.DirectoryReadCalls != before.DirectoryReadCalls+1 || after.PathResolutionCalls < before.PathResolutionCalls+2 || after.FilesystemStatCalls <= before.FilesystemStatCalls {
		t.Fatal(before, after)
	}
	missing := filepath.Join(root, "missing")
	s.roots[missing] = true
	j.RootPath = []byte(missing)
	before = s.Metrics()
	batch, err := s.Next(context.Background(), j)
	if err != nil || batch.Fault == "" {
		t.Fatal(batch, err)
	}
	after = s.Metrics()
	if after.PathResolutionCalls != before.PathResolutionCalls+1 || after.DirectoryReadCalls != before.DirectoryReadCalls {
		t.Fatal("failed resolution was not accounted separately", before, after)
	}
	// A rejected component still consumes a stat attempt, but never an open.
	before = s.Metrics()
	if _, err := s.openat(-1, "missing"); err == nil {
		t.Fatal("invalid descriptor accepted")
	}
	after = s.Metrics()
	if after.StatCalls != before.StatCalls+1 || after.DirectoryOpenCalls != before.DirectoryOpenCalls {
		t.Fatal(before, after)
	}
}

func TestMetricsDoNotWaitForScannerLock(t *testing.T) {
	s, _, _ := scannerFixture(t)
	s.mu.Lock()
	done := make(chan Metrics, 1)
	go func() { done <- s.Metrics() }()
	select {
	case <-done:
	case <-time.After(time.Second):
		s.mu.Unlock()
		t.Fatal("metrics blocked on scanner lock")
	}
	s.mu.Unlock()
}

func TestMetricsDuringScan(t *testing.T) {
	s, j, root := scannerFixture(t)
	if err := os.Mkdir(filepath.Join(root, "child"), 0700); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 10000; i++ {
			s.Metrics()
		}
	}()
	next(t, s, j)
	<-done
	if s.Metrics().DirectoryReadCalls != 1 {
		t.Fatal(s.Metrics())
	}
}
