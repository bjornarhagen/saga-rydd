//go:build !darwin && !linux

package doctor

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
)

func TestDoctorUnsupportedPlatformBeforeCallback(t *testing.T) {
	called := false
	_, err := Observe(context.Background(), Request{DataDirectory: filepath.Join(testAbsoluteRoot(), "generated", "absent"), Catalog: testCatalog()}, func([]byte) (Configuration, error) { called = true; return Configuration{}, nil })
	if !errors.Is(err, ErrPlatform) || called {
		t.Fatal(err, called)
	}
}
