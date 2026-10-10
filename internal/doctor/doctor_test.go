package doctor

import (
	"encoding/binary"
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

func testCatalog() HeaderCatalog { return HeaderCatalog{0x52594444, 4, 16} }

func testAbsoluteRoot() string {
	if filepath.Separator == '\\' {
		return "C:\\"
	}
	return string(filepath.Separator)
}

func testHeader(version uint32) []byte {
	b := make([]byte, HeaderBytes)
	copy(b, "SQLite format 3\x00")
	binary.BigEndian.PutUint16(b[16:18], 4096)
	b[18], b[19] = 1, 1
	binary.BigEndian.PutUint32(b[60:64], version)
	binary.BigEndian.PutUint32(b[68:72], testCatalog().ApplicationID)
	return b
}

func TestDoctorDirectoryDepthBoundary(t *testing.T) {
	for _, n := range []int{64, 65} {
		path := testAbsoluteRoot() + strings.Repeat("d"+string(filepath.Separator), n-1) + "d"
		err := ValidateDirectory(path)
		if (n == 64) != (err == nil) {
			t.Fatalf("depth %d: %v", n, err)
		}
	}
	for _, path := range []string{"", "/", "relative", "/a/../b", "/a/", "/a\x00b", "/a\xffb", "/" + strings.Repeat("a", MaxPathBytes)} {
		if !errors.Is(ValidateDirectory(path), ErrArguments) {
			t.Fatalf("admitted %q", path)
		}
	}
	long := testAbsoluteRoot() + strings.Repeat("a", MaxPathBytes-len(testAbsoluteRoot()))
	if err := ValidateDirectory(long); err != nil {
		t.Fatal(err)
	}
	limits := fixedLimits()
	if limits.DirectoryDescriptors != 65 || limits.RegularDescriptors != 2 || limits.RetainedDescriptors != 67 || limits.Children != 6 {
		t.Fatalf("limits: %+v", limits)
	}
}

func TestDoctorHeaderObservations(t *testing.T) {
	cases := []struct {
		name   string
		mutate func([]byte)
		want   string
	}{
		{"compatible", func([]byte) {}, "declared_reader_range"},
		{"legacy", func(b []byte) { binary.BigEndian.PutUint32(b[60:64], 3) }, "legacy_declared"},
		{"future", func(b []byte) { binary.BigEndian.PutUint32(b[60:64], 17) }, "outside_declared_range"},
		{"zero", func(b []byte) { binary.BigEndian.PutUint32(b[60:64], 0) }, "outside_declared_range"},
		{"foreign", func(b []byte) { binary.BigEndian.PutUint32(b[68:72], 1) }, "foreign_header"},
		{"magic", func(b []byte) { b[0] = 0 }, "malformed_header"},
		{"bad_page", func(b []byte) { binary.BigEndian.PutUint16(b[16:18], 513) }, "malformed_header"},
		{"bad_write", func(b []byte) { b[18] = 3 }, "malformed_header"},
		{"wal_header", func(b []byte) { b[18], b[19] = 2, 2 }, "declared_reader_range"},
		{"largest_page", func(b []byte) { binary.BigEndian.PutUint16(b[16:18], 1) }, "declared_reader_range"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := testHeader(16)
			tc.mutate(b)
			h, got := headerObservation(b, testCatalog())
			if h == nil || got != tc.want {
				t.Fatalf("%+v / %s", h, got)
			}
			if tc.name == "largest_page" && h.PageSize != 65536 {
				t.Fatal(h.PageSize)
			}
		})
	}
	if h, status := headerObservation(testHeader(16)[:99], testCatalog()); h != nil || status != "incomplete_header" {
		t.Fatal(h, status)
	}
	r := newReport("/generated")
	if r.CurrentStateSchema != nil || r.WorkerActivity != "unknown" || r.LockAvailability != "unknown" || r.LedgerIntegrity != "not_checked" || len(r.Checks) != 7 {
		t.Fatalf("unknowns: %+v", r)
	}
	if r.Effects != (Effects{}) {
		t.Fatalf("effects: %+v", r.Effects)
	}
}

func TestDoctorGuidanceContracts(t *testing.T) {
	r := newReport("/generated/private")
	r.Status = "attention_required"
	r.Checks[0].Status = "present"
	r.Checks[0].Metadata = &Metadata{IdentityRechecked: true}
	safe, zero := true, int64(0)
	r.Checks[1].Status = "valid_config"
	r.Checks[1].Metadata = &Metadata{Suitable: &safe, IdentityRechecked: true}
	r.Checks[2].Status = "absent"
	for _, i := range []int{3, 4, 5} {
		r.Checks[i].Status = "absent"
	}
	find := func(id string) *Guidance {
		for _, g := range guidance(r) {
			if g.ID == id {
				value := g
				return &value
			}
		}
		return nil
	}
	g := find("explicit_state_initialization_or_migration")
	if g == nil || g.Automatic || !g.Inferred || !strings.Contains(g.Effect, "MUTATING") || len(g.Preconditions) != 3 {
		t.Fatalf("init: %+v", g)
	}
	if len(guidance(r)) > 5 {
		t.Fatal("guidance bound")
	}
	r.Checks[3].Status = "present"
	r.Checks[3].Metadata = &Metadata{Suitable: &safe, IdentityRechecked: true, LogicalBytes: &zero}
	if find("explicit_state_initialization_or_migration") == nil {
		t.Fatal("safe zero metadata should permit conditional guidance")
	}
	nonzero := int64(1)
	r.Checks[3].Metadata.LogicalBytes = &nonzero
	if find("explicit_state_initialization_or_migration") != nil {
		t.Fatal("nonempty sidecar")
	}
	r.Checks[3].Status = "absent"
	r.Status = "incomplete"
	if find("explicit_state_initialization_or_migration") != nil {
		t.Fatal("incomplete init guidance")
	}
	r.Checks[1].Metadata.IdentityRechecked = false
	if find("check_selected_configuration") != nil {
		t.Fatal("unrechecked config guidance")
	}
	for _, g := range guidance(r) {
		if g.Automatic || !g.Inferred {
			t.Fatal(g)
		}
	}
}
