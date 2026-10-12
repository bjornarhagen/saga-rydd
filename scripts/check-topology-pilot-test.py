#!/usr/bin/env python3
"""Generated fixtures for the native topology pilot receipt checker."""

import base64
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import struct
import sys
import tempfile
import unittest


REVISION = "463fa022b3f6c60b4fcd72aa543f39de305d77de"


def write_private(path, body):
    path.write_bytes(body)
    path.chmod(0o600)


def write_json(path, value):
    write_private(path, json.dumps(value, separators=(",", ":")).encode() + b"\n")


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def native_header(machine=62):
    ident = b"\x7fELF\x02\x01\x01" + b"\x00" * 9
    return struct.pack("<16sHHIQQQIHHHHHH", ident, 2, machine, 1,
                       0, 0, 0, 0, 64, 0, 0, 0, 0, 0)


def resource(scope, elapsed=10000000000):
    return {
        "available": True,
        "user_cpu_ns": 1000000,
        "system_cpu_ns": 500000,
        "peak_rss_bytes": 16 << 20,
        "launch_to_wait_or_phase_elapsed_ns": elapsed,
        "peak_rss_scope": scope,
    }


def saved_job(identity, root, path=".", started=False):
    return {
        "ID": identity, "Root": root, "Due": 1791936000000000000,
        "Attempts": 1 if started else 0, "LeaseUntil": 0,
        "Claimed": 1 if started else 0, "Kind": "inventory",
        "Status": "pending", "Error": "", "Lease": "",
        "Path": base64.b64encode(path.encode()).decode(),
        "Cursor": base64.b64encode(b"generated cursor").decode() if started else None,
    }


class PilotFixture:
    """Literal producer-shaped evidence, without importing checker constants."""

    def __init__(self, parent, shape="wide", completed=False):
        self.output = parent / "pilot"
        self.output.mkdir(mode=0o700)
        (self.output / "state").mkdir(mode=0o700)
        self.binary, self.helper = parent / "rydd", parent / "backgroundtopology"
        for path, label in ((self.binary, b"rydd fixture"),
                            (self.helper, b"topology fixture")):
            write_private(path, native_header() + label)
            path.chmod(0o700)
        self.shape, self.completed = shape, completed
        levels = 1 if shape == "wide" else 64
        self.metadata = {
            "contract": "build_metadata_v1", "version": "dev",
            "version_source": "linked_value", "executable_label_verified": True,
            "revision": REVISION, "source_status": "clean", "go_version": "go1.27.1",
            "goos": "linux", "goarch": "amd64", "cgo_enabled": False,
        }
        config = (
            "version = 1\nroots = [" + json.dumps(str(self.output / "node_modules"))
            + ", " + json.dumps(str(self.output / "healthy")) + "]\n"
            "[scan]\nwork_seconds = 1\ninterval_seconds = 1\n"
            "metadata_per_second = 100000\napi_attempts_per_second = 0\n"
            "metadata_attempts_per_day = 1099511627776\n"
            "read_bytes_per_second = 5242880\nread_bytes_per_day = 5368709120\n"
            "pause_on_battery = false\nmax_scan_chunks_per_day = 100000\n"
            "max_state_bytes = 1073741824\nadaptive_revisits = false\n"
            "compact_inventory = true\ncpu_session_charges = false\n"
            "cpu_charge_seconds_per_hour = 0\ncpu_charge_seconds_per_day = 0\n"
        ).encode()
        write_private(self.output / "state/config.toml", config)
        oracle_names = {
            "primary_before": "primary-before.sqlite",
            "primary_after": "primary-after.sqlite",
            "healthy_before": "healthy-before.sqlite",
            "healthy_after": "healthy-after.sqlite",
        }
        artifact_hashes = {}
        for key, name in oracle_names.items():
            path = self.output / name
            # The checker validates recorded artifact bytes, not SQLite rows.
            # Distinct before/after bytes must not be rejected as row changes.
            write_private(path, b"SQLite format 3\x00" + key.encode() + b"\x00" * 64)
            artifact_hashes[key] = digest(path)
        self.report = {
            "contract": "generated_background_topology_profile_v1",
            "shape": shape, "primary_file_observations_expected": 4096,
            "primary_file_bearing_levels": levels, "sampling_window_limit_seconds": 600,
            "graceful_stop_window_seconds": 15,
            "finite_profile": {
                "WorkSeconds": 1, "IntervalSeconds": 1, "MetadataPerSecond": 100000,
                "APIAttemptsPerSecond": 0, "MetadataAttemptsPerDay": 1099511627776,
                "ReadBytesPerSecond": 5242880, "ReadBytesPerDay": 5368709120,
                "PauseOnBattery": False, "MaxScanChunksPerDay": 100000,
                "MaxStateBytes": 1073741824, "AdaptiveRevisits": False,
                "CompactInventory": True, "CPUSessionCharges": False,
                "CPUChargeSecondsPerHour": 0, "CPUChargeSecondsPerDay": 0,
            },
            "initial_available_output_filesystem_bytes": 32 << 30,
            "GenerationNS": 100000000, "OracleBeforeNS": 200000000,
            "OracleAfterNS": 300000000,
            "Before": {
                "Files": 4096, "Directories": levels,
                "Logical": 128 if shape == "wide" else 2048,
                "Allocated": 16384 if shape == "wide" else 262144,
                "DirectoryAllocated": 4096,
                "BodiesSHA256": (
                    "1aa198556ccd1da8943cf39cef12f88c4431f6916e280ee3320120b602a41b0c"
                    if shape == "wide" else
                    "27c7bf37fc3c9a7b5a6143dffa96a3dc3b9365b8dacf557b93099322200498cf"
                ),
            },
            "Samples": [],
            "Workers": [resource("individual_child_lifetime"),
                        resource("individual_child_lifetime")],
            "Observer": resource("maximum_individual_observer_child"),
            "Generation": resource("helper_process_lifetime_through_phase_end", 100000000),
            "OracleBefore": resource("helper_process_lifetime_through_phase_end", 200000000),
            "OracleAfter": resource("helper_process_lifetime_through_phase_end", 300000000),
            "ControlLatencyNS": [2000000, 3000000, 2000000],
            "Completed": completed, "BodyIdentityOracleUnchanged": True,
            "PauseNoProgress": True, "SIGKILLProven": True, "RestartPreserved": True,
            "FairProgress": True, "FairCompletion": True,
            "ExactCachedTotals": completed, "ExactFutureDailyJobs": completed,
            "Outcome": "completed" if completed else "partial",
            "terminal_evidence_saved": True,
            "forced_direct_child_cancellation_requested": False,
            "Stage": "finished", "SourceRevision": REVISION,
            "SourceRevisionStatus": "declared", "DeclaredBuildMetadata": self.metadata,
            "SourceAuthenticationVerified": False,
            "HarnessBinarySHA256": digest(self.helper), "BinarySHA256": digest(self.binary),
            "ConfigSHA256": digest(self.output / "state/config.toml"),
            "ArtifactSHA256": artifact_hashes,
            "DefaultProfileAccepted": False, "HourlyTargetAccepted": False,
            "PhysicalIOVerified": False, "ProviderAccepted": False, "SoakAccepted": False,
            "Qualifiers": [
                "Finite generated zero-byte topology with bounded sentinel bodies; not representative content or defaults acceptance.",
                "Worker SELF CPU excludes generator, independent oracle and observer children; those costs are separate.",
                "Scanner/API accounting is not physical-I/O accounting. Entered native operations can outlast cancellation.",
                "Only new owned source roots are used; saved cursors, quotas and daily due times are not reset.",
                "Healthy source progress precedes primary EOF; healthy EOF precedes full primary cache/scratch drain.",
                "Concurrent development can affect elapsed time and host scheduling.",
                "Sampling ends at its declared window; source work can continue until the stop request is received. Full worker resources cover launch through Wait return, including separate stop/cleanup grace.",
                "Generator/oracle RSS is the parent process lifetime high-water through each phase. Observer RSS is the maximum individual CLI child, not combined simultaneous memory.",
                "Oracle comparisons request at most 4 MiB total named-database page cache, disable mmap, and use file-backed temporary sorting; temporary disk and helper overhead are separate from worker costs.",
                "Child cancellation addresses the guarded direct child only. No descendant/process-group cleanup is claimed.",
                "The initial output-filesystem capacity check is an observation, not a disk reservation. Allocation failures remain partial evidence. Generation and each oracle phase have a separate 1800-second deadline. A finite sample does not establish full million-entry drain, defaults or soak acceptance.",
                "Supplied-binary build metadata and revision are declared observations, not authenticated source provenance. Missing VCS metadata remains unknown; binary SHA identifies the observed bytes only.",
            ],
        }
        observations = [(256, 0 if shape == "wide" else 1, 32, 0, 1),
                        (4096, levels, 33, 1, 1)]
        if completed:
            observations.append((4096, levels, 33, 2, 0))
        for index, (files, directories, healthy, ready, scratch) in enumerate(observations):
            self.report["Samples"].append({
                "elapsed_ns": (index + 1) * 5000000000,
                "PrimaryFiles": files, "PrimaryCompleteDirs": directories,
                "HealthyCompleteDirs": healthy, "ReadyCaches": ready,
                "ScratchScopes": scratch, "Dispatch": 10 + index,
                "DBBytes": 262144, "WALBytes": 4096, "Wait": "cadence",
                "Charges": {
                    "reserved_attempts": 1000 + index * 100,
                    "observed_attempts": 10 + index,
                    "unknown_reserved_attempts": 0, "outstanding_reserved_attempts": 0,
                    "known_unused_reserved_attempts": 990 + index * 99,
                },
                "CPUStatus": "observed",
                "APICounters": {
                    "entry_inspections": 300 + index, "entry_rate_per_second": 100000,
                    "throttle_wait_ns": 0, "throttled": False, "stat_calls": 100 + index,
                    "directory_open_calls": 10, "directory_read_calls": 2,
                    "filesystem_stat_calls": 10, "mount_identity_calls": 0,
                    "path_resolution_calls": 2,
                },
                "DispatchDay": "2026-10-12",
            })
            summary = {
                "Files": files, "CompleteDirs": directories, "HealthyCompleteDirs": healthy,
                "ReadyCaches": ready, "Caches": 2, "Scratch": scratch,
                "Errors": 0, "Skips": 0, "Running": 0, "Dispatch": 10 + index, "Jobs": None,
            }
            write_json(self.output / ("sample-%03d-summary.json" % index), summary)
            if index == 0:
                jobs = [saved_job(1, 1, started=True), saved_job(2, 2, "branch31")]
            elif ready == 2:
                jobs = [saved_job(1, 1), saved_job(2, 2)]
            else:
                jobs = [saved_job(2, 2)]
            write_json(self.output / ("sample-%03d-jobs-00.json" % index), jobs)
        self.terminal = copy.deepcopy(summary)
        self.terminal["Jobs"] = copy.deepcopy(jobs)
        write_json(self.output / "saved-terminal.json", self.terminal)
        for index in range(2):
            write_private(self.output / ("worker-%d.stdout" % index), b"")
            write_private(self.output / ("worker-%d.stderr" % index), b"")
        self.commands = ["version", "state init", "status"]
        self.commands += ["status"] * len(observations)
        self.commands += ["pause", "status", "status", "status", "resume", "stop"]
        if completed:
            self.commands.append("report")
        self.save()
        self.save_commands()

    def save(self):
        write_json(self.output / "aggregate.json", self.report)

    def save_commands(self):
        for path in self.output.glob("command-*.*"):
            path.unlink()
        for index, command in enumerate(self.commands, 1):
            path = self.output / ("command-%03d.stdout" % index)
            if command is None:
                write_private(path, b"")
                diagnostic = b"retained final observer diagnostic\n"
            else:
                reply = {"api_version": 1, "ok": True, "command": command}
                if command == "version":
                    reply.update(version="dev", build_metadata=self.metadata)
                write_json(path, reply)
                diagnostic = b""
            write_private(self.output / ("command-%03d.stderr" % index), diagnostic)


class TopologyPilotCheckerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        source = Path(__file__).with_name("check-topology-pilot.py")
        spec = importlib.util.spec_from_file_location("topology_pilot_checker_for_test", source)
        cls.checker = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = cls.checker
        spec.loader.exec_module(cls.checker)

    def fixture(self, shape="wide", completed=False):
        temporary = tempfile.TemporaryDirectory(prefix="rydd-topology-checker-")
        self.addCleanup(temporary.cleanup)
        return PilotFixture(Path(temporary.name), shape, completed)

    def check(self, fixture, shape=None, revision=REVISION):
        return self.checker.check_pilot(fixture.output, fixture.binary, fixture.helper,
                                        shape or fixture.shape, revision)

    def refused(self, fixture, **arguments):
        with self.assertRaises(self.checker.Refused):
            self.check(fixture, **arguments)

    def assert_scalar_result(self, result, shape, outcome):
        self.assertIsInstance(result, dict)
        self.assertEqual(result["shape"], shape)
        self.assertEqual(result["outcome"], outcome)
        self.assertTrue(all(type(value) in (str, int, bool, type(None))
                            for value in result.values()))

    def test_accepts_wide_partial_with_source_eof_and_partial_cache(self):
        fixture = self.fixture()
        self.assert_scalar_result(self.check(fixture), "wide", "partial")
        self.assertEqual(fixture.report["Samples"][-1]["PrimaryFiles"], 4096)
        self.assertEqual(fixture.report["Samples"][-1]["ReadyCaches"], 1)

    def test_accepts_deep_partial(self):
        fixture = self.fixture("deep")
        self.assert_scalar_result(self.check(fixture), "deep", "partial")

    def test_accepts_early_null_charges_and_api_counters(self):
        fixture = self.fixture()
        fixture.report["Samples"][0].update(Charges=None, APICounters=None)
        fixture.save()
        self.assert_scalar_result(self.check(fixture), "wide", "partial")

    def test_accepts_completed(self):
        for shape in ("wide", "deep"):
            with self.subTest(shape=shape):
                fixture = self.fixture(shape, completed=True)
                self.assert_scalar_result(self.check(fixture), shape, "completed")
                self.assertEqual(len(fixture.report["Samples"]), 3)

    def test_terminal_drain_does_not_upgrade_partial_sample(self):
        fixture = self.fixture()
        fixture.terminal.update(ReadyCaches=2, Scratch=0,
                                Jobs=[saved_job(1, 1), saved_job(2, 2)])
        write_json(fixture.output / "saved-terminal.json", fixture.terminal)
        self.assert_scalar_result(self.check(fixture), "wide", "partial")

    def test_one_final_interrupted_partial_observer_is_qualified(self):
        fixture = self.fixture()
        fixture.commands.insert(fixture.commands.index("stop"), None)
        fixture.save_commands()
        result = self.check(fixture)
        self.assertEqual(result["unvalidated_final_observer_receipts"], 1)
        self.assert_scalar_result(result, "wide", "partial")

    def test_rejects_invalid_aggregate_evidence(self):
        changes = {
            "wrong_shape": lambda r: r.update(shape="deep"),
            "wrong_head": lambda r: r.update(SourceRevision="a" * 40),
            "wrong_profile": lambda r: r["finite_profile"].update(IntervalSeconds=2),
            "extra_profile_key": lambda r: r["finite_profile"].update(extra=0),
            "missing_profile_key": lambda r: r["finite_profile"].pop("CPUSessionCharges"),
            "dirty_metadata": lambda r: r["DeclaredBuildMetadata"].update(source_status="dirty"),
            "wrong_native_metadata": lambda r: r["DeclaredBuildMetadata"].update(goarch="arm64"),
            "missing_metadata": lambda r: r.update(DeclaredBuildMetadata=None),
            "bad_binary_hash": lambda r: r.update(BinarySHA256="0" * 64),
            "bad_helper_hash": lambda r: r.update(HarnessBinarySHA256="0" * 64),
            "bad_config_hash": lambda r: r.update(ConfigSHA256="0" * 64),
            "partial_completed_flag": lambda r: r.update(Completed=True),
            "partial_cached_totals": lambda r: r.update(ExactCachedTotals=True),
            "partial_future_jobs": lambda r: r.update(ExactFutureDailyJobs=True),
            "bool_integer": lambda r: r["Samples"][0].update(PrimaryFiles=True),
            "wrong_wait_type": lambda r: r["Samples"][0].update(Wait=False),
            "wrong_cpu_status_type": lambda r: r["Samples"][0].update(CPUStatus=0),
            "wrong_api_counter_type": lambda r: r["Samples"][0]["APICounters"].update(entry_inspections=True),
            "wrong_dispatch_day_type": lambda r: r["Samples"][0].update(DispatchDay=20261012),
            "wrong_qualifier_type": lambda r: r.update(Qualifiers=[False]),
            "missing_sample_key": lambda r: r["Samples"][0].pop("Charges"),
            "bad_charge_conservation": lambda r: r["Samples"][0]["Charges"].update(observed_attempts=999),
            "missing_fair_progress": lambda r: r["Samples"][0].update(HealthyCompleteDirs=1),
            "forced_cleanup": lambda r: r.update(forced_direct_child_cancellation_requested=True),
            "worker_count": lambda r: r["Workers"].pop(),
            "unavailable_worker_usage": lambda r: r["Workers"][0].update(available=False),
            "phase_elapsed_mismatch": lambda r: r["Generation"].update(launch_to_wait_or_phase_elapsed_ns=100000001),
            "physical_acceptance": lambda r: r.update(PhysicalIOVerified=True),
            "source_authentication": lambda r: r.update(SourceAuthenticationVerified=True),
            "extra_aggregate_key": lambda r: r.update(extra=True),
            "missing_aggregate_key": lambda r: r.pop("Stage"),
        }
        for name, change in changes.items():
            with self.subTest(case=name):
                fixture = self.fixture()
                change(fixture.report)
                fixture.save()
                self.refused(fixture)

    def test_rejects_each_missing_lifecycle_flag(self):
        for field in ("BodyIdentityOracleUnchanged", "PauseNoProgress", "SIGKILLProven",
                      "RestartPreserved", "FairProgress", "FairCompletion"):
            with self.subTest(field=field):
                fixture = self.fixture()
                fixture.report[field] = False
                fixture.save()
                self.refused(fixture)

    def test_rejects_contradictory_completed_evidence(self):
        for field in ("Completed", "ExactCachedTotals", "ExactFutureDailyJobs"):
            with self.subTest(field=field):
                fixture = self.fixture(completed=True)
                fixture.report[field] = False
                fixture.save()
                self.refused(fixture)

    def test_rejects_receipt_and_artifact_faults(self):
        changes = {
            "missing_command": lambda f: (f.output / "command-003.stdout").unlink(),
            "missing_stderr": lambda f: (f.output / "command-003.stderr").unlink(),
            "missing_page": lambda f: (
                write_json(f.output / "sample-000-jobs-01.json", [saved_job(3, 1, "branch32")]),
                (f.output / "sample-000-jobs-00.json").unlink(),
            ),
            "missing_summary": lambda f: (f.output / "sample-000-summary.json").unlink(),
            "missing_terminal": lambda f: (f.output / "saved-terminal.json").unlink(),
            "missing_oracle": lambda f: (f.output / "primary-after.sqlite").unlink(),
            "changed_oracle": lambda f: write_private(f.output / "healthy-after.sqlite", b"changed"),
            "changed_config": lambda f: write_private(f.output / "state/config.toml", b"changed\n"),
            "missing_worker_stream": lambda f: (f.output / "worker-1.stdout").unlink(),
            "invalid_present_job": lambda f: write_json(f.output / "sample-000-jobs-00.json", [saved_job(True, 1)]),
            "unallowed_command": lambda f: write_json(f.output / "command-003.stdout", {"api_version": 1, "ok": True, "command": "hash"}),
            "failed_control": lambda f: write_json(f.output / "command-006.stdout", {"api_version": 1, "ok": False, "command": "pause"}),
        }
        for name, change in changes.items():
            with self.subTest(case=name):
                fixture = self.fixture()
                change(fixture)
                self.refused(fixture)

    def test_rejects_wrong_binary_headers_and_current_bytes(self):
        for name, path_name, body in (
            ("binary_machine", "binary", native_header(183)),
            ("helper_machine", "helper", native_header(183)),
            ("binary_class", "binary", b"\x7fELF\x01\x01" + b"\x00" * 58),
            ("binary_endian", "binary", b"\x7fELF\x02\x02" + b"\x00" * 58),
            ("changed_binary", "binary", native_header() + b"changed current bytes"),
        ):
            with self.subTest(case=name):
                fixture = self.fixture()
                path = getattr(fixture, path_name)
                write_private(path, body)
                path.chmod(0o700)
                if name != "changed_binary":
                    key = "BinarySHA256" if path_name == "binary" else "HarnessBinarySHA256"
                    fixture.report[key] = digest(path)
                    fixture.save()
                self.refused(fixture)

    def test_rejects_json_and_file_bounds(self):
        for name in ("malformed_json", "duplicate_key", "trailing_json", "aggregate_limit",
                     "version_limit", "command_limit", "stderr_limit", "job_page_limit",
                     "binary_limit", "oracle_limit", "config_limit"):
            with self.subTest(case=name):
                fixture = self.fixture()
                aggregate = fixture.output / "aggregate.json"
                if name == "malformed_json":
                    write_private(aggregate, b"{")
                elif name == "duplicate_key":
                    write_private(aggregate, b'{"contract":"duplicate",' + aggregate.read_bytes()[1:])
                elif name == "trailing_json":
                    write_private(aggregate, aggregate.read_bytes() + b"{}")
                else:
                    bounded_files = {
                        "aggregate_limit": (aggregate, 2 << 20),
                        "version_limit": (fixture.output / "command-001.stdout", 16 << 10),
                        "command_limit": (fixture.output / "command-003.stdout", 256 << 10),
                        "stderr_limit": (fixture.output / "command-003.stderr", 64 << 10),
                        "job_page_limit": (fixture.output / "sample-000-jobs-00.json", 2 << 20),
                        "binary_limit": (fixture.binary, 64 << 20),
                        "oracle_limit": (fixture.output / "primary-before.sqlite", 32 << 20),
                        "config_limit": (fixture.output / "state/config.toml", 1 << 20),
                    }
                    path, limit = bounded_files[name]
                    with path.open("r+b") as stream:
                        stream.truncate(limit + 1)
                self.refused(fixture)

    def test_rejects_interrupted_observer_outside_partial_stop_boundary(self):
        for name in ("two_observers", "before_resume", "completed", "missing_stop"):
            with self.subTest(case=name):
                fixture = self.fixture(completed=name == "completed")
                boundary = fixture.commands.index("resume" if name == "before_resume" else "stop")
                fixture.commands.insert(boundary, None)
                if name == "two_observers":
                    fixture.commands.insert(boundary, None)
                if name == "missing_stop":
                    fixture.commands.remove("stop")
                fixture.save_commands()
                self.refused(fixture)


if __name__ == "__main__":
    unittest.main()
