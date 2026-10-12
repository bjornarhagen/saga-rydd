#!/usr/bin/env python3
"""Check one generated native Linux pilot; print only qualified scalar evidence."""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import struct
import sys

JSON_LIMIT = 2 << 20
COMMAND_LIMIT = 256 << 10
STREAM_LIMIT = 64 << 10
BINARY_LIMIT = 64 << 20
ORACLE_LIMIT = 32 << 20
PROFILE = {
    "WorkSeconds": 1, "IntervalSeconds": 1, "MetadataPerSecond": 100000,
    "APIAttemptsPerSecond": 0, "MetadataAttemptsPerDay": 1 << 40,
    "ReadBytesPerSecond": 5 << 20, "ReadBytesPerDay": 5 << 30,
    "PauseOnBattery": False, "MaxScanChunksPerDay": 100000,
    "MaxStateBytes": 1 << 30, "AdaptiveRevisits": False,
    "CompactInventory": True, "CPUSessionCharges": False,
    "CPUChargeSecondsPerHour": 0, "CPUChargeSecondsPerDay": 0,
}
RESULT_FIELDS = set("contract shape primary_file_observations_expected primary_file_bearing_levels sampling_window_limit_seconds graceful_stop_window_seconds finite_profile initial_available_output_filesystem_bytes GenerationNS OracleBeforeNS OracleAfterNS Before Samples Workers Observer Generation OracleBefore OracleAfter ControlLatencyNS Completed BodyIdentityOracleUnchanged PauseNoProgress SIGKILLProven RestartPreserved FairProgress FairCompletion ExactCachedTotals ExactFutureDailyJobs Outcome terminal_evidence_saved forced_direct_child_cancellation_requested Stage SourceRevision SourceRevisionStatus DeclaredBuildMetadata SourceAuthenticationVerified HarnessBinarySHA256 BinarySHA256 ConfigSHA256 ArtifactSHA256 DefaultProfileAccepted HourlyTargetAccepted PhysicalIOVerified ProviderAccepted SoakAccepted Qualifiers".split())
SAMPLE_FIELDS = set("elapsed_ns PrimaryFiles PrimaryCompleteDirs HealthyCompleteDirs ReadyCaches ScratchScopes Dispatch DBBytes WALBytes Wait Charges CPUStatus APICounters DispatchDay".split())
SAVED_FIELDS = set("Files CompleteDirs HealthyCompleteDirs ReadyCaches Caches Scratch Errors Skips Running Dispatch Jobs".split())
USAGE_FIELDS = set("available user_cpu_ns system_cpu_ns peak_rss_bytes launch_to_wait_or_phase_elapsed_ns peak_rss_scope".split())
METADATA_FIELDS = set("contract version version_source executable_label_verified revision source_status go_version goos goarch cgo_enabled".split())
METRICS_FIELDS = set("entry_inspections entry_rate_per_second throttle_wait_ns throttled stat_calls directory_open_calls directory_read_calls filesystem_stat_calls mount_identity_calls path_resolution_calls".split())
JOB_FIELDS = set("ID Root Due Attempts LeaseUntil Claimed Kind Status Error Lease Path Cursor".split())
WITNESSES = "BodyIdentityOracleUnchanged PauseNoProgress SIGKILLProven RestartPreserved FairProgress FairCompletion terminal_evidence_saved".split()
ACCEPTANCE = "SourceAuthenticationVerified DefaultProfileAccepted HourlyTargetAccepted PhysicalIOVerified ProviderAccepted SoakAccepted forced_direct_child_cancellation_requested".split()
ORACLES = ("primary_before", "primary_after", "healthy_before", "healthy_after")
BODY_DIGEST = {"wide": "1aa198556ccd1da8943cf39cef12f88c4431f6916e280ee3320120b602a41b0c", "deep": "27c7bf37fc3c9a7b5a6143dffa96a3dc3b9365b8dacf557b93099322200498cf"}


class Refused(ValueError):
    pass


def need(condition):
    if not condition:
        raise Refused("pilot evidence refused")


def integer(value, low=0, high=(1 << 63) - 1):
    need(type(value) is int and low <= value <= high)
    return value


def keys(value, expected):
    need(type(value) is dict and set(value) == expected)


def signature(s):
    return (s.st_dev, s.st_ino, s.st_mode, s.st_nlink, s.st_size, s.st_mtime_ns, s.st_ctime_ns)


def observed_file(path, limit, collect=True):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        before = os.fstat(fd)
        need(stat.S_ISREG(before.st_mode) and before.st_nlink == 1 and 0 <= before.st_size <= limit)
        digest, body, header = hashlib.sha256(), bytearray(), bytearray()
        remaining = before.st_size
        while remaining:
            chunk = os.read(fd, min(65536, remaining))
            need(bool(chunk))
            remaining -= len(chunk)
            digest.update(chunk)
            if len(header) < 64:
                header.extend(chunk[:64-len(header)])
            if collect:
                body.extend(chunk)
        need(not os.read(fd, 1))
        need(signature(before) == signature(os.fstat(fd)) == signature(os.lstat(path)))
        return bytes(body), digest.hexdigest(), before.st_size, bytes(header), before.st_mode
    finally:
        os.close(fd)


def unique(pairs):
    result = {}
    for name, value in pairs:
        need(name not in result)
        result[name] = value
    return result


def json_value(body):
    def bad_constant(_):
        raise Refused("non-finite JSON refused")
    try:
        result = json.loads(body.decode("utf-8"), object_pairs_hook=unique, parse_constant=bad_constant)
    except (ValueError, UnicodeError, RecursionError) as error:
        raise Refused("invalid JSON refused") from error
    def bounded(value, depth=0):
        need(depth <= 8)
        if type(value) is dict:
            need(len(value) <= 128)
            for key, child in value.items():
                need(len(key) <= 128)
                bounded(child, depth+1)
        elif type(value) is list:
            need(len(value) <= 1024)
            for child in value:
                bounded(child, depth+1)
    bounded(result)
    return result


def read_json(path, limit=JSON_LIMIT):
    return json_value(observed_file(path, limit)[0])


def executable(path, expected):
    _, digest, size, h, mode = observed_file(path, BINARY_LIMIT, False)
    need(size >= 64 and mode & 0o111 and digest == expected)
    need(h[:7] == b"\x7fELF\x02\x01\x01" and h[7] in (0, 3))
    need(struct.unpack_from("<HHI", h, 16) == (2, 62, 1) and struct.unpack_from("<H", h, 52)[0] == 64)
    return digest, size


def usage(value, scope):
    keys(value, USAGE_FIELDS)
    need(value["available"] is True and value["peak_rss_scope"] == scope)
    for name in ("user_cpu_ns", "system_cpu_ns", "peak_rss_bytes", "launch_to_wait_or_phase_elapsed_ns"):
        integer(value[name], 1 if name in ("peak_rss_bytes", "launch_to_wait_or_phase_elapsed_ns") else 0)


def jobs(rows):
    need(type(rows) is list and len(rows) <= 129)
    for row in rows:
        keys(row, JOB_FIELDS)
        for name in ("ID", "Root", "Due", "Attempts", "LeaseUntil", "Claimed"):
            integer(row[name], 1 if name in ("ID", "Root") else 0)
        need(row["Claimed"] <= 1 and row["Kind"] == "inventory" and row["Status"] in ("pending", "running"))
        for name, limit in (("Error",2048),("Lease",128)):
            need(type(row[name]) is str and len(row[name].encode()) <= limit)
        for name, limit in (("Path",512),("Cursor",65536)):
            value = row[name]
            if name == "Cursor" and value is None:
                continue
            need(type(value) is str and len(value) <= 4*((limit+2)//3))
            try:
                raw = base64.b64decode(value, validate=True)
            except ValueError as error:
                raise Refused("invalid saved bytes refused") from error
            need(len(raw) <= limit and (name != "Path" or len(raw) > 0))


def saved(value, levels, sampled):
    keys(value, SAVED_FIELDS)
    for name in SAVED_FIELDS - {"Jobs"}:
        integer(value[name])
    need(value["Files"] <= 4096 and value["CompleteDirs"] <= levels and value["HealthyCompleteDirs"] <= 33)
    need(value["Caches"] <= 2 and value["ReadyCaches"] <= value["Caches"] and value["Scratch"] <= 5)
    need(value["Errors"] == 0 and value["Skips"] == 0)
    if sampled:
        need(value["Jobs"] is None)
    else:
        jobs(value["Jobs"])
        need(value["Running"] == 0)


def drained(value, levels):
    return value["Files"] == 4096 and value["CompleteDirs"] == levels and value["HealthyCompleteDirs"] == 33 and value["Caches"] == value["ReadyCaches"] == 2 and value["Scratch"] == value["Errors"] == value["Skips"] == value["Running"] == 0


def _check_pilot(output, binary, helper, shape, revision):
    output, binary, helper = Path(output), Path(binary), Path(helper)
    need(shape in ("wide", "deep") and bool(re.fullmatch(r"[0-9a-f]{40}", revision)))
    need(output.is_absolute() and binary.is_absolute() and helper.is_absolute())
    need(stat.S_ISDIR(os.lstat(output).st_mode))
    r = read_json(output / "aggregate.json")
    keys(r, RESULT_FIELDS)
    levels = 1 if shape == "wide" else 64
    need(r["contract"] == "generated_background_topology_profile_v1" and r["shape"] == shape and r["Stage"] == "finished")
    for name, wanted in (("primary_file_observations_expected",4096), ("primary_file_bearing_levels",levels), ("sampling_window_limit_seconds",600), ("graceful_stop_window_seconds",15)):
        need(integer(r[name]) == wanted)
    keys(r["finite_profile"], set(PROFILE))
    need(all(type(r["finite_profile"][k]) is type(v) and r["finite_profile"][k] == v for k,v in PROFILE.items()))
    integer(r["initial_available_output_filesystem_bytes"], 16 << 30)
    need(all(r[k] is True for k in WITNESSES) and all(r[k] is False for k in ACCEPTANCE))
    need(type(r["Completed"]) is bool and r["Outcome"] == ("completed" if r["Completed"] else "partial"))
    need(r["ExactCachedTotals"] is r["Completed"] and r["ExactFutureDailyJobs"] is r["Completed"])
    need(type(r["Qualifiers"]) is list and len(r["Qualifiers"]) <= 16 and all(type(q) is str and len(q.encode()) <= 2048 for q in r["Qualifiers"]))
    need(r["SourceRevision"] == revision and r["SourceRevisionStatus"] == "declared")
    m = r["DeclaredBuildMetadata"]
    keys(m, METADATA_FIELDS)
    need(m["contract"] == "build_metadata_v1" and m["version"] == "dev" and m["version_source"] == "linked_value" and m["executable_label_verified"] is True)
    need(m["revision"] == revision and m["source_status"] == "clean" and m["go_version"] == "go1.27.1" and m["goos"] == "linux" and m["goarch"] == "amd64" and m["cgo_enabled"] is False)
    binary_hash, binary_bytes = executable(binary, r["BinarySHA256"])
    helper_hash, helper_bytes = executable(helper, r["HarnessBinarySHA256"])
    need(observed_file(output / "state/config.toml", 1 << 20, False)[1] == r["ConfigSHA256"])
    keys(r["ArtifactSHA256"], set(ORACLES))
    for name in ORACLES:
        _, digest, _, header, _ = observed_file(output / (name.replace("_", "-")+".sqlite"), ORACLE_LIMIT, False)
        need(header[:16] == b"SQLite format 3\x00" and digest == r["ArtifactSHA256"][name])
    keys(r["Before"], set("Files Directories Logical Allocated DirectoryAllocated BodiesSHA256".split()))
    before = r["Before"]
    for name in ("Files", "Directories", "Logical", "Allocated", "DirectoryAllocated"):
        integer(before[name])
    need(before["Files"] == 4096 and before["Directories"] == levels and before["Logical"] == (128 if shape == "wide" else 2048) and before["BodiesSHA256"] == BODY_DIGEST[shape])
    need(type(r["Workers"]) is list and len(r["Workers"]) == 2)
    for i, u in enumerate(r["Workers"]):
        usage(u, "individual_child_lifetime")
        for suffix in ("stdout", "stderr"):
            observed_file(output / ("worker-%d.%s" % (i,suffix)), STREAM_LIMIT)
    for key in ("Generation", "OracleBefore", "OracleAfter"):
        usage(r[key], "helper_process_lifetime_through_phase_end")
        need(integer(r[key+"NS"], 1) == r[key]["launch_to_wait_or_phase_elapsed_ns"])
    usage(r["Observer"], "maximum_individual_observer_child")
    need(type(r["ControlLatencyNS"]) is list and len(r["ControlLatencyNS"]) == 3)
    for latency in r["ControlLatencyNS"]:
        integer(latency, 1)
    samples = r["Samples"]
    need(type(samples) is list and 1 <= len(samples) <= 721)
    names = set()
    with os.scandir(output) as entries:
        for entry in entries:
            need(len(names) < 12000)
            names.add(entry.name)
    wanted_summaries, wanted_pages = set(), set()
    progress = completion = full = False
    previous_elapsed = -1
    for i,s in enumerate(samples):
        keys(s, SAMPLE_FIELDS)
        need(integer(s["elapsed_ns"]) > previous_elapsed)
        previous_elapsed = s["elapsed_ns"]
        summary_name = "sample-%03d-summary.json" % i
        wanted_summaries.add(summary_name)
        v = read_json(output / summary_name)
        saved(v, levels, True)
        for field, saved_field in (("PrimaryFiles","Files"),("PrimaryCompleteDirs","CompleteDirs"),("HealthyCompleteDirs","HealthyCompleteDirs"),("ReadyCaches","ReadyCaches"),("ScratchScopes","Scratch"),("Dispatch","Dispatch")):
            need(integer(s[field]) == v[saved_field])
        for field in ("DBBytes", "WALBytes"):
            integer(s[field])
        for field in ("Wait", "CPUStatus"):
            need(type(s[field]) is str and len(s[field]) <= 64)
        need(type(s["DispatchDay"]) is str and (s["DispatchDay"] == "" or bool(re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}",s["DispatchDay"]))))
        if s["APICounters"] is not None:
            keys(s["APICounters"], METRICS_FIELDS)
            for field,value in s["APICounters"].items():
                if field == "throttled":
                    need(type(value) is bool)
                else:
                    integer(value, 0, (1 << 64)-1)
        c = s["Charges"]
        if c is not None:
            keys(c, set("reserved_attempts observed_attempts unknown_reserved_attempts outstanding_reserved_attempts known_unused_reserved_attempts".split()))
            need(integer(c["reserved_attempts"]) == sum(integer(c[k]) for k in c if k != "reserved_attempts"))
        progress |= v["HealthyCompleteDirs"] > 1 and v["CompleteDirs"] < levels
        completion |= v["HealthyCompleteDirs"] == 33 and not drained(v, levels)
        full |= drained(v, levels)
        pages = sorted(n for n in names if n.startswith("sample-%03d-jobs-" % i))
        need(len(pages) <= 9 and pages == ["sample-%03d-jobs-%02d.json" % (i,p) for p in range(len(pages))])
        count = 0
        for name in pages:
            wanted_pages.add(name)
            page = read_json(output / name)
            need(type(page) is list and 1 <= len(page) <= 16)
            jobs(page)
            count += len(page)
        need(count <= 129)
    need(progress and completion and (not r["Completed"] or full))
    need({n for n in names if n.startswith("sample-") and n.endswith("-summary.json")} == wanted_summaries)
    need({n for n in names if n.startswith("sample-") and "-jobs-" in n} == wanted_pages)
    terminal = read_json(output / "saved-terminal.json")
    saved(terminal, levels, False)
    need(not r["Completed"] or drained(terminal, levels))
    commands = sorted((n for n in names if n.startswith("command-") and n.endswith(".stdout")), key=lambda n: int(n[8:-7]))
    need(len(samples)+9+int(r["Completed"]) <= len(commands) <= 2000)
    need(commands == ["command-%03d.stdout" % i for i in range(1,len(commands)+1)])
    need({n for n in names if n.startswith("command-")} == set(commands) | {n[:-6]+"stderr" for n in commands})
    kinds, unvalidated = [], 0
    for index,name in enumerate(commands):
        body = observed_file(output / name, 16384 if index == 0 else COMMAND_LIMIT)[0]
        observed_file(output / (name[:-6]+"stderr"), STREAM_LIMIT)
        try:
            e = json_value(body)
        except Refused:
            e = None
        if e is None or (type(e) is dict and e.get("command") == "status" and e.get("ok") is False and type(e.get("api_version")) is int and e["api_version"] == 1):
            # This slot's argv/exit are not recorded. Never call it validated.
            need(not r["Completed"] and index == len(commands)-2 and "resume" in kinds and unvalidated == 0)
            unvalidated, kind = 1, "unvalidated_final_observer"
        else:
            need(type(e) is dict and type(e.get("api_version")) is int and e["api_version"] == 1 and e.get("ok") is True)
            kind = e.get("command")
            need(kind in ("version", "state init", "status", "pause", "resume", "stop", "report"))
            if kind == "version":
                need(e.get("build_metadata") == m and e.get("version") == m["version"])
        kinds.append(kind)
    need(kinds[:2] == ["version","state init"] and kinds.count("version") == kinds.count("state init") == 1)
    need(kinds.count("pause") == kinds.count("resume") == kinds.count("stop") == 1 and kinds.count("report") == int(r["Completed"]))
    need(kinds.index("pause") < kinds.index("resume") < kinds.index("stop"))
    need(kinds[-1] == ("report" if r["Completed"] else "stop"))
    return {"contract":"native_linux_small_topology_ci_v1", "revision":revision, "shape":shape, "outcome":r["Outcome"], "samples":len(samples), "command_receipts":len(commands), "unvalidated_final_observer_receipts":unvalidated, "saved_job_page_completeness_verified":False, "workers":2, "worker_cpu_ns":sum(u["user_cpu_ns"]+u["system_cpu_ns"] for u in r["Workers"]), "maximum_individual_worker_peak_rss_bytes":max(u["peak_rss_bytes"] for u in r["Workers"]), "max_control_latency_ns":max(r["ControlLatencyNS"]), "binary_bytes":binary_bytes, "helper_bytes":helper_bytes, "binary_sha256":binary_hash, "helper_sha256":helper_hash, "source_authentication_verified":False, "default_profile_accepted":False, "hourly_target_accepted":False, "physical_io_verified":False, "provider_accepted":False, "soak_accepted":False, "million_entry_accepted":False}


def check_pilot(output, binary, helper, shape, revision):
    try:
        return _check_pilot(output,binary,helper,shape,revision)
    except Refused:
        raise
    except (OSError, ValueError, TypeError, KeyError, RecursionError, struct.error) as error:
        raise Refused("pilot evidence refused") from error


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for name in ("output","binary","helper","revision"):
        p.add_argument("--"+name, required=True)
    p.add_argument("--shape", required=True, choices=("wide","deep"))
    a = p.parse_args()
    try:
        summary = check_pilot(a.output,a.binary,a.helper,a.shape,a.revision)
        body = json.dumps(summary, sort_keys=True, separators=(",",":"))
        need(len(body.encode()) <= 8192)
    except (Refused, OSError, ValueError, TypeError, KeyError, RecursionError):
        print("Native topology pilot check refused; retain generated evidence.", file=sys.stderr)
        return 1
    print(body)
    return 0


if __name__ == "__main__":
    sys.exit(main())
