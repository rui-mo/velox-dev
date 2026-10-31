#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Runs the Velox Spark fuzzers with Spark as the reference DB and triages the
failures.

Subcommands:
  run      Runs a fuzzer with a number of seeds and records the results.
  summary  Groups the failures recorded by 'run' by their errors.
  repro    Reruns one fuzzer iteration and prints its expressions, the Spark
           SQL and the error.
  recheck  Reruns the failed iterations of runs with the current build, e.g.
           to find the failures that a fix resolved.
  sql      Runs Spark SQL in the Spark Connect server with the session configs
           of the expression fuzzer.

Environment variables:
  SPARK_FUZZER_HOME   Directory of the Spark installation, the Python venv and
                      the fuzzer runs. Default: ~/spark-fuzzer.
  SPARK_CONNECT_PORT  Port of the Spark Connect server. Default: 15002.
  VELOX_BUILD_DIR     Velox build directory. Default: <repo>/_build/release.
"""

import argparse
import json
import os
import random
import re
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

REPO_DIR = Path(__file__).resolve().parents[2]
SCRIPT_DIR = Path(__file__).resolve().parent
HOME = Path(os.environ.get("SPARK_FUZZER_HOME", Path.home() / "spark-fuzzer"))
BUILD_DIR = Path(os.environ.get("VELOX_BUILD_DIR", REPO_DIR / "_build" / "release"))
SPARK_CONNECT_PORT = os.environ.get("SPARK_CONNECT_PORT", "15002")

FUZZERS = {
    "expression": "velox/expression/fuzzer/spark_expression_fuzzer_test",
    "aggregation": "velox/functions/sparksql/fuzzer/spark_aggregation_fuzzer_test",
}

# Fuzzer flags used by default. Flags passed after '--' are appended and take
# precedence.
DEFAULT_FLAGS = {
    "expression": [
        "--velox_fuzzer_enable_complex_types",
        "--velox_fuzzer_enable_decimal_type",
        "--enable_dereference",
        "--enable_variadic_signatures",
        "--velox_fuzzer_enable_column_reuse",
        "--velox_fuzzer_enable_expression_reuse",
        "--max_expression_trees_per_step=2",
        "--lazy_vector_generation_ratio=0.2",
        "--special_forms=and,or,cast,coalesce,if,switch",
    ],
    "aggregation": [
        "--enable_sorted_aggregations=false",
    ],
}

# glog line prefix, e.g. "E1031 00:25:39.251145 99357 File.cpp:156] ".
GLOG_PREFIX = re.compile(r"^[IWEF]\d{4} \d\d:\d\d:\d\d\.\d+ +\d+ [^\]]+\] ")

# Session configs of the queries of the expression fuzzer, i.e.
# SparkQueryRunner::defaultSessionConfigs() and projectionSessionConfigs().
SESSION_CONFIGS = {
    "spark.sql.ansi.enabled": "false",
    "spark.sql.session.timeZone": "America/Los_Angeles",
    "spark.sql.legacy.timeParserPolicy": "CORRECTED",
    "spark.sql.parquet.datetimeRebaseModeInRead": "CORRECTED",
    "spark.sql.parquet.int96RebaseModeInRead": "CORRECTED",
    "spark.sql.mapKeyDedupPolicy": "LAST_WIN",
    "spark.sql.codegen.wholeStage": "false",
    "spark.sql.codegen.factoryMode": "NO_CODEGEN",
    "spark.sql.optimizer.excludedRules": ",".join(
        "org.apache.spark.sql.catalyst.optimizer." + rule
        for rule in [
            "ConstantFolding",
            "ReorderAssociativeOperator",
            "SimplifyConditionals",
            "NullPropagation",
            "SimplifyBinaryComparison",
            "SimplifyExtractValueOps",
        ]
    ),
    "spark.sql.subexpressionElimination.enabled": "false",
}


def log(text):
    print(text, flush=True)


def git_commit():
    process = subprocess.run(
        ["git", "-C", str(REPO_DIR), "rev-parse", "--short", "HEAD"],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
    )
    return process.stdout.strip()


def server_address(url):
    host, _, port = url.rpartition(":")
    return host or "localhost", int(port)


def server_is_up(url):
    try:
        with socket.create_connection(server_address(url), timeout=5):
            return True
    except OSError:
        return False


def restart_server(url):
    log("Spark Connect server is down. Restarting it.")
    _, port = server_address(url)
    subprocess.run(
        [str(SCRIPT_DIR / "spark-server.sh"), "restart"],
        env=dict(os.environ, SPARK_FUZZER_HOME=str(HOME), SPARK_CONNECT_PORT=str(port)),
        check=False,
    )
    for _ in range(120):
        if server_is_up(url):
            # Let the server finish starting the executors.
            time.sleep(10)
            return
        time.sleep(1)
    sys.exit(f"Spark Connect server at {url} didn't start.")


def fuzzer_command(args, seed, extra_flags, binary=None):
    binary = Path(binary) if binary else BUILD_DIR / FUZZERS[args.fuzzer]
    if not binary.exists():
        sys.exit(f"{binary} doesn't exist. Build it first.")
    command = [
        str(binary),
        f"--seed={seed}",
        "--logtostderr=1",
        "--minloglevel=0",
    ]
    if args.fuzzer == "expression":
        command += [
            f"--spark_url={args.spark_url}",
            f"--spark_query_timeout_sec={args.query_timeout_sec}",
        ]
        if args.skip:
            command.append(f"--skip_functions={args.skip}")
    if args.only:
        command.append(f"--only={args.only}")
    command += DEFAULT_FLAGS[args.fuzzer] + extra_flags
    return command


def split_names(names):
    return [name.strip() for name in (names or "").split(",") if name.strip()]


def read_known_issues(path):
    """Reads the functions to skip from a file with one function per line.
    '#' starts a comment."""
    names = []
    if path and Path(path).exists():
        for line in Path(path).read_text().splitlines():
            name = line.split("#", 1)[0].strip()
            if name:
                names.append(name)
    return names


def strip_prefix(line):
    return GLOG_PREFIX.sub("", line.rstrip("\n"))


def parse_log(text):
    """Extracts the stats and the failure of the last iteration from a log."""
    lines = text.splitlines()
    result = {"stats": {}}
    for line in lines:
        message = strip_prefix(line)
        if message.startswith("Velox suppressed an error that the reference DB"):
            result["stats"]["suppressed"] = result["stats"].get("suppressed", 0) + 1
        for key, pattern in [
            ("total", r"Total test cases: (\d+)"),
            ("verified", r"Total test cases verified in the reference DB: (\d+)"),
            ("both_failed", r"Total test cases failed in both .*: (\d+)"),
            ("unsupported", r"Total test cases unsupported in the reference DB: (\d+)"),
            # Aggregation fuzzer.
            ("total", r"Total masked aggregations: \d+ .*"),
        ]:
            match = re.match(pattern, message)
            if match and match.groups():
                result["stats"][key] = int(match.group(1))

    # Only the last iteration matters for a failure.
    start = 0
    for i, line in enumerate(lines):
        match = re.search(r"Started iteration (\d+) \(seed: (\d+)\)", line)
        if match:
            start = i
            result["iteration"] = int(match.group(1))
            result["iteration_seed"] = int(match.group(2))
    tail = lines[start:]
    tail_text = "\n".join(strip_prefix(line) for line in tail)

    expressions = []
    for line in tail:
        match = re.search(r"Executing expression \d+ : (.*)$", line)
        if match and match.group(1) != '"row_number"':
            expressions.append(match.group(1))
    result["expressions"] = expressions

    reasons = re.findall(r"^Reason: (.*)$", tail_text, flags=re.MULTILINE)
    result["reason"] = reasons[0] if reasons else ""
    queries = re.findall(
        r"(?:^Query: |Reference path throws for query: |Spark query: )(SELECT .*)$",
        tail_text,
        flags=re.MULTILINE,
    )
    result["spark_sql"] = queries[-1] if queries else ""
    # Logged when only Spark throws: the smallest subexpression that fails in
    # Spark while Velox doesn't throw for it.
    smallest = re.findall(
        r"^Smallest failing reference query: (.*)$", tail_text, flags=re.MULTILINE
    )
    if smallest:
        result["spark_sql_smallest"] = smallest[-1]

    categories = [
        ("mismatch", "Velox and reference DB results don't match"),
        ("velox_only_error", "Only common path threw exception"),
        ("spark_only_error", "Only reference path threw exception"),
        ("sql_parse_error", "Spark failed to parse the query"),
        ("server_error", "Spark Connect server is not usable"),
        ("server_error", "Failed to run query in Spark Connect server"),
        ("velox_runtime_error", "VeloxRuntimeErrors other than"),
        ("velox_error", "Exceptions other than"),
        ("velox_error", "wasn't supposed to throw"),
    ]
    result["category"] = "other"
    for category, marker in categories:
        if marker in tail_text:
            result["category"] = category
            break
    if result["category"] == "spark_only_error":
        errors = re.findall(r"Spark query failed: (.*)$", tail_text, flags=re.MULTILINE)
        if errors:
            result["spark_error"] = errors[-1]
    if result["category"] == "mismatch":
        # The diff printed by assertEqualResults.
        match = re.search(
            r"^(\d+ extra rows, \d+ missing rows|Expected \d+, got \d+.*)$",
            tail_text,
            flags=re.MULTILINE,
        )
        if match:
            # Up to 10 extra and 10 missing rows.
            diff = tail_text[match.start() :].split("\n")[:26]
            result["diff"] = "\n".join(diff)
    if re.search(r"\*\*\* (SIGSEGV|SIGABRT|SIGBUS|SIGILL|SIGFPE)", text):
        result["signal"] = re.search(r"\*\*\* (SIG[A-Z]+)", text).group(1)
        if result["category"] == "other":
            result["category"] = "crash"
    return result


def normalize_reason(reason):
    """Makes reasons of the same error comparable by removing values."""
    reason = re.sub(r"'[^']*'", "'?'", reason)
    reason = re.sub(r'"[^"]*"', '"?"', reason)
    reason = re.sub(r"\b\d+(\.\d+)?\b", "N", reason)
    return reason[:200]


def failure_key(result):
    """Returns the normalized cause of a failure to group failures by."""
    error = result.get("spark_error")
    if not error:
        return normalize_reason(result.get("reason", ""))
    # Spark error classes, e.g. [DUPLICATED_MAP_KEY], identify the error.
    match = re.match(r"\[([A-Z_.]+)\]", error)
    if match:
        return f"Spark: [{match.group(1)}]"
    # Errors of tasks are wrapped, e.g. 'Job aborted due to stage failure: ...
    # most recent failure: Lost task ... (TID 1) (...): <exception>'.
    match = re.search(r"most recent failure: .*?\): (.*)", error)
    if match:
        error = match.group(1)
    return "Spark: " + normalize_reason(error)


def cmd_run(args, extra_flags):
    out = Path(args.out) if args.out else HOME / "runs" / time.strftime("%Y%m%d-%H%M%S")
    out.mkdir(parents=True, exist_ok=True)
    skip = split_names(args.skip)
    if args.fuzzer == "expression" and not args.no_known_issues:
        skip += read_known_issues(args.known_issues)
    args.skip = ",".join(sorted(set(skip)))
    # Rebuilding Velox during the run must not change the fuzzer binary, and
    # 'repro' reruns iterations with the same binary.
    build_binary = BUILD_DIR / FUZZERS[args.fuzzer]
    if not build_binary.exists():
        sys.exit(f"{build_binary} doesn't exist. Build it first.")
    binary = out / build_binary.name
    shutil.copy2(build_binary, binary)
    with open(out / "config.json", "w") as f:
        json.dump(
            {
                "fuzzer": args.fuzzer,
                "only": args.only,
                "skip": args.skip,
                "flags": DEFAULT_FLAGS[args.fuzzer] + extra_flags,
                "spark_url": args.spark_url,
                "query_timeout_sec": args.query_timeout_sec,
                "binary": str(binary),
                "velox_commit": git_commit(),
            },
            f,
            indent=2,
        )
    log(f"Results are written to {out}")
    if args.skip:
        log(f"Skipped functions: {args.skip}")
    rng = random.Random()
    deadline = time.time() + args.total_minutes * 60 if args.total_minutes else None
    for i in range(args.runs):
        if deadline and time.time() > deadline:
            break
        seed = args.seed + i if args.seed is not None else rng.randrange(1, 2**31)
        if not server_is_up(args.spark_url):
            restart_server(args.spark_url)
        repro_dir = out / "repro" / str(seed)
        command = fuzzer_command(args, seed, extra_flags, binary) + [
            f"--duration_sec={args.duration_sec}",
        ]
        if args.fuzzer == "expression":
            command.append(f"--repro_persist_path={repro_dir}")
            repro_dir.mkdir(parents=True, exist_ok=True)
        log_path = out / f"seed-{seed}.log"
        started = time.time()
        timed_out = False
        with open(log_path, "w") as log_file:
            process = subprocess.Popen(
                command,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
            try:
                returncode = process.wait(timeout=args.duration_sec + args.slack_sec)
            except subprocess.TimeoutExpired:
                timed_out = True
                os.killpg(process.pid, signal.SIGKILL)
                returncode = process.wait()
        result = parse_log(log_path.read_text(errors="replace"))
        result.update(
            {
                "seed": seed,
                "returncode": returncode,
                "seconds": round(time.time() - started),
                "log": str(log_path),
            }
        )
        if timed_out:
            result["category"] = "hang"
        if returncode == 0 and not timed_out:
            result["category"] = "pass"
            log_path.rename(log_path.with_suffix(".pass.log"))
            result["log"] = str(log_path.with_suffix(".pass.log"))
        elif repro_dir.exists() and not any(repro_dir.iterdir()):
            repro_dir.rmdir()
        with open(out / "results.jsonl", "a") as f:
            f.write(json.dumps(result) + "\n")
        stats = result["stats"]
        log(
            f"[{i + 1}/{args.runs}] seed={seed} {result['category']} "
            f"verified={stats.get('verified', '-')}/{stats.get('total', '-')} "
            f"unsupported={stats.get('unsupported', '-')} "
            f"suppressed={stats.get('suppressed', 0)} "
            f"{result['seconds']}s {failure_key(result)[:120]}"
        )
    summarize([out / "results.jsonl"])


def read_results(path):
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines()]


def latest_run_dirs():
    runs = HOME / "runs"
    dirs = sorted(d for d in runs.iterdir() if d.is_dir()) if runs.exists() else []
    return dirs[-1:]


def summarize(paths, examples=3):
    results = []
    for path in paths:
        results += read_results(path)
    passed = [r for r in results if r["category"] == "pass"]
    failures = [r for r in results if r["category"] != "pass"]
    total = sum(r["stats"].get("total", 0) for r in results)
    verified = sum(r["stats"].get("verified", 0) for r in results)
    unsupported = sum(r["stats"].get("unsupported", 0) for r in results)
    suppressed = sum(r["stats"].get("suppressed", 0) for r in results)
    log(
        f"\n{len(results)} runs, {len(passed)} passed, {len(failures)} failed. "
        f"Test cases of passed runs: {total} total, {verified} verified, "
        f"{unsupported} unsupported. Spark errors that Velox suppressed: "
        f"{suppressed}."
    )
    groups = {}
    for result in failures:
        log_path = Path(result.get("log", ""))
        if (
            result["category"] == "spark_only_error"
            and "spark_error" not in result
            and log_path.is_file()
        ):
            # Results of older versions of this script lack the Spark error.
            result["spark_error"] = parse_log(log_path.read_text()).get(
                "spark_error", ""
            )
        key = (result["category"], failure_key(result))
        groups.setdefault(key, []).append(result)
    for (category, reason), group in sorted(
        groups.items(), key=lambda item: -len(item[1])
    ):
        log(f"\n[{category}] x{len(group)}: {reason}")
        for result in group[:examples]:
            seed = result.get("iteration_seed", result["seed"])
            log(f"  iteration seed {seed}, log {result['log']}")
            for expression in result.get("expressions", [])[:2]:
                log(f"    expression: {expression[:300]}")
            if result.get("spark_sql_smallest"):
                log(
                    f"    smallest failing Spark SQL: {result['spark_sql_smallest'][:300]}"
                )


def cmd_summary(args, _):
    run_dirs = [Path(d) for d in args.run_dirs] or latest_run_dirs()
    name = "recheck.jsonl" if args.rechecked else "results.jsonl"
    summarize([d / name for d in run_dirs], args.examples)


def iteration_command(args, seed, extra_flags):
    """Returns the command that reruns one iteration. Reuses the flags and the
    binary of args.run_dir if set."""
    flags = extra_flags
    binary = None
    if args.run_dir:
        config = json.loads((Path(args.run_dir) / "config.json").read_text())
        args.fuzzer = config["fuzzer"]
        args.only = args.only or config["only"]
        # Iteration seeds only reproduce with the same candidate functions.
        if args.skip is None:
            args.skip = config.get("skip", "")
        flags = config["flags"][len(DEFAULT_FLAGS[args.fuzzer]) :] + extra_flags
        if not args.current_build:
            binary = config.get("binary")
    binary = getattr(args, "binary", None) or binary
    return fuzzer_command(args, seed, flags, binary) + ["--steps=1", "--v=1"]


def run_iteration(args, seed, extra_flags, log_path):
    """Reruns one iteration and returns its parsed result."""
    if not server_is_up(args.spark_url):
        restart_server(args.spark_url)
    command = iteration_command(args, seed, extra_flags)
    try:
        process = subprocess.run(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            errors="replace",
            timeout=args.iteration_timeout_sec,
        )
        output, returncode = process.stdout, process.returncode
    except subprocess.TimeoutExpired as e:
        output = e.stdout or ""
        if isinstance(output, bytes):
            output = output.decode(errors="replace")
        returncode = None
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.write_text(output)
    result = parse_log(output)
    if returncode is None:
        result["category"] = "hang"
    elif returncode == 0:
        result["category"] = "pass"
    result.update({"command": command, "returncode": returncode, "log": str(log_path)})
    return result


def cmd_repro(args, extra_flags):
    log_path = Path(args.log) if args.log else HOME / "repro" / f"{args.seed}.log"
    result = run_iteration(args, args.seed, extra_flags, log_path)
    log("Command: " + " ".join(result["command"]))
    log(f"Log: {log_path}")
    log(f"Exit code: {result['returncode']}")
    log(f"Category: {result['category']}")
    for expression in result["expressions"]:
        log(f"Expression: {expression}")
    log(f"Spark SQL: {result['spark_sql']}")
    if result.get("spark_sql_smallest"):
        log(f"Smallest failing Spark SQL: {result['spark_sql_smallest']}")
    if result["reason"]:
        log(f"Reason: {result['reason']}")
    if result.get("diff"):
        log(f"Diff:\n{result['diff']}")


def cmd_recheck(args, extra_flags):
    """Reruns the failed iterations of runs, by default with the current build,
    e.g. to find the failures that a fix resolved."""
    still_failing = 0
    total = 0
    # Copies of the current fuzzers, so rebuilding Velox during the recheck
    # doesn't change them.
    snapshot_dir = Path(tempfile.mkdtemp(prefix="recheck-", dir=HOME))
    snapshots = {}
    try:
        for run_dir in [Path(d) for d in args.run_dirs] or latest_run_dirs():
            config = json.loads((run_dir / "config.json").read_text())
            fuzzer = config["fuzzer"]
            if not args.run_binary and fuzzer not in snapshots:
                source = BUILD_DIR / FUZZERS[fuzzer]
                if not source.exists():
                    sys.exit(f"{source} doesn't exist. Build it first.")
                snapshots[fuzzer] = snapshot_dir / source.name
                shutil.copy2(source, snapshots[fuzzer])
            total_run, failing_run = recheck_run(
                args, run_dir, snapshots.get(fuzzer), extra_flags
            )
            total += total_run
            still_failing += failing_run
    finally:
        shutil.rmtree(snapshot_dir, ignore_errors=True)
    log(f"\n{total} failures rechecked, {still_failing} still fail.")
    log("Run 'summary --rechecked' to group the remaining failures.")


def recheck_run(args, run_dir, binary, extra_flags):
    failures = [
        r
        for r in read_results(run_dir / "results.jsonl")
        if r["category"] != "pass" and "iteration_seed" in r
    ]
    log(f"Rechecking {len(failures)} failures of {run_dir}.")
    rechecked = []
    still_failing = 0
    for old in failures:
        seed = old["iteration_seed"]
        repro_args = argparse.Namespace(
            run_dir=str(run_dir),
            fuzzer=None,
            only=None,
            skip=None,
            current_build=binary is not None,
            binary=binary,
            spark_url=args.spark_url,
            query_timeout_sec=args.query_timeout_sec,
            iteration_timeout_sec=args.iteration_timeout_sec,
        )
        new = run_iteration(
            repro_args, seed, extra_flags, HOME / "repro" / f"{seed}.log"
        )
        new.update({"seed": old["seed"], "old_category": old["category"]})
        rechecked.append(new)
        if new["category"] != "pass":
            still_failing += 1
        log(
            f"  iteration seed {seed} (run seed {old['seed']}): "
            f"{old['category']} -> {new['category']} "
            f"{failure_key(new)[:120] if new['category'] != 'pass' else ''}"
        )
    with open(run_dir / "recheck.jsonl", "w") as f:
        for result in rechecked:
            f.write(json.dumps(result) + "\n")
    return len(failures), still_failing


def cmd_sql(args, _):
    python = HOME / "venv" / "bin" / "python"
    if Path(sys.prefix).resolve() != (HOME / "venv").resolve() and python.exists():
        os.execv(str(python), [str(python), __file__] + sys.argv[1:])
    from pyspark.sql import SparkSession

    host, port = server_address(args.spark_url)
    spark = SparkSession.builder.remote(f"sc://{host}:{port}").getOrCreate()
    confs = dict(SESSION_CONFIGS)
    for key_value in args.conf or []:
        key, _, value = key_value.partition("=")
        confs[key] = value
    for key, value in confs.items():
        spark.conf.set(key, value)
    started = time.time()
    try:
        if args.values:
            # Spark evaluates queries on literal rows when it optimizes them,
            # even with ConstantFolding excluded, and infers the nullability of
            # their columns. Read the rows from Parquet like the fuzzer does.
            path = f"file://{HOME / 'tmp' / 'sql-values'}"
            spark.sql(f"SELECT * FROM VALUES {args.values}").write.mode(
                "overwrite"
            ).parquet(path)
            spark.sql(
                f"CREATE OR REPLACE TEMPORARY VIEW T AS SELECT * FROM parquet.`{path}`"
            )
        df = spark.sql(args.query)
        if args.explain:
            df.explain(mode="extended")
        rows = df.collect()
        for row in rows[: args.limit]:
            log(repr(row))
        log(f"OK: {len(rows)} rows in {time.time() - started:.1f}s")
    except Exception as e:  # noqa: BLE001
        log(f"ERROR: {str(e)[:2000]}")
        sys.exit(1)


def main():
    argv = sys.argv[1:]
    extra_flags = []
    if "--" in argv:
        index = argv.index("--")
        argv, extra_flags = argv[:index], argv[index + 1 :]

    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--spark-url", default=f"localhost:{SPARK_CONNECT_PORT}")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run = subparsers.add_parser(
        "run", help="Run a fuzzer with many seeds. Fuzzer flags follow '--'."
    )
    run.add_argument("--fuzzer", choices=FUZZERS, default="expression")
    run.add_argument("--runs", type=int, default=10)
    run.add_argument("--duration-sec", type=int, default=300)
    run.add_argument("--total-minutes", type=int, default=0)
    run.add_argument("--seed", type=int, help="First seed. Random by default.")
    run.add_argument("--only", help="Comma separated functions to test.")
    run.add_argument("--skip", help="Comma separated functions to skip.")
    run.add_argument(
        "--known-issues",
        default=str(SCRIPT_DIR / "known-issues.txt"),
        help="File with functions to skip, one per line. "
        "Default: known-issues.txt next to this script.",
    )
    run.add_argument(
        "--no-known-issues",
        action="store_true",
        help="Don't skip the functions in the known issues file.",
    )
    run.add_argument("--out", help="Output directory.")
    run.add_argument("--query-timeout-sec", type=int, default=60)
    run.add_argument("--slack-sec", type=int, default=600)
    run.set_defaults(handler=cmd_run)

    summary = subparsers.add_parser("summary", help="Summarize failures.")
    summary.add_argument("run_dirs", nargs="*", help="Default: the latest run.")
    summary.add_argument(
        "--examples", type=int, default=3, help="Failures to print per group."
    )
    summary.add_argument(
        "--rechecked",
        action="store_true",
        help="Summarize the results of the last 'recheck' instead of the run.",
    )
    summary.set_defaults(handler=cmd_summary)

    repro = subparsers.add_parser(
        "repro", help="Rerun one iteration with its seed. Fuzzer flags follow '--'."
    )
    repro.add_argument("--seed", type=int, required=True, help="Iteration seed.")
    repro.add_argument("--run-dir", help="Reuse the flags of this run.")
    repro.add_argument("--fuzzer", choices=FUZZERS, default="expression")
    repro.add_argument("--only")
    repro.add_argument(
        "--skip", help="Comma separated functions to skip. Default: as in the run."
    )
    repro.add_argument(
        "--current-build",
        action="store_true",
        help="Use the fuzzer of the current build instead of the one of the run, "
        "e.g. to check a fix.",
    )
    repro.add_argument("--query-timeout-sec", type=int, default=60)
    repro.add_argument("--iteration-timeout-sec", type=int, default=900)
    repro.add_argument(
        "--log",
        help="Write the full log to this file. Defaults to "
        "$SPARK_FUZZER_HOME/repro/<seed>.log.",
    )
    repro.set_defaults(handler=cmd_repro)

    recheck = subparsers.add_parser(
        "recheck",
        help="Rerun the failed iterations of runs with the current build. "
        "Fuzzer flags follow '--'.",
    )
    recheck.add_argument("run_dirs", nargs="*", help="Default: the latest run.")
    recheck.add_argument(
        "--run-binary",
        action="store_true",
        help="Use the fuzzer of the run instead of the current build.",
    )
    recheck.add_argument("--query-timeout-sec", type=int, default=60)
    recheck.add_argument("--iteration-timeout-sec", type=int, default=900)
    recheck.set_defaults(handler=cmd_recheck)

    sql = subparsers.add_parser(
        "sql", help="Run Spark SQL with the session configs of the fuzzer."
    )
    sql.add_argument("query")
    sql.add_argument(
        "--conf",
        action="append",
        help="Spark conf key=value. Overrides the session configs of the fuzzer.",
    )
    sql.add_argument(
        "--values",
        help="Rows of a VALUES clause with column names, e.g. "
        "\"(1, 'a'), (NULL, 'b') AS t(c0, c1)\". They are written to Parquet "
        "and read by the query from the view T, like the fuzzer reads its input.",
    )
    sql.add_argument(
        "--explain", action="store_true", help="Print the plans of the query."
    )
    sql.add_argument("--limit", type=int, default=20)
    sql.set_defaults(handler=cmd_sql)

    args = parser.parse_args(argv)
    args.handler(args, extra_flags)


if __name__ == "__main__":
    main()
