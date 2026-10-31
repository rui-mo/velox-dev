# Spark fuzzer environment

Scripts to run the Velox Spark fuzzers with Spark as the reference DB and to
triage their failures. The expression fuzzer verifies the results of Velox's
Spark functions against Spark 4.3 through the Spark Query Runner, which runs the
queries in a Spark Connect server. See
`velox/docs/develop/testing/spark-query-runner.rst` and
`velox/docs/develop/testing/expression-fuzzer.rst` for how it works.

## Quick start

```bash
# 1. Install Java 21, Spark and the Spark Connect Python client in
#    $SPARK_FUZZER_HOME, ~/spark-fuzzer by default.
scripts/spark-fuzzer/setup.sh

# 2. Start the Spark Connect server on localhost:15002.
scripts/spark-fuzzer/spark-server.sh start

# 3. Build the fuzzer. It requires VELOX_BUILD_TESTING and VELOX_ENABLE_PARQUET.
cmake --build _build/release --target spark_expression_fuzzer_test

# 4. Run 10 fuzzer runs of 5 minutes each and summarize the failures.
scripts/spark-fuzzer/spark_fuzzer.py run --runs 10 --duration-sec 300
```

## Files

| File | Description |
| --- | --- |
| `setup.sh` | Installs the environment. Rerun it after changing the configs. |
| `spark-server.sh` | Starts, stops, restarts and checks the Spark Connect server. `log` prints the path of its log. |
| `spark_fuzzer.py` | Runs the fuzzers and triages their failures. |
| `known-issues.txt` | Functions that `spark_fuzzer.py run` skips because of known issues. |
| `env.sh` | Common settings, sourced by the shell scripts. |
| `conf/` | Spark configs that `setup.sh` copies to `$SPARK_HOME/conf`. |

`$SPARK_FUZZER_HOME` contains:

| Directory | Contents |
| --- | --- |
| `downloads/` | The downloaded Java and Spark packages. |
| `jdk/` | Java 21, unless it was already installed. |
| `spark-<version>-bin-hadoop3/` | Spark. |
| `venv/` | Python venv with `pyspark-client`, used by `spark_fuzzer.py sql`. |
| `logs/`, `run/` | Logs and PID file of the Spark Connect server. |
| `runs/<run>/` | Results of `spark_fuzzer.py run`: `config.json`, `results.jsonl`, the fuzzer binary, a log per seed and the repro files of failed seeds. |
| `repro/` | Logs of `spark_fuzzer.py repro` and `recheck`. |

## Environment variables

| Variable | Default | Description |
| --- | --- | --- |
| `SPARK_FUZZER_HOME` | `~/spark-fuzzer` | Directory of the environment. Must be absolute. |
| `SPARK_CONNECT_PORT` | `15002` | Port of the Spark Connect server. |
| `SPARK_FUZZER_JAVA_HOME` | Java 21 | Java of the Spark Connect server. |
| `SPARK_FUZZER_SPARK_VERSION` | `4.3.0` | Spark version to install. |
| `SPARK_FUZZER_SPARK_HOME` | `$SPARK_FUZZER_HOME/spark-<version>-bin-hadoop3` | Spark installation. |
| `VELOX_BUILD_DIR` | `<repo>/_build/release` | Velox build with the fuzzers. |

The scripts ignore `SPARK_HOME` and `SPARK_VERSION` so that they never change
another Spark installation.

## Java version

Spark 4.3 runs on Java 17, 21 and 25. Use Java 21: Velox casts REAL and DOUBLE to
VARCHAR with the shortest digits that round-trip, like `Float.toString` and
`Double.toString` of Java 19 and later. Java 17 sometimes returns more digits
(JDK-4511638), e.g. `9.999999999999999E22` for `1.0E23`, which differs for 3%
of DOUBLE and 19% of REAL values in scientific notation and makes the fuzzer
report false mismatches. `setup.sh` downloads Temurin 21 from Adoptium unless
Java 21 is installed.

## Python version

`spark_fuzzer.py sql` runs queries with `pyspark-client`, which needs Python
3.11 or later. `setup.sh` creates the venv with the first such Python on the
`PATH`, or downloads Python 3.12 with `uv` if there is none.

## Running the fuzzers

`spark_fuzzer.py run` runs a fuzzer with a number of seeds, by default the
expression fuzzer with random seeds, and records the results in
`$SPARK_FUZZER_HOME/runs/<timestamp>` or `--out`. It copies the fuzzer binary
into the run directory, so rebuilding Velox during a run doesn't change it. It
restarts the Spark Connect server if it is down, e.g. after it ran out of
memory.

```bash
# Test only some functions.
scripts/spark-fuzzer/spark_fuzzer.py run --only concat_ws,split --runs 5 --duration-sec 120

# Run for 2 hours with seeds 1000, 1001, etc. and skip a function.
scripts/spark-fuzzer/spark_fuzzer.py run --runs 1000 --total-minutes 120 --seed 1000 --skip sha2

# Pass fuzzer flags after '--'.
scripts/spark-fuzzer/spark_fuzzer.py run --runs 5 -- --batch_size=50

# Run the aggregation fuzzer.
scripts/spark-fuzzer/spark_fuzzer.py run --fuzzer aggregation --runs 5
```

Each seed has one of these results:

| Category | Meaning |
| --- | --- |
| `pass` | All iterations passed. |
| `mismatch` | Velox and Spark returned different results. |
| `velox_only_error` | Velox threw an error, while Spark didn't. |
| `spark_only_error` | Spark threw an error, while Velox didn't. |
| `sql_parse_error` | Spark failed to parse the generated SQL: a bug in the SQL generation. |
| `server_error` | The Spark Connect server failed. |
| `velox_error`, `velox_runtime_error` | Velox threw an unexpected error. |
| `crash`, `hang` | The fuzzer crashed or didn't finish in time. |
| `other` | Another failure. See the log. |

## Triaging failures

```bash
# Group the failures of the latest run, or of the given runs, by their cause.
scripts/spark-fuzzer/spark_fuzzer.py summary [runs/<run> ...] [--examples 5]

# Rerun the failed iteration with its seed, printed by 'summary', and print its
# expressions, the Spark SQL and the diff. --current-build uses the current
# build instead of the binary of the run, e.g. to check a fix.
scripts/spark-fuzzer/spark_fuzzer.py repro --seed <iteration seed> --run-dir runs/<run> [--current-build]

# Run Spark SQL in the server, with the session configs of the fuzzer.
scripts/spark-fuzzer/spark_fuzzer.py sql "SELECT concat_ws(',', array('a', NULL))"

# Rerun all failed iterations of runs with the current build, e.g. after a fix,
# and group the failures that remain.
scripts/spark-fuzzer/spark_fuzzer.py recheck runs/<run>
scripts/spark-fuzzer/spark_fuzzer.py summary runs/<run> --rechecked
```

The fuzzer logs the Spark query of each failure. To find the cause of a
mismatch, simplify the query with `spark_fuzzer.py sql` and compare it with
Velox in a unit test, e.g. in `velox/functions/sparksql/tests`. Not every
mismatch is a bug in a Velox function: the expression fuzzer documentation
describes the known differences that the framework handles, e.g. errors that
Velox suppresses.

## Known issues

`known-issues.txt` lists functions with known, unfixed issues. `run` passes
them to `--skip_functions`, unless `--no-known-issues` is set. Add a function
with a comment that explains the issue, and remove it in the commit that fixes
the issue.
