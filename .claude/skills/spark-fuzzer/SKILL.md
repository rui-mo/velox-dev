---
name: spark-fuzzer
description: Run the Velox Spark expression or aggregation fuzzer against Spark 4.3 through the Spark query runner, triage its failures, fix Velox Spark functions with a failing test first and commit each fix. Use when asked to start, run or continue Spark fuzzer testing, to triage or fix Spark fuzzer failures, or when the user mentions "/spark-fuzzer".
---

# Spark Fuzzer

Finds and fixes differences between Velox's Spark functions and Spark 4.3. The
Spark expression fuzzer generates random expressions, evaluates them in Velox
and verifies the results against Spark through the Spark query runner, which
translates them to Spark SQL and runs them in a Spark Connect server. The Spark
aggregation fuzzer does the same for aggregate functions.

Read these before the first run in a session:

- `scripts/spark-fuzzer/README.md`: the environment and the `spark_fuzzer.py`
  commands.
- `scripts/spark-fuzzer/REPORT.md`: the fixes and known issues found so far.
- `scripts/spark-fuzzer/known-issues.txt`: the functions that runs skip.
- `.claude/CLAUDE.md` and `CODING_STYLE.md`: the coding and test rules.

## Rules

- Spark 4.3 is the reference. If Velox follows another Spark version on
  purpose, or the behavior depends on a Spark config, ask the user before
  changing it. Offer a Velox config that keeps the old behavior, like
  `spark.legacy_truncate_for_empty_regex_split`.
- Velox's `spark.ansi_enabled`, read with `SparkQueryConfig::ansiEnabled()`,
  is Spark's `spark.sql.ansi.enabled`. A function reads it only if Spark's
  implementation depends on it, e.g. through `failOnError` or `EvalMode`.
  Other functions behave the same in both modes.
- Fix one function per commit. Framework changes, i.e. to the fuzzer, the
  verifier, the Spark query runner or the scripts, go into separate commits.
- Start every fix with a test that fails without it. Take the expected values
  from Spark with `spark_fuzzer.py sql`, never from Velox or from memory.
- Fix at most the number of function issues the user asked for. Ask if they
  didn't say.
- Commit each verified fix without asking. Ask first if the fix changes the
  behavior of Presto functions, adds a config or skips a function.
- Never stop or restart the Spark Connect server while a run uses it.
  `spark_fuzzer.py` starts it when it is down.
- Keep the run directories. `repro` and `recheck` need them.

## 1. Prepare

The environment lives in `$SPARK_FUZZER_HOME`, `~/spark-fuzzer` by default. If
the variable is unset and `~/spark-fuzzer` doesn't exist, ask the user for the
path, or whether to install a new environment with
`scripts/spark-fuzzer/setup.sh`. Set the variable in every command, e.g.
`SPARK_FUZZER_HOME=<path> scripts/spark-fuzzer/spark_fuzzer.py ...`.

```bash
scripts/spark-fuzzer/spark-server.sh status || scripts/spark-fuzzer/spark-server.sh start
cmake --build _build/release -j 16 --target spark_expression_fuzzer_test
# For aggregate functions.
cmake --build _build/release -j 16 --target spark_aggregation_fuzzer_test
```

The build needs `VELOX_BUILD_TESTING` and `VELOX_ENABLE_PARQUET`. Set
`VELOX_BUILD_DIR` to use another build directory.

## 2. Run

```bash
# 10 runs of 5 minutes with random seeds.
scripts/spark-fuzzer/spark_fuzzer.py run --runs 10 --duration-sec 300 --out $SPARK_FUZZER_HOME/runs/<name>

# Only some functions, with seeds 7000 to 7004.
scripts/spark-fuzzer/spark_fuzzer.py run --only split,regexp_replace --runs 5 --seed 7000 --out ...

# Aggregate functions.
scripts/spark-fuzzer/spark_fuzzer.py run --fuzzer aggregation --runs 5 --duration-sec 300 --out ...

# ANSI mode on in Velox and Spark. Off by default.
scripts/spark-fuzzer/spark_fuzzer.py run --ansi on --runs 10 --duration-sec 300 --out ...
```

- A run takes about `runs * duration-sec` seconds. Run it in the background and
  wait for it to finish. Meanwhile, work on the failures of earlier runs.
- `run` copies the fuzzer binary into the run directory, so rebuilding Velox
  during a run is safe.
- Name run directories after their purpose, e.g. `campaign7` or `regex2`.
- The aggregation fuzzer uses the server on port 15002. It reports the errors
  that only one engine raises. For functions whose results depend on the order
  of the input rows, e.g. `first`, it compares only the number of result rows.
- Test both ANSI modes after changing a function that reads ANSI mode. Runs
  stop at their first failure, so skip the functions with known ANSI failures
  with `--skip` until they are fixed.
- To find the functions that Spark never verified in a mode, e.g. because
  Spark rejects their arguments or both engines always throw, run `summary
  <runs> --examples 0 --coverage` on runs of both modes. Test them with `run
  --only`, or with `sql --ansi both` and unit tests.

## 3. Triage

```bash
scripts/spark-fuzzer/spark_fuzzer.py summary $SPARK_FUZZER_HOME/runs/<name> --examples 3
scripts/spark-fuzzer/spark_fuzzer.py repro --seed <iteration seed> --run-dir $SPARK_FUZZER_HOME/runs/<name>
```

`summary` groups the failures by their cause and prints their iteration seeds.
`repro` reruns one iteration with the fuzzer of the run and prints its
expressions, the Spark SQL and the differing rows. If only Spark fails, it also
prints the smallest failing Spark SQL. A run uses the binary of the commit in
`velox_commit` of its `config.json`, so later commits may have fixed some of
its failures. For each group:

1. Rerun it with `repro --current-build`. If it passes without a warning, an
   earlier fix resolved it. The warning means that the build generates other
   expressions for the seed, e.g. because a commit skipped a function, so the
   rerun doesn't tell. Continue with step 2 then.
2. Reduce it to one function call and run the call in Spark:

   ```bash
   scripts/spark-fuzzer/spark_fuzzer.py sql "SELECT split(c0, c1) FROM T" --values "('a,b', ',') AS t(c0, c1)"
   ```

   `--values` writes the rows to Parquet, like the fuzzer does, because Spark
   evaluates queries on literal rows while it optimizes them. `--conf` changes
   a config, `--ansi both` runs the query with ANSI mode off and on, and
   `--explain` prints the plans.
3. Evaluate the same call in Velox in a unit test, e.g. with `evaluateOnce` in
   `velox/functions/sparksql/tests`.
4. Classify the failure:
   - **Function bug:** Velox and Spark differ for the reduced call. Fix it in
     step 4.
   - **Framework issue:** the fuzzer or the runner makes the engines evaluate
     different things. Fix it in step 5.
   - **Known difference:** see "Known differences" below. Don't fix it.

Test cases that Spark can't run aren't verified. To find the functions that
lose the most coverage this way, run:

```bash
scripts/spark-fuzzer/spark_fuzzer.py summary $SPARK_FUZZER_HOME/runs/<name> --examples 0 --unsupported 20
```

Improve the largest groups as framework issues in step 5, unless Spark can't
express them, e.g. `IN` with a NULL list. If Spark rejects argument types that
Velox accepts, e.g. maps as map keys, fix the function: restrict its signature,
e.g. with `Orderable<T>` or an orderable type variable, so that Velox rejects
these types too and the fuzzer no longer generates them.

## 4. Fix a function

1. Add a test next to the related tests in `velox/functions/sparksql/tests`,
   `velox/functions/sparksql/aggregates/tests` for aggregate functions, or
   `velox/functions/lib/tests` for code shared with Presto. Build
   `velox_functions_spark_test`, `velox_functions_spark_aggregates_test` or
   `velox_functions_lib_test` and check that the test fails. If the fix comes
   first, stash the fixed sources with `git stash push -- <files>`, rebuild,
   run the test and `git stash pop`. Test a function that depends on ANSI mode
   in both modes, like `ArithmeticTest.abs`.

   ```bash
   cmake --build _build/release -j 16 --target velox_functions_spark_test
   _build/release/velox/functions/sparksql/tests/velox_functions_spark_test --gtest_filter='SplitTest.*'
   ```

2. Fix the function. Follow Spark's implementation, e.g. in
   `org.apache.spark.sql.catalyst.expressions`, rather than guessing from
   examples. If the code is shared with Presto, keep Presto's behavior and run
   `velox_functions_test` too.
3. Run the whole test suite of the function and `repro --current-build` of the
   failing iterations.
4. Update the function documentation in `velox/docs/functions/spark/` if the
   behavior changed.
5. Remove the function from `known-issues.txt` if the fix resolves its entry.
6. Commit, see step 6.

## 5. Fix a framework issue

Common causes, each fixed before for other cases:

- A Spark optimizer rule skips the evaluation of an expression, so Spark loses
  an error that Velox raises. Exclude the rule in the session configs of
  `velox/functions/sparksql/fuzzer/SparkQueryRunner.cpp` and in
  `SESSION_CONFIGS` of `spark_fuzzer.py`.
- Random arguments exceed an intended limit of Velox, e.g. pad sizes, repeat
  counts or round scales. Constrain them with an argument generator in
  `velox/expression/fuzzer/ArgValuesGenerators.{h,cpp}`, registered in
  `velox/expression/fuzzer/SparkExpressionFuzzerTest.cpp`, with a test in
  `velox/expression/fuzzer/tests/ArgValuesGeneratorsTest.cpp`.
- Spark requires a constant argument, e.g. a unit, a time zone or a mask
  character. If Velox requires it too, declare it `Constant<T>` in the
  registration in `velox/functions/sparksql/registration/`, in its own commit
  with a test. Then add an argument generator for valid values, e.g.
  `MaskArgValuesGenerator`, or `ToJsonArgValuesGenerator` with time zones from
  `timeZoneNames()`.
- Spark only accepts some argument types, e.g. decimal precisions. Restrict
  them with an `ArgTypesGenerator` in `velox/functions/sparksql/fuzzer/`, e.g.
  `AbsUnaryMinusArgTypesGenerator`.
- Spark has no such signature, e.g. it casts the argument to another type.
  Skip the signature in `sparkSkippedFunctions()` of
  `SparkExpressionFuzzerTest.cpp`, which only applies when verifying against
  Spark.
- The Spark SQL is wrong. Fix the translation in
  `velox/functions/sparksql/fuzzer/SparkSql.cpp` and test it in
  `velox/functions/sparksql/fuzzer/tests/SparkSqlTest.cpp`.

Run the unit tests of the changed component, e.g. `velox_spark_query_runner_test`
or `velox_expression_fuzzer_unit_test`. The `DISABLED_` tests of
`velox_spark_query_runner_test` need the server and
`--gtest_also_run_disabled_tests`. Then rerun the failing iterations and a short
run of the affected functions.

## 6. Commit

1. Run `pre-commit run --files <files>`. It may reformat files; add them again
   and rerun it until it passes.
2. Draft the message with the `write-commit-message` skill. Use the title
   conventions of the existing commits of this work instead of its `[velox]`
   prefix:
   - Function fix: `fix(<scope>): <Description> (#N)`, with the scope of the
     changed code, e.g. `sparksql`, `type` or `vector`. N is the next number
     after the last function fix in `REPORT.md`.
   - Framework: `fix(fuzzer): <Description>` or `feat(fuzzer): <Description>`.
   - Titles have at most 67 characters.
3. Lead the body with the Spark and Velox results of a reduced call, e.g.
   `split('aab', 'a*')` returns `["", "", "b", ""]` in Spark and
   `["", "b", ""]` in Velox. Name the fuzzer seed if a run found it.
4. Commit only the files of this fix.

## 7. Recheck and report

```bash
scripts/spark-fuzzer/spark_fuzzer.py recheck $SPARK_FUZZER_HOME/runs/<name>
scripts/spark-fuzzer/spark_fuzzer.py summary $SPARK_FUZZER_HOME/runs/<name> --rechecked
```

After the fixes, rerun the failed iterations with the current build and start a
run with new seeds. `recheck` reports the failures whose seeds generate other
expressions with the current build as `not_reproduced`. Check them as in step
3. At the end of a session, add the fixes and the new known issues to
`REPORT.md` in a separate `docs(fuzzer)` commit, and summarize them for the
user.

## Known differences

Don't fix these. Add a function to `known-issues.txt` with a comment only if it
fails in most runs.

- **Last-bit math:** Velox uses glibc's math functions and Java uses fdlibm or
  HotSpot intrinsics. They differ in the last bit for some inputs, which changes
  the results of discontinuous functions, e.g. `width_bucket`, `floor` and casts
  to strings. See the README for the rates.
- **Intended limits of Velox:** `array_repeat` and `array_insert` return at most
  10000 elements, `repeat` and the pad functions at most 1 MB.
- **Regular expressions:** Velox uses RE2 and Spark uses `java.util.regex`. The
  fuzzer only generates patterns that behave the same in both. Other patterns,
  e.g. with back references, or `.` and `$` with line terminators, differ.
- **Spark bugs:** see `REPORT.md`, e.g. `levenshtein` with a null threshold.
  Check Spark's master branch and JIRA before calling a Spark result a bug, and
  name the JIRA in the skip comment.
- **Others:** `REPORT.md` lists the rest, e.g. `to_json` of binary map keys and
  the delimiters of `str_to_map`.

## Tips

- The session time zone is America/Los_Angeles. Its offset before 1883 has
  seconds (-07:52:58), and DST changes skip or repeat an hour. Datetime
  functions often differ around these. Spark's `timestampadd` follows
  `ZonedDateTime.plus*`, which keeps the offset of the input in an overlap,
  and `timestampdiff` follows `LocalDateTime.until`.
- The build treats warnings as errors, e.g. a conditional operator that mixes
  an enum and an integer. Cast with `static_cast`.
- To loop over types of different classes in a test, write `for (const auto&
  type : std::vector<TypePtr>{BIGINT(), MAP(VARCHAR(), BIGINT())})`. A braced
  list of them doesn't deduce an initializer list.
