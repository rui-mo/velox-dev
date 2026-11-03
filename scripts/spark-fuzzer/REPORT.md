# Spark fuzzer report

This report covers verifying Velox's Spark functions against Spark 4.1 with the
Spark expression fuzzer and the Spark query runner. It lists the framework
changes, the function fixes, the known differences, the open issues and the
results of the fuzzer runs. The work starts at commit `10d88f912`.

- [README.md](README.md): how to set up the environment and run the fuzzers.
- [.claude/skills/spark-fuzzer/SKILL.md](../../.claude/skills/spark-fuzzer/SKILL.md):
  the workflow to run the fuzzers, triage failures and commit fixes, also used
  by the `spark-fuzzer` agent in `.github/agents/`.

## Summary

- The Spark query runner targets Spark 4.1.4 through Spark Connect.
- The runner translates Project, Aggregation, Window and Values plan nodes to
  Spark SQL with the same semantics. Expressions that Spark SQL can't express
  are reported as unsupported, and the fuzzer skips their verification.
- `spark_expression_fuzzer_test --spark_url=localhost:15002` verifies Velox's
  results and errors against Spark.
- `scripts/spark-fuzzer/` persists the environment: it installs Java 21 and
  Spark 4.1.4, runs the Spark Connect server, runs fuzzer campaigns and
  triages failures.
- 48 function fixes, one commit each, and 61 commits for the Spark query
  runner, the fuzzers, their argument generators, the scripts, the
  `spark-fuzzer` skill and this report, listed below.
- In the first campaign, all 30 seeds failed. In the one-hour run of the
  expression fuzzer, 11 of the 12 seeds passed, and Spark verified 9333 of the
  11169 test cases. The failed seed hit a known last-bit difference of `sinh`.
  In the one-hour run of the aggregation fuzzer, all 11 seeds passed, and Spark
  verified 8243 of the 8842 iterations. These runs and the shorter runs after
  them led to fixes 38 to 48, 2fde421c4 and 189503ee0. With them, all 3 seeds
  of the last expression run passed.

## Environment

| Component | Setting |
| --- | --- |
| Spark | 4.1.4, `local-cluster[1,4,3072]`, Spark Connect on port 15002 |
| Java | 21. Java 17 prints some floats and doubles with more digits (JDK-4511638). |
| Session configs | ANSI mode off, time zone `America/Los_Angeles`, `mapKeyDedupPolicy=LAST_WIN`, `CORRECTED` Parquet datetime rebase |
| Evaluation | Interpreted: `spark.sql.codegen.wholeStage=false`, `spark.sql.codegen.factoryMode=NO_CODEGEN` |
| Optimizer | Excludes `ConstantFolding`, `ReorderAssociativeOperator`, `SimplifyConditionals`, `NullPropagation`, `SimplifyBinaryComparison` and `SimplifyExtractValueOps`, and disables subexpression elimination |
| Executor | `spark.executor.killOnFatalError.depth 0`, so an `OutOfMemoryError` fails the task instead of killing the executor |

Spark's semantics are those of its interpreted evaluation, which Velox follows.
The excluded optimizer rules skip the evaluation of expressions whose result
they know, so Spark loses errors that Velox raises, or rewrite expressions with
other semantics, e.g. `ReorderAssociativeOperator` evaluates
`1 + try_add(c0, 2)` as `c0 + (1 + 2)`, which doesn't return null on overflow.

## Framework

### Spark query runner

| Commit | Change |
| --- | --- |
| 805070216 | Upgrade the runner, the Spark Connect protos and the docker image to Spark 4.1.4. |
| 78ce954e1 | Classify Spark errors by exception class and SQLSTATE. Analysis errors, unsupported features, resource limits and fatal JVM errors are unsupported. Runtime errors are reference query failures that Velox must match. |
| 565f97bf6 | Translate typed expressions, constants of all types, types, aggregates and window functions to Spark SQL. |
| cfdea0a66 | Drop the temporary views of the input after each query. |
| 2d98254ab | Keep the last value of duplicate map keys, like Velox. |
| 5066d175d, 96d33ef4a, 6d9a7c82b, 32d93716f, 9239c5414 | Exclude optimizer rules that skip evaluations, and subexpression elimination. |
| 7cd24de92 | Report a query as unsupported if Spark fails to create the input views. |
| 357ad29fc | Evaluate queries without generated code, which differs from the interpreted evaluation, e.g. for `array_join`. |
| 44ac99601 | Treat Spark's datetime upgrade errors as query failures. |
| a65cd0137 | Keep executors alive on OutOfMemoryError. |
| 758b4c4bb | Treat Spark's errors for invalid escapes in `LIKE` patterns as query failures. They are analysis errors that Spark raises while evaluating `LIKE`. |

The translation in `velox/functions/sparksql/fuzzer/SparkSql.cpp` handles:

- Operators, `AND`, `OR`, `NOT`, `BETWEEN`, `IN`, `LIKE ... ESCAPE`, `IF`,
  `CASE WHEN` for `switch`, `coalesce`, `CAST` and `TRY_CAST`, `nullif`,
  dereferences and row constructors.
- Lambdas of higher-order functions.
- Constants of all Spark types, including decimals, timestamps, intervals,
  arrays, maps and structs, and typed nulls.
- Functions with other names or syntax in Spark: `unaryminus`, `bitwise_not`,
  `year_of_week`, `get_timestamp`, `week_of_year`, `trim` and its variants,
  `size` with `legacySizeOfNull`, `array_insert` with `legacyNegativeIndex`,
  `timestampadd` and `timestampdiff` units, `to_json` with a time zone.
- Aggregates with `DISTINCT`, `FILTER` and `IGNORE NULLS` in single
  aggregations, and window functions with `ROWS` frames with constant offsets
  and `RANGE` frames without offsets.

These are unsupported, so the fuzzer doesn't verify them:

| Reason | Examples |
| --- | --- |
| No Spark SQL equivalent | `rand`, `uuid`, `shuffle`, `might_contain`, `checked_*`, `*_deny_precision_loss`, `try`, functions internal to Gluten, `array_sort` with a key function |
| Spark requires a constant | `IN` lists, `LIKE` escapes, `to_json` time zones, `timestampadd` and `timestampdiff` units, the legacy flags of `size` and `array_insert`, frame offsets |
| Spark can't express it | NULL or empty `IN` lists, null map keys, structs without fields, timestamps with nanoseconds or out of Spark's range, k `RANGE` frames, aggregates with sorting keys |
| Spark returns no comparable result | `to_json` of maps with binary keys, which Spark writes as Java object identities, e.g. `[B@14b6cefd` |
| Spark rejects it during analysis | casts that Spark doesn't allow (`CAST_WITHOUT_SUGGESTION`), arguments of different types (`DATA_DIFF_TYPES`), `mode` with `DISTINCT` (`INVALID_WITHIN_GROUP_EXPRESSION`) |

`spark_fuzzer.py summary <run> --unsupported 20` prints the most common
reasons of a run.

### Spark expression fuzzer

| Commit | Change |
| --- | --- |
| 72d477365 | Add `--spark_url` and `--spark_query_timeout_sec`. Skip functions that Spark SQL doesn't have. |
| c1ee868ff | Log the reference query of mismatches. |
| 0be11ee44 | Add `--skip_functions`. |
| eb139e198 | Accept errors that only Spark throws if Velox throws for the smallest failing subexpression. Velox doesn't evaluate the arguments of default-null functions on null rows. |
| 0a9fae7f7 | Accept errors that only Velox throws if Spark throws for the smallest failing subexpression. Velox evaluates all arguments of functions such as `array_prepend`. |
| 8bd8c380d, d09586941, ba9a8c883 | Skip signatures that Spark doesn't have or evaluates differently: `array_sort` with a key lambda, `levenshtein` with a threshold, `element_at(array, bigint)`, `array_intersect` of one argument, `array_position` with a start. |
| f5ebb432c, 988eb044d | Skip math functions whose results differ from Java in the last bit, see below. |
| 518a2fc1b, 6f84dd6ee, d222201d1, d91ace017 | Bound `round` scales, decimal `abs` and `unaryminus` precisions, pad sizes and `repeat` counts to the limits of Velox and Spark. |
| 2e269fbf0 | Generate valid `make_timestamp` fields and time zones. |
| bd5cfddb1 | Generate regular expressions that behave the same in RE2 and `java.util.regex`. |
| f2fe893cc, 1d49b9d56, b0fb6eabd | Mark the `rlike` and `regexp_extract` patterns, the `array_insert` flag and the `timestampadd` and `timestampdiff` units as constant arguments. |
| 7b8e5337d, 1dfe51fea | Translate legacy `array_insert`, and generate positions within the arrays. |
| feebd9225 | Generate valid `timestampadd` and `timestampdiff` units. |
| 147237ae9 | Generate single-character `mask` replacements. |
| 91e465cc4, 4652a175c, 72a7a70b5 | Translate `to_json` with a time zone, skip binary map keys, and generate valid time zones. |
| ecb3eab72 | Generate `LIKE` patterns with wildcards and escape sequences, and single-character escapes. |
| 189503ee0 | Add `--velox_fuzzer_orderable_map_keys`, which the Spark fuzzer turns on. It generates map keys only of orderable types, because Spark rejects map keys that are or contain maps. |

### Spark aggregation fuzzer

| Commit | Change |
| --- | --- |
| 03fcd2174 | Compare the results of `min` and `max` with Spark. They were exempt because DuckDB, the reference before Spark, returns null when it compares nested nulls. |
| 1f3ec0b39 | Compare the results of functions with custom verification, e.g. `collect_list` and `first`, with Spark if an input column is DATE. The fuzzer skipped them because the Presto runner writes the input to DWRF files, which can't store DATE. |
| 2fde421c4 | Generate positions in [0, 32768) for `bitmap_construct_agg` and 4096-byte bitmaps for `bitmap_or_agg`. Velox threw for random inputs in every iteration, so Spark never verified them. |

### Scripts

| Commit | Change |
| --- | --- |
| 9b88acdb5 | Add `setup.sh`, `spark-server.sh` and `spark_fuzzer.py` with `run`, `summary`, `repro` and `recheck`. |
| bd2b83003 | `spark_fuzzer.py sql` runs queries with the session configs of the fuzzer, on Parquet input. |
| 8c28541aa | Stop the fuzzer when `spark_fuzzer.py` is interrupted. |
| 3eb43728c | Detect reruns that generate other expressions than the failed iteration. |
| 1371b9db3 | `summary --unsupported N` counts the unsupported test cases by reason. |
| 32a8d78ec | `run` and `summary` report the verified and unsupported iterations of the aggregation fuzzer. |
| 2f8228869 | `sql` starts the Spark Connect server if it is down, e.g. after a reboot. |
| 6386a14c0 | Add this report. |
| 43172b25c | Add the `spark-fuzzer` skill and agent, which run the fuzzers, triage the failures and commit the fixes. |
| 6946ee1f4 | The skill fixes functions that accept argument types that Spark rejects, by restricting their signatures. |

## Function fixes

Each fix has a unit test that fails without it, with the expected values taken
from Spark 4.1.4.

| # | Commit | Function | Fix |
| --- | --- | --- | --- |
| 1 | c21a46c58 | `array_sort` | Order nested nulls first. |
| 2 | d71550152 | `array_remove` | Compare nested nulls as values instead of throwing. |
| 3 | 110d1290b | datetime functions | Support empty patterns, e.g. `date_format(ts, '')`. |
| 4 | 25f60d3f9 | `overlay` | Wrap `pos - 1` and `pos + len` around like 32-bit integers. |
| 5 | 4fc3be653 | `timestamp_seconds` | Throw on overflow. |
| 6 | d120d2639 | `round` | Round integers to negative scales, and handle large scales. |
| 7 | f2b89fc1c | `between` | Return false if the value is out of the range of the non-null bound. |
| 8 | e228d675d | `array_contains` | Compare nested nulls as values instead of throwing. |
| 9 | 197d385e4 | `lpad`, `rpad` | Return an empty string for negative sizes, and truncate for an empty pad string. |
| 10 | 82aa50d37 | `regexp_extract_all` | Check the group only on a match. |
| 11 | 1f43ff064 | `regexp_extract` | Check the group only on a match. |
| 12 | 0c3b62300 | `cast` to string | Cast REAL and DOUBLE with the shortest digits that round-trip. |
| 13 | 7de4efcb8 | `substring_index` | Return an empty string for an empty delimiter. |
| 14 | f0569f8ba | `format_number` | Format the decimal digits that Java derives for doubles. |
| 15 | a5a7a78b4 | `bit_count` | Count the 64 bits of sign-extended negative values. |
| 16 | fd55deb98 | `months_between` | Use the time since the start of the local day. |
| 17 | cc6dd8074 | `map_zip_with` | Keep the key order of the input maps. |
| 18 | c2078ac15 | `json_array_length` | Follow Jackson's JSON syntax. |
| 19 | 4f5968ea4 | `map_concat` | Keep the position of the first duplicate key. |
| 20 | 71449edc9 | `regexp_extract_all` | Use group 1 by default. |
| 21 | 6a03ec001 | `regexp_extract` | Use group 1 by default. |
| 22 | c1615f3a9 | `atanh` | Compute it with `log1p`. |
| 23 | ffcfa8dcf | `regexp_replace` | Replace an empty match right after a match. |
| 24 | 98162bc72 | `regexp_replace` | Use Java's replacement syntax. |
| 25 | 6f310e191 | `regexp_replace` | Return the input for positions past the last character. |
| 26 | 75b5f41fd | `split` | Follow `Pattern.split` for empty matches. |
| 27 | 0e447aaf5 | `split` | Keep the rest of the input for an empty delimiter with a limit (SPARK-49968). `spark.legacy_truncate_for_empty_regex_split` keeps the old behavior. |
| 28 | d2ba0f93c | `regexp_extract_all` | Don't match inside multi-byte characters. |
| 29 | b92ebbad5 | `in` | Throw if the list isn't constant instead of treating it as empty. |
| 30 | ffd2da970 | `second` | Apply the session time zone. |
| 31 | 238765652 | `timestampdiff` | Compare local date-times. |
| 32 | cae56d54f | `timestampadd` | Support BIGINT quantities, and throw on overflow. |
| 33 | fa3de6d2f | `timestampadd` | Keep the offset of the input in a DST overlap. |
| 34 | 31108e40d | `timestampdiff` | Compare microseconds, and adjust month ends like `LocalDateTime.until`. |
| 35 | 2b34f9408 | `mask` | Keep a NUL replacement character. |
| 36 | - | `mask` | Dropped. Spark 4.3 treats a supplementary character as one character, as Velox does. |
| 37 | 10bf95100 | `like` | Escape with `\` by default, and throw for `\` before other characters or at the end. |
| 38 | 2eb19b81a | `map` | Reject keys that are or contain maps. |
| 39 | 8c7ab33f0 | `map_from_arrays` | Reject keys that are or contain maps. |
| 40 | ee2896f55 | `map_from_entries` | Reject keys that are or contain maps. |
| 41 | 5e3790f2b | `array_remove` | Reject elements that are or contain maps. |
| 42 | 469b878ed | `array_union` | Reject elements that are or contain maps. |
| 43 | c8e8885d0 | `array_distinct` | Reject elements that are or contain maps. |
| 44 | 5eee347af | `array_intersect` | Reject elements that are or contain maps. |
| 45 | d4e51a38e | `array_except` | Reject elements that are or contain maps. |
| 46 | d29a19a73 | `array_contains` | Reject elements that are or contain maps. |
| 47 | e71fafcac | `array_position` | Reject elements that are or contain maps. |
| 48 | a79c4aa87 | `cast` from string | Parse REAL and DOUBLE as Java does, with type suffixes, e.g. `9f`, and hexadecimal numbers, e.g. `0x1.8p1`. |

## Known differences

These are not fixed. `known-issues.txt` skips the functions that fail in most
runs.

| Function | Difference |
| --- | --- |
| `acos`, `asin`, `atan`, `atan2`, `cbrt`, `cosh`, `hypot`, `log10` | glibc and Java's fdlibm or HotSpot intrinsics differ in the last bit for 3% to 55% of random inputs. Discontinuous functions, e.g. `width_bucket`, `floor` and casts to strings, amplify the difference. |
| `sin`, `cos`, `tan`, `sinh`, `tanh`, `exp`, `log` and other math functions | glibc and Java differ in the last bit for less than 1% of random inputs, so the fuzzer doesn't skip them. Seed 90010 failed for `width_bucket(cos(c1), tanh(sinh(c2)), c3, c4)` with 800317658043838737 buckets. `sinh` returned 0.19130470795657534 in Velox and 0.19130470795657536 in Spark, which moved the result from bucket 462593777917631553 in Spark to 462593777917631617 in Velox. |
| `asinh`, `acosh` | Spark 4.1 loses precision, e.g. `asinh(-1E10)` is `-Infinity`. Fixed in Spark master by SPARK-56089. |
| `array_repeat`, `array_insert` | Velox returns at most 10000 elements. |
| `repeat`, `lpad`, `rpad` | Velox returns at most 1 MB. |
| `str_to_map` | Spark treats the delimiters as regular expressions. |
| `aggregate` | Velox evaluates the initial state for rows with null arrays and fails on its errors. Spark returns null. |
| `sequence` | Spark Connect doesn't return the error of illegal boundaries, so the query hangs. The fuzzer skips `sequence`. |
| regex functions | RE2 and `java.util.regex` accept different syntax, e.g. back references, lookarounds, possessive quantifiers and a `{` that doesn't start a repetition. The fuzzer generates patterns that both interpret the same way. |
| `mask` | Spark replaces characters with `?` for supplementary replacement characters. Velox keeps them, as documented. |
| `to_json` | Spark writes binary map keys as Java object identities. The runner reports them as unsupported. |
| `get_timestamp` | Spark returns null for a pattern with an unclosed quote if the pattern has `S`, and throws otherwise. Velox throws. |
| decimal `abs`, `unaryminus` | Spark rounds the results to 34 significant digits. The fuzzer generates precisions up to 34. |
| `levenshtein` | Spark's interpreted evaluation treats a null threshold as 0. The fuzzer skips the signature with a threshold. |
| `bitmap_or_agg` | Velox throws for bitmaps that aren't 4096 bytes long, as documented. Spark ORs the first 4096 bytes of bitmaps of any length. `bitmap_construct_agg` returns 4096 bytes. |

## Open issues

| Function | Issue |
| --- | --- |
| `date_format` and other datetime functions | Velox accepts Joda pattern letters that Spark's `DateTimeFormatter` rejects or interprets differently, e.g. `Y`, `w`, `e`, `x` and `C`. |
| `array_union`, `sort_array` | A mismatch in campaign 3 with Spark's generated code didn't reproduce with interpreted evaluation. |

## Fuzzer runs

Each campaign runs the expression fuzzer for all functions except those in
`known-issues.txt`. A test case is unsupported if Spark can't evaluate it, e.g.
because it has no equivalent or rejects the arguments. A suppressed error is a
Spark error that Velox suppresses on rows that it doesn't evaluate.

| Run | Build | Seeds | Passed | Test cases | Verified | Unsupported |
| --- | --- | --- | --- | --- | --- | --- |
| campaign1 | 0be11ee44 | 30 | 0 | - | - | - |
| campaign2 | 6f84dd6ee | 30 | 2 | 1331 | 939 | 342 |
| campaign3 | eb139e198 | 30 | 7 | 6848 | 4771 | 1821 |
| campaign4 | 9b88acdb5 | 24 | 9 | 8773 | 6131 | 2303 |
| campaign5 | 988eb044d | 24 | 7 | 7471 | 5345 | 1867 |
| campaign6 | 3eb43728c | 12 | 9 | 10302 | 7365 | 2511 |
| campaign7 | 72a7a70b5 | 8 | 8 | 8528 | 6973 | 1010 |
| campaign8 | 43172b25c | 12 | 11 | 11169 | 9333 | 1179 |
| campaign9 | ee2896f55 | 3 | 3 | 2983 | 2585 | 246 |
| campaign10 | 189503ee0 | 3 | 2 | 2423 | 2129 | 104 |
| campaign11 | a79c4aa87 | 3 | 3 | 2693 | 2379 | 144 |

Each seed ran for 2 to 5 minutes and stopped at the first failure. The test
cases are those of the passed seeds. The failures were fixed or classified
above, except for single failures that later builds no longer reproduce. In
campaign 1, Spark threw `DUPLICATED_MAP_KEY` in 15 of the 30 seeds. After a
reboot of the machine, an expression run with seeds 87000 and 87001 started the
Spark Connect server and passed, and Spark verified 923 of the 1088 test cases.

Campaign 8 is the one-hour run, 12 seeds of 5 minutes. Seed 90010 failed for
the last-bit difference of `sinh` that is listed under known differences.
Spark rejected 391 of the test cases for map keys that are or contain maps, and
311 for comparing maps, mostly in array functions. Fixes 38 to 47 make Velox
reject the same function calls, so the fuzzer no longer generates them.
Campaign 9 ran after fixes 38 to 40, and still had 54 test cases with constant
maps whose keys are maps. 189503ee0 stops the fuzzer from generating such
constants.

Campaign 10 ran with all of these changes. Spark rejected none of its test
cases for map keys or array elements that are maps, and the unsupported test
cases dropped from 8.2% in campaign 9 to 4.3%. Seed 94000 failed because Velox
returned NULL for `try_cast('9f' AS FLOAT)`, which fix 48 corrects. All 3 seeds
of campaign 11 passed after fix 48.

Runs of single functions verified the fixes and generators:

| Run | Functions | Passed | Verified test cases |
| --- | --- | --- | --- |
| regex1b | regex functions | 5 of 5 | 8185 of 8534 |
| verify-array-insert6 | `array_insert` | all | 1260 of 1344, up from 81 of 243 |
| verify-ts-fix34 | `timestampadd`, `timestampdiff`, date functions | 4 of 4 | 2345 of 2739 |
| verify-mask-gen | `mask` | 3 of 3 | 1597 of 1597 |
| verify-to-json-tz2 | `to_json` | 3 of 3 | 1130 of 1699 |
| verify-like | `like` | 3 of 3 | 2947 of 2997 |

The unsupported `to_json` test cases convert scalars, which Spark and Velox
reject. The other `like` test cases have invalid escapes, and both Spark and
Velox throw for them.

The aggregation fuzzer ran all aggregate functions except where the table
names them. It runs the Spark query only if Velox succeeds, so iterations in
which Velox throws are neither verified nor unsupported.

| Run | Build | Functions | Seeds | Passed | Iterations | Verified | Unsupported |
| --- | --- | --- | --- | --- | --- | --- | --- |
| agg-smoke2 | 03fcd2174 | all | 3 | 3 | 999 | 845 | 3 |
| agg-smoke3 | 1f3ec0b39 | all | 3 | 3 | 934 | 872 | 4 |
| verify-agg-minmax | 2f8228869 | `min`, `max` | 3 | 3 | 824 | 824 | 0 |
| agg-campaign1 | 43172b25c | all | 11 | 11 | 8842 | 8243 | 35 |
| bitmap-agg | 2fde421c4 | `bitmap_construct_agg`, `bitmap_or_agg` | 2 | 2 | 430 | 430 | 0 |
| agg-campaign2 | ee2896f55 | all | 2 | 2 | 1281 | 1276 | 5 |

agg-campaign1 is the one-hour run, 11 seeds of 5 minutes. Velox threw in 564
of its iterations, all of them for `bitmap_construct_agg` positions out of
range and `bitmap_or_agg` bitmaps that aren't 4096 bytes long. After
2fde421c4, Velox threw in none of the iterations of agg-campaign2. In
agg-smoke2, the fuzzer also skipped functions with custom verification if an
input column was DATE, which 1f3ec0b39 fixes. All unsupported iterations are
`mode` with `DISTINCT`, which Spark rejects.

## Limitations and next steps

- The aggregation fuzzer runs the Spark query only if Velox succeeds, so it
  doesn't compare Velox's errors with Spark. For example, Velox throws for
  `bitmap_or_agg` bitmaps that aren't 4096 bytes long, which Spark accepts.
  For functions whose results depend on the order of the input rows, e.g.
  `first`, `max_by` and `mode`, it compares only the number of result rows
  with Spark. Spark rejects `mode` with `DISTINCT`, so these iterations are
  unsupported.
- The runner translates window functions, which `SparkQueryRunnerTest`
  verifies against Spark, but no window fuzzer uses the Spark query runner.
- Unsupported test cases dropped from 24% of all in campaign 6 to 12% in
  campaign 7, and to 4.3% and 5.3% in campaigns 10 and 11. The fuzzer skips
  signatures that Spark doesn't have, the runner translates legacy
  `array_insert` and `to_json` with a time zone, and the generators produce
  the constant arguments that Spark requires. Velox now rejects maps as map
  keys and in array functions that compare elements, as Spark does. The
  largest remaining groups in campaign 11 are `IN` with NULL or empty lists
  (38), which Spark SQL can't express, `to_json` of scalars (25) and casts
  that Spark doesn't allow (20). An argument generator for `in` could avoid
  the NULL and empty lists. Campaign 10 also had year-month intervals in
  arrays, maps or structs (7). The Parquet input stores intervals as integers,
  and the runner converts only top-level columns back.
- Velox's variadic `map` signature accepts any arguments after the first key
  and value, e.g. an odd number of arguments or keys of different types. Both
  Spark and Velox reject these calls, so these test cases are unsupported.
- Random strings only have characters of the Basic Multilingual Plane, so the
  fuzzer doesn't cover supplementary characters, e.g. emojis.
- Spark-only behaviors of Spark 4.1 that differ from Spark master, e.g.
  SPARK-56089, are skipped until the runner moves to a newer Spark release.
