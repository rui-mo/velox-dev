# Spark fuzzer report

This report covers verifying Velox's Spark functions against Spark 4.3 with the
Spark expression and aggregation fuzzers and the Spark query runner. It lists
the framework changes, the function fixes, the known differences, the open
issues and the results of the fuzzer runs. The work starts at commit
`10d88f912`. It first targeted Spark 4.1.4, and [Spark 4.3](#spark-43)
describes the move to Spark 4.3.0.

- [README.md](README.md): how to set up the environment and run the fuzzers.
- [.claude/skills/spark-fuzzer/SKILL.md](../../.claude/skills/spark-fuzzer/SKILL.md):
  the workflow to run the fuzzers, triage failures and commit fixes, also used
  by the `spark-fuzzer` agent in `.github/agents/`.

## Summary

- The Spark query runner targets Spark 4.3.0 through Spark Connect.
- The runner translates Project, Aggregation, Window and Values plan nodes to
  Spark SQL with the same semantics. Expressions that Spark SQL can't express
  are reported as unsupported, and the fuzzer skips their verification.
- `spark_expression_fuzzer_test --spark_url=localhost:15002` verifies Velox's
  results and errors against Spark.
- `scripts/spark-fuzzer/` persists the environment: it installs Java 21 and
  Spark 4.3.0, runs the Spark Connect server, runs fuzzer campaigns and
  triages failures.
- 60 function fixes, numbered 1 to 61 without 36, one commit each, and 60
  commits for the Spark query runner, the fuzzers, their argument generators,
  the scripts, the `spark-fuzzer` skill and this report, listed below.
- Against Spark 4.1.4, all 30 seeds of the first campaign failed. In the
  one-hour run of the expression fuzzer, 11 of the 12 seeds passed, and Spark
  verified 9333 of the 11169 test cases. The failed seed hit a known last-bit
  difference of `sinh`. In the one-hour run of the aggregation fuzzer, all 11
  seeds passed, and Spark verified 8243 of the 8842 iterations. These runs and
  the shorter runs after them led to fixes 38 to 48, b786c50c5 and 8a840784d.
  With them, all 3 seeds of the last expression run passed.
- Against Spark 4.3.0, the one-hour runs failed for behaviors that changed
  after Spark 4.1.4. Velox now follows Spark 4.3.0, with fixes 4 and 49 to 54.
  The next one-hour runs had fixes 4 and 49 to 52. In the run of the
  expression fuzzer, 10 of the 12 seeds passed, and Spark verified 13424 of
  the 15124 test cases. In the run of the aggregation fuzzer, all 12 seeds
  passed, and Spark verified 9232 of the 9277 iterations. These runs and the
  shorter runs after them led to fixes 55, 56 and 59 to 61. With them, all 3
  seeds of the last run over all expressions passed.
- Velox follows Spark 4.3.0 by default. Where Spark 3.5.1 returns other
  results for a fix, a config restores the earlier behavior, see
  [Spark 3.5.1](#spark-351).

## Environment

| Component | Setting |
| --- | --- |
| Spark | 4.3.0, `local-cluster[1,4,3072]`, Spark Connect on port 15002 |
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

## Spark 4.3

The runner first targeted Spark 4.1.4. Spark 4.2 and 4.3 fixed behaviors that
Velox followed, and Velox now follows Spark 4.3.0. The commit of each fix
describes the behavior of the earlier releases, and [Spark 3.5.1](#spark-351)
lists the configs that restore it.

| Fix | Function | Spark change | First release | Found by |
| --- | --- | --- | --- | --- |
| 4 | `overlay` | Clamps overflowing positions (SPARK-58713). | 4.2.1, 4.3.0 | spark43-expr |
| 49 | `covar_samp` | Returns infinite results instead of NaN (SPARK-58291). | 4.3.0 | spark43-agg |
| 50 | `corr` | Returns NULL for zero variance, even with NaN or infinite inputs (SPARK-58213). | 4.3.0 | spark43-agg |
| 51 | `substring_index` | Supports an INT32_MIN count (SPARK-58443). | 4.3.0 | review |
| 52 | `translate` | Keeps NUL replacement characters (SPARK-57747). | 4.3.0 | review |
| 53 | `collect_set` | Deduplicates NaNs, and returns -0.0 as 0.0 (SPARK-57298). | 4.2.0 | review |
| 54 | `mode` | Counts -0.0 as 0.0, and returns 0.0 (SPARK-57329). | 4.2.0 | review |

The fuzzer runs are listed under [Fuzzer runs](#fuzzer-runs). The review
covers the changes of Spark's expressions from 4.1.0 to 4.3.0.

The move changed these commits of the Spark 4.1.4 version:

- Fix 36 made `mask` treat a supplementary character as two characters, as
  Spark 4.2 and earlier do. Spark 4.3 treats it as one character
  (SPARK-48973), as Velox does, so the fix is dropped.
- The expression fuzzer skipped `reverse` of VARBINARY, which Spark 4.1
  reverses as a string, and `asinh` and `acosh`, which lose precision in
  Spark 4.1. Spark 4.2 fixed both (SPARK-54731, SPARK-56089), so the skips are
  dropped.
- a4fef3498 used `array_join` as an example of generated code that differs
  from the interpreted evaluation. Spark 4.2 fixed it (SPARK-57200), so the
  commit uses `map_concat` and `from_utc_timestamp`.

These changes of Spark 4.2 and 4.3 need no change in Velox:

- `element_at` returns NULL for an INT32_MIN index instead of an internal
  error (SPARK-57335), as Velox does.
- `count(DISTINCT m)` ignores the order of the map entries (SPARK-58420), as
  Velox does.
- `timestamp_seconds` throws `DATETIME_OVERFLOW` instead of a Java
  `ArithmeticException` on overflow (SPARK-57577). Velox throws too.
- Spark Connect returns the errors of `sequence` for illegal boundaries
  (SPARK-58627). With Spark 4.1, the query hung. The fuzzer still skips
  `sequence`, see [Known differences](#known-differences).

The runs against Spark 4.3.0 also found other differences, which fixes 55, 56
and 59 to 61 correct. Checks of decimal casts against Spark 4.3.0 led to fixes
57 and 58.

## Spark 3.5.1

Velox follows Spark 4.3.0 by default. Where Spark 3.5.1 returns other results
for a fix, a config restores the behavior of the earlier releases. The configs
are false by default, and `velox/docs/configs.rst` describes them.

| Fix | Function | Config | Behavior if true | Spark releases |
| --- | --- | --- | --- | --- |
| 4 | `overlay` | `spark.legacy_overlay_wrap_positions` | Computes `pos - 1` and `pos + len` as 32-bit integers that wrap around on overflow. | 4.2.0 and earlier |
| 27 | `split` | `spark.legacy_truncate_for_empty_regex_split` | Drops the rest of the input for an empty delimiter with a limit. | 4.0 and earlier |
| 49 | `covar_samp`, `corr` | `spark.legacy_covariance_merge_empty_buffer` | Merges intermediate results into empty buffers with the full formula, which returns NaN for an infinite or large average. | 4.2.1 and earlier |
| 50 | `corr` | `spark.legacy_corr_zero_variance` | Divides the co-moment by the product of the standard deviations even if a variance is zero. The result is NaN if the other variance is NaN or infinite. Otherwise it's NULL, or `DIVIDE_BY_ZERO` with ANSI mode on. | 4.2.1 and earlier |
| 52 | `translate` | `spark.legacy_translate_nul_as_deletion` | Deletes the characters whose replacement is NUL. | 4.2.1 and earlier |
| 53 | `collect_set` | `spark.legacy_collect_set_distinct_nans` | Returns every NaN input, and -0.0 as is. | 4.1.4 and earlier |
| 54 | `mode` | `spark.legacy_mode_negative_zero` | Returns -0.0 as is. | 4.1.4 and earlier |
| 57 | `cast` from string to decimal | `spark.legacy_decimal_zero_large_exponent` | Treats zeros with more than 38 integral digits as too large, e.g. `'0e38'`. | 4.1.3 and earlier, and 4.2.0 |

The Spark releases are those that behave like the config, according to their
sources. The tests of the configs expect the results of Spark 3.5.1, which ran
the queries on Java 17 through Spark Connect. The other fixes return the same
results in Spark 3.5.1 and 4.3.0, or need no config:

- Fixes 5 and 24 throw for the same inputs in Spark 3.5.1, but with Java
  exceptions instead of Spark's error classes, e.g. an `ArithmeticException`
  instead of `DATETIME_OVERFLOW` for `timestamp_seconds`.
- Fix 9: Spark 3.5.1 throws an `ArithmeticException` for `lpad` and `rpad`
  with a non-empty pad and a size below INT32_MIN plus the number of
  characters, e.g. `lpad('text', -2147483648)`. Spark 4.0.5, 4.1.4, 4.2.1 and
  4.3.0 return an empty string (SPARK-58708). Both return an empty string for
  the other negative sizes.
- Fix 12: Spark casts doubles to strings with Java's `Double.toString`, whose
  digits depend on the Java version, not on the Spark release.
- Fix 32: Spark 3.5.1 casts the quantity of `timestampadd` to INT. Spark 4.0
  and later accept BIGINT quantities (SPARK-50669).
- Fix 51: Spark 3.5.1 throws a `NegativeArraySizeException` for an INT32_MIN
  count of `substring_index`.

These differences from Spark 3.5.1 remain:

- `mode`: with `spark.legacy_mode_negative_zero`, Velox still counts -0.0 and
  0.0 as one value, and all NaNs as one value. Spark 4.1.4 and earlier count
  -0.0 and 0.0 separately, and Spark 3.5.1 counts each NaN separately
  (SPARK-45599).
- `collect_set`: with `spark.legacy_collect_set_distinct_nans`, Velox still
  deduplicates arrays and structs that differ only in -0.0 and 0.0. Spark
  4.1.4 and earlier keep them as separate elements.
- `covar_samp`, `corr`: Spark 4.2.1 and earlier also return NaN after merging
  an empty buffer into one with an infinite or large average.
  `spark.legacy_covariance_merge_empty_buffer` doesn't emulate this merge.
- `variance`, `var_samp`, `stddev`, `stddev_samp`, `skewness`, `kurtosis`:
  Spark 4.2.1 and earlier return NaN after the merges that
  `spark.legacy_covariance_merge_empty_buffer` emulates for `covar_samp` and
  `corr`. Velox never did, and no config emulates them.
- `round` of decimals: Spark 3.5.1 returns NULL if the precision of a value is
  below that of its type, e.g. for `round(percentile_approx(c0, 0.95), 3)`
  over 2.150. Spark 3.5.8, 4.0.2, 4.1.1 and later return the value, as Velox
  does (SPARK-54750).
- `cast` from TIMESTAMP to TINYINT, SMALLINT and INT: with ANSI mode off,
  Spark 3.5.1 wraps around on overflow. Spark 4.0 and later return NULL, as
  Velox does (SPARK-45816).
- `mask`: Spark 4.2.1 and earlier treat a supplementary character as two
  characters (SPARK-48973), see [Spark 4.3](#spark-43).
- `cast` from string to decimal: on Java 17, Spark returns NULL for
  `'0e2147483648'`, whose exponent exceeds INT32_MAX. On Java 21, it returns
  0, as Velox does.

## Framework

### Spark query runner

| Commit | Change |
| --- | --- |
| 805070216 | Upgrade the runner, the Spark Connect protos and the docker image to Spark 4.3.0. |
| 78ce954e1 | Classify Spark errors by exception class and SQLSTATE. Analysis errors, unsupported features, resource limits and fatal JVM errors are unsupported. Runtime errors are reference query failures that Velox must match. |
| 565f97bf6 | Translate typed expressions, constants of all types, types, aggregates and window functions to Spark SQL. |
| cfdea0a66 | Drop the temporary views of the input after each query. |
| 2d98254ab | Keep the last value of duplicate map keys, like Velox. |
| 27654cb6d, 9bb172f4c, 8658af687, c5857754a, 686dab798 | Exclude optimizer rules that skip evaluations, and subexpression elimination. |
| 82b3dfd67 | Report a query as unsupported if Spark fails to create the input views. |
| a4fef3498 | Evaluate queries without generated code, which differs from the interpreted evaluation, e.g. for `map_concat` and `from_utc_timestamp`. |
| 4e35336aa | Treat Spark's datetime upgrade errors as query failures. |
| 6d7f28195 | Keep executors alive on OutOfMemoryError. |
| 7007d31fc | Treat Spark's errors for invalid escapes in `LIKE` patterns as query failures. They are analysis errors that Spark raises while evaluating `LIKE`. |

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
| 59da7469e | Accept errors that only Spark throws if Velox throws for the smallest failing subexpression. Velox doesn't evaluate the arguments of default-null functions on null rows. |
| 9932d6375 | Accept errors that only Velox throws if Spark throws for the smallest failing subexpression. Velox evaluates all arguments of functions such as `array_prepend`. |
| 8bd8c380d, 3325d2cdb, 52b4a5283 | Skip signatures that Spark doesn't have or evaluates differently: `array_sort` with a key lambda, `levenshtein` with a threshold, `element_at(array, bigint)`, `array_intersect` of one argument, `array_position` with a start. |
| 044f1bf01, 6b2b30bb3 | Skip math functions whose results differ from Java in the last bit, see below. |
| e4d2ffba3, 21a21d7d4, 50ca99372, beb1601eb | Bound `round` scales, decimal `abs` and `unaryminus` precisions, pad sizes and `repeat` counts to the limits of Velox and Spark. |
| c728d68e5 | Generate valid `make_timestamp` fields and time zones. |
| e1db0cd7b | Generate regular expressions that behave the same in RE2 and `java.util.regex`. |
| e84338e06, faa99a485, 6972b63cc | Mark the `rlike` and `regexp_extract` patterns, the `array_insert` flag and the `timestampadd` and `timestampdiff` units as constant arguments. |
| bd3ccd752, 5f6fecb60 | Translate legacy `array_insert`, and generate positions within the arrays. |
| f8d29c8b9 | Generate valid `timestampadd` and `timestampdiff` units. |
| 169800f50 | Generate single-character `mask` replacements. |
| 4339d45b2, fdc5079bd, df5609bf0 | Translate `to_json` with a time zone, skip binary map keys, and generate valid time zones. |
| cbdd6f248 | Generate `LIKE` patterns with wildcards and escape sequences, and single-character escapes. |
| 8a840784d | Add `--velox_fuzzer_orderable_map_keys`, which the Spark fuzzer turns on. It generates map keys only of orderable types, because Spark rejects map keys that are or contain maps. |

### Spark aggregation fuzzer

| Commit | Change |
| --- | --- |
| f528d9df7 | Compare the results of `min` and `max` with Spark. They were exempt because DuckDB, the reference before Spark, returns null when it compares nested nulls. |
| 7fcd009a9 | Compare the results of functions with custom verification, e.g. `collect_list` and `first`, with Spark if an input column is DATE. The fuzzer skipped them because the Presto runner writes the input to DWRF files, which can't store DATE. |
| b786c50c5 | Generate positions in [0, 32768) for `bitmap_construct_agg` and 4096-byte bitmaps for `bitmap_or_agg`. Velox threw for random inputs in every iteration, so Spark never verified them. |
| 9eaf2218e | Generate no infinities for `covar_samp`. With them, its result depends on how the rows are split into partial aggregations, in both Velox and Spark. |

### Scripts

| Commit | Change |
| --- | --- |
| e6972384f | Add `setup.sh`, `spark-server.sh` and `spark_fuzzer.py` with `run`, `summary`, `repro` and `recheck`. |
| 344744168 | `spark_fuzzer.py sql` runs queries with the session configs of the fuzzer, on Parquet input. |
| 8df95fbdf | Stop the fuzzer when `spark_fuzzer.py` is interrupted. |
| 5a10442f0 | Detect reruns that generate other expressions than the failed iteration. |
| c63e3d6c1 | `summary --unsupported N` counts the unsupported test cases by reason. |
| 186880af2 | `run` and `summary` report the verified and unsupported iterations of the aggregation fuzzer. |
| 3923826c8 | `sql` starts the Spark Connect server if it is down, e.g. after a reboot. |
| d5b1aa00d | Add this report. |
| f8b701005 | Add the `spark-fuzzer` skill and agent, which run the fuzzers, triage the failures and commit the fixes. |
| 3c1e14dc7 | The skill fixes functions that accept argument types that Spark rejects, by restricting their signatures. |
| a78c85b9d | Add the one-hour runs and the fixes after them to this report. |
| 06184461b | Update this report for Spark 4.3. |

## Function fixes

Each fix has a unit test that fails without it, with the expected values taken
from Spark 4.3.0.

| # | Commit | Function | Fix |
| --- | --- | --- | --- |
| 1 | c21a46c58 | `array_sort` | Order nested nulls first. |
| 2 | d71550152 | `array_remove` | Compare nested nulls as values instead of throwing. |
| 3 | 110d1290b | datetime functions | Support empty patterns, e.g. `date_format(ts, '')`. |
| 4 | 1027dbaf5 | `overlay` | Compute `pos - 1` and `pos + len` as 64-bit integers, and clamp them to the 32-bit range (SPARK-58713). `spark.legacy_overlay_wrap_positions` keeps the old behavior. |
| 5 | 63298c90d | `timestamp_seconds` | Throw on overflow. |
| 6 | a8124425e | `round` | Round integers to negative scales, and handle large scales. |
| 7 | f1f2bd9f1 | `between` | Return false if the value is out of the range of the non-null bound. |
| 8 | 625679107 | `array_contains` | Compare nested nulls as values instead of throwing. |
| 9 | 0ded051b9 | `lpad`, `rpad` | Return an empty string for negative sizes, and truncate for an empty pad string. |
| 10 | a3f9bb2d1 | `regexp_extract_all` | Check the group only on a match. |
| 11 | 48909d889 | `regexp_extract` | Check the group only on a match. |
| 12 | b5ff356ac | `cast` to string | Cast REAL and DOUBLE with the shortest digits that round-trip. |
| 13 | 9012f3e35 | `substring_index` | Return an empty string for an empty delimiter. |
| 14 | 22e230e75 | `format_number` | Format the decimal digits that Java derives for doubles. |
| 15 | 05b2c45c1 | `bit_count` | Count the 64 bits of sign-extended negative values. |
| 16 | 9fbd6041f | `months_between` | Use the time since the start of the local day. |
| 17 | c7be6059a | `map_zip_with` | Keep the key order of the input maps. |
| 18 | 96460c275 | `json_array_length` | Follow Jackson's JSON syntax. |
| 19 | 42422f514 | `map_concat` | Keep the position of the first duplicate key. |
| 20 | 806f6f412 | `regexp_extract_all` | Use group 1 by default. |
| 21 | f34672cac | `regexp_extract` | Use group 1 by default. |
| 22 | e97fc74f4 | `atanh` | Compute it with `log1p`. |
| 23 | 2d1da1ae4 | `regexp_replace` | Replace an empty match right after a match. |
| 24 | 25aa48756 | `regexp_replace` | Use Java's replacement syntax. |
| 25 | 1ea7f681f | `regexp_replace` | Return the input for positions past the last character. |
| 26 | 683fd1df7 | `split` | Follow `Pattern.split` for empty matches. |
| 27 | ca9357a8b | `split` | Keep the rest of the input for an empty delimiter with a limit (SPARK-49968). `spark.legacy_truncate_for_empty_regex_split` keeps the old behavior. |
| 28 | df1861c40 | `regexp_extract_all` | Don't match inside multi-byte characters. |
| 29 | f81825e1a | `in` | Throw if the list isn't constant instead of treating it as empty. |
| 30 | 0dc5d78a9 | `second` | Apply the session time zone. |
| 31 | 2c7363dd7 | `timestampdiff` | Compare local date-times. |
| 32 | 29e4c0acf | `timestampadd` | Support BIGINT quantities, and throw on overflow. |
| 33 | d3c57a490 | `timestampadd` | Keep the offset of the input in a DST overlap. |
| 34 | 3ae60f116 | `timestampdiff` | Compare microseconds, and adjust month ends like `LocalDateTime.until`. |
| 35 | 1df568998 | `mask` | Keep a NUL replacement character. |
| 36 | - | `mask` | Dropped. Spark 4.3 treats a supplementary character as one character, as Velox does (SPARK-48973). |
| 37 | 19310f6dd | `like` | Escape with `\` by default, and throw for `\` before other characters or at the end. |
| 38 | f8ad66550 | `map` | Reject keys that are or contain maps. |
| 39 | 67137b917 | `map_from_arrays` | Reject keys that are or contain maps. |
| 40 | c1b7199b3 | `map_from_entries` | Reject keys that are or contain maps. |
| 41 | dabb8fb92 | `array_remove` | Reject elements that are or contain maps. |
| 42 | b93b4e46d | `array_union` | Reject elements that are or contain maps. |
| 43 | 586490cb8 | `array_distinct` | Reject elements that are or contain maps. |
| 44 | 71c3af901 | `array_intersect` | Reject elements that are or contain maps. |
| 45 | 72465a036 | `array_except` | Reject elements that are or contain maps. |
| 46 | 236276732 | `array_contains` | Reject elements that are or contain maps. |
| 47 | 24e0b6c77 | `array_position` | Reject elements that are or contain maps. |
| 48 | 16cac79db | `cast` from string | Parse REAL and DOUBLE as Java does, with type suffixes, e.g. `9f`, and hexadecimal numbers, e.g. `0x1.8p1`. |
| 49 | 835734a7e | `covar_samp` | Return infinite results instead of NaN (SPARK-58291). `spark.legacy_covariance_merge_empty_buffer` keeps the old behavior. |
| 50 | dc9e2295d | `corr` | Return NULL if x or y has zero variance, even if the other has NaN or infinity (SPARK-58213). `spark.legacy_corr_zero_variance` keeps the old behavior. |
| 51 | 27d223af6 | `substring_index` | Return the input for an INT32_MIN count (SPARK-58443). |
| 52 | 29a1915cc | `translate` | Keep NUL replacement characters (SPARK-57747). `spark.legacy_translate_nul_as_deletion` keeps the old behavior. |
| 53 | 909db24ac | `collect_set` | Deduplicate NaNs, and return -0.0 as 0.0 (SPARK-57298). `spark.legacy_collect_set_distinct_nans` keeps the old behavior. |
| 54 | 557984ed4 | `mode` | Return -0.0 as 0.0 (SPARK-57329). `spark.legacy_mode_negative_zero` keeps the old behavior. |
| 55 | 63aa1fea2 | `log2` | Compute `ln(x) / ln(2)`, as Spark does. |
| 56 | 21ed42edb | `pmod` | Keep the sign of zero remainders of REAL and DOUBLE. |
| 57 | 6cd94734a | `cast` from string to decimal | Handle huge exponents, e.g. `'0e50'`. `spark.legacy_decimal_zero_large_exponent` keeps the old behavior for zeros. |
| 58 | 16efde0fb | `cast` from string to decimal | Round once for negative exponents, e.g. `'44.5e-3'` to 0.04. |
| 59 | c45eb53f9 | datetime functions | Support the pattern letter `X`. |
| 60 | d1ea8f24b | `get_timestamp`, `unix_timestamp`, `to_unix_timestamp` | Split patterns with `S` on single quotes, so an unclosed quote starts a literal. |
| 61 | 0f4dbf339 | `cast` from string to timestamp | Return NULL for years of more than 6 digits and for timestamps out of the range of microseconds. |

## Known differences

These are not fixed. `known-issues.txt` skips the functions that fail in most
runs.

| Function | Difference |
| --- | --- |
| `acos`, `asin`, `atan`, `atan2`, `cbrt`, `cosh`, `hypot`, `log10` | glibc and Java's fdlibm or HotSpot intrinsics differ in the last bit for 3% to 55% of random inputs. Discontinuous functions, e.g. `width_bucket`, `floor` and casts to strings, amplify the difference. |
| `sin`, `cos`, `tan`, `sinh`, `tanh`, `exp`, `ln`, `log`, `log2` and other math functions | glibc and Java differ in the last bit for less than 1% of random inputs, so the fuzzer doesn't skip them. Seed 90010 failed for `width_bucket(cos(c1), tanh(sinh(c2)), c3, c4)` with 800317658043838737 buckets. `sinh` returned 0.19130470795657534 in Velox and 0.19130470795657536 in Spark, which moved the result from bucket 462593777917631553 in Spark to 462593777917631617 in Velox. Against Spark 4.3.0, `ln(0.6304716791491956)` returned -0.46128704266817416 in Velox and -0.4612870426681741 in Spark. |
| `array_repeat`, `array_insert`, `sequence` | Velox returns at most 10000 elements. The fuzzer skips `sequence`. |
| `repeat`, `lpad`, `rpad` | Velox returns at most 1 MB. |
| `str_to_map` | Spark treats the delimiters as regular expressions. |
| `aggregate` | Velox evaluates the initial state for rows with null arrays and fails on its errors. Spark returns null. |
| regex functions | RE2 and `java.util.regex` accept different syntax, e.g. back references, lookarounds, possessive quantifiers and a `{` that doesn't start a repetition. The fuzzer generates patterns that both interpret the same way. |
| `regexp_replace` | Spark matches empty strings between the UTF-16 surrogates of a supplementary character, e.g. `regexp_replace('a😀b', '[a-c]*', '<$0>')` is `<a><>?<>?<b><>`. Random strings have no supplementary characters. |
| `split` | For invalid UTF-8, Spark takes the length of a character from its first byte. Fix 27 matches Spark only for valid UTF-8. |
| `json_array_length` | Spark returns NULL for numbers of more than 1000 characters, Jackson's limit. |
| `to_json` | Spark writes binary map keys as Java object identities. The runner reports them as unsupported. |
| `cast` from string to DATE | Spark returns NULL for years of more than 7 digits, leading zeros included, e.g. `'00002020-01-01'` and `'00020200'`. Velox returns 2020-01-01 and 20200-01-01. |
| `cast` from DATE to string | Spark writes years after 9999 with a sign, e.g. `+10233-09-21`. Velox writes `10233-09-21`. The fuzzer generates dates from 1903 to 2036. |
| `map`, `map_from_entries` | Spark replaces -0.0 keys with 0.0 (SPARK-47563), e.g. it returns `{0.0 -> 2}` for the keys -0.0 and 0.0 with the values 1 and 2. Velox keeps -0.0 and 0.0 as distinct keys, as `MapTest.topLevelSignedZeroKeysStayDistinct` expects. Keys that contain -0.0, e.g. arrays, are equal to those with 0.0 in both. |
| `from_unixtime` | With the legacy formatter, `spark.legacy_date_formatter` in Velox and `spark.sql.legacy.timeParserPolicy=LEGACY` in Spark, `from_unixtime(0, '')` returns an empty string in Spark and NULL in Velox. |
| decimal `abs`, `unaryminus` | Spark rounds the results to 34 significant digits. The fuzzer generates precisions up to 34. |
| `levenshtein` | Spark's interpreted evaluation treats a null threshold as 0. The fuzzer skips the signature with a threshold. |
| `bitmap_or_agg` | Velox throws for bitmaps that aren't 4096 bytes long, as documented. Spark ORs the first 4096 bytes of bitmaps of any length. `bitmap_construct_agg` returns 4096 bytes. |

## Open issues

| Function | Issue |
| --- | --- |
| `date_format` and other datetime functions | Velox accepts Joda pattern letters that Spark's `DateTimeFormatter` rejects or interprets differently, e.g. `Y`, `w`, `e`, `x` and `C`. Fix 59 adds `X`, but a pattern with `X` and `z` or `Z` can still resolve the zone differently from Spark. |
| `array_union`, `sort_array` | A mismatch in campaign 3 with Spark's generated code didn't reproduce with interpreted evaluation. |

## Fuzzer runs

Each campaign runs the expression fuzzer for all functions except those in
`known-issues.txt`. A test case is unsupported if Spark can't evaluate it, e.g.
because it has no equivalent or rejects the arguments. A suppressed error is a
Spark error that Velox suppresses on rows that it doesn't evaluate.

### Spark 4.1.4

These runs used Spark 4.1.4 and the versions of the listed commits that
targeted it.

| Run | Build | Seeds | Passed | Test cases | Verified | Unsupported |
| --- | --- | --- | --- | --- | --- | --- |
| campaign1 | 0be11ee44 | 30 | 0 | - | - | - |
| campaign2 | 21a21d7d4 | 30 | 2 | 1331 | 939 | 342 |
| campaign3 | 59da7469e | 30 | 7 | 6848 | 4771 | 1821 |
| campaign4 | e6972384f | 24 | 9 | 8773 | 6131 | 2303 |
| campaign5 | 6b2b30bb3 | 24 | 7 | 7471 | 5345 | 1867 |
| campaign6 | 5a10442f0 | 12 | 9 | 10302 | 7365 | 2511 |
| campaign7 | df5609bf0 | 8 | 8 | 8528 | 6973 | 1010 |
| campaign8 | f8b701005 | 12 | 11 | 11169 | 9333 | 1179 |
| campaign9 | c1b7199b3 | 3 | 3 | 2983 | 2585 | 246 |
| campaign10 | 8a840784d | 3 | 2 | 2423 | 2129 | 104 |
| campaign11 | 16cac79db | 3 | 3 | 2693 | 2379 | 144 |

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
maps whose keys are maps. 8a840784d stops the fuzzer from generating such
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
| agg-smoke2 | f528d9df7 | all | 3 | 3 | 999 | 845 | 3 |
| agg-smoke3 | 7fcd009a9 | all | 3 | 3 | 934 | 872 | 4 |
| verify-agg-minmax | 3923826c8 | `min`, `max` | 3 | 3 | 824 | 824 | 0 |
| agg-campaign1 | f8b701005 | all | 11 | 11 | 8842 | 8243 | 35 |
| bitmap-agg | b786c50c5 | `bitmap_construct_agg`, `bitmap_or_agg` | 2 | 2 | 430 | 430 | 0 |
| agg-campaign2 | c1b7199b3 | all | 2 | 2 | 1281 | 1276 | 5 |

agg-campaign1 is the one-hour run, 11 seeds of 5 minutes. Velox threw in 564
of its iterations, all of them for `bitmap_construct_agg` positions out of
range and `bitmap_or_agg` bitmaps that aren't 4096 bytes long. After
b786c50c5, Velox threw in none of the iterations of agg-campaign2. In
agg-smoke2, the fuzzer also skipped functions with custom verification if an
input column was DATE, which 7fcd009a9 fixes. All unsupported iterations are
`mode` with `DISTINCT`, which Spark rejects.

### Spark 4.3.0

spark43-expr, spark43-agg and spark43-asinh-acosh used the last build for
Spark 4.1.4, before the changes in [Spark 4.3](#spark-43). spark43-expr,
final-expr, spark43-agg and final-agg are the one-hour runs, 12 seeds of 5
minutes. Each seed ran for 90 seconds to 5 minutes and stopped at the first
failure.

| Run | Build | Seeds | Passed | Test cases | Verified | Unsupported |
| --- | --- | --- | --- | --- | --- | --- |
| spark43-expr | last 4.1.4 build | 12 | 4 | 5157 | 4556 | 277 |
| final-expr | 29a1915cc | 12 | 10 | 15124 | 13424 | 661 |
| head-expr | c45eb53f9 | 3 | 3 | 3229 | 2881 | 157 |
| c43-expr-all | 0f4dbf339 | 3 | 3 | 4136 | 3666 | 174 |

The 8 failed seeds of spark43-expr hit overflowing `overlay` positions, which
fix 4 now clamps as Spark 4.3 does. The 2 failed seeds of final-expr hit the
pattern letter `X`, which fix 59 supports, and `log2` and `pmod`, which fixes
55 and 56 correct.

| Run | Build | Functions | Passed | Verified test cases |
| --- | --- | --- | --- | --- |
| spark43-asinh-acosh | last 4.1.4 build | `asinh`, `acosh` | 3 of 3 | 4665 of 4665 |
| final-mask | 29a1915cc | `mask` | 3 of 3 | 1517 of 1517 |
| final-reverse | 29a1915cc | `reverse` | 3 of 3 | 1486 of 1486 |
| t43-expr-str | c45eb53f9 | `overlay`, `substring_index`, `translate`, `reverse`, `cast` | 3 of 3 | 3856 of 3862 |
| t43-expr-dt | c45eb53f9 | datetime functions, `ln`, `log2`, `pmod`, `mask`, `cast` | 0 of 3 | - |
| c43-expr-dt | 0f4dbf339 | datetime functions, `ln`, `log2`, `pmod`, `mask`, `cast` | 2 of 3 | 3167 of 3324 |

spark43-asinh-acosh, final-mask and final-reverse ran functions that the
Spark 4.1.4 version skipped or fixed differently. t43-expr-dt failed for
unclosed quotes in patterns with `S`, which fix 60 supports, and for timestamp
strings out of Spark's range, which fix 61 rejects. c43-expr-dt failed for the
last-bit difference of `ln` that is listed under known differences.

| Run | Build | Functions | Seeds | Passed | Iterations | Verified | Unsupported |
| --- | --- | --- | --- | --- | --- | --- | --- |
| spark43-agg | last 4.1.4 build | all | 12 | 0 | - | - | - |
| spark43-covar-dataspec | 9eaf2218e | `covar_samp` | 4 | 4 | 1909 | 1909 | 0 |
| final-agg | 29a1915cc | all | 12 | 12 | 9277 | 9232 | 45 |
| final-corr-covar | 29a1915cc | `corr`, `covar_samp` | 3 | 3 | 660 | 660 | 0 |
| t43-agg | c45eb53f9 | `mode`, `collect_set`, `covar_samp`, `covar_pop`, `corr` | 3 | 3 | 1859 | 1764 | 95 |
| head-agg | c45eb53f9 | all | 3 | 3 | 1831 | 1827 | 4 |

spark43-agg failed 4 seeds for `covar_samp`, which now returns infinite
results with fix 49, and 8 seeds for `corr`, which now returns NULL for zero
variance with fix 50. With fix 49, 3 of the failed iterations of `covar_samp`
returned NaN or infinity depending on the partial aggregations, which
9eaf2218e avoids. All unsupported iterations are `mode` with `DISTINCT`.

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
  and the runner converts only top-level columns back. In final-expr against
  Spark 4.3.0, 4.4% of the test cases are unsupported. The largest groups are
  `IN` with NULL, non-constant or empty lists (188), Spark errors that Velox
  suppresses (129), `to_json` of scalars (71) and casts that Spark doesn't
  allow (65).
- Velox's variadic `map` signature accepts any arguments after the first key
  and value, e.g. an odd number of arguments or keys of different types. Both
  Spark and Velox reject these calls, so these test cases are unsupported.
- Random strings only have characters of the Basic Multilingual Plane, so the
  fuzzer doesn't cover supplementary characters, e.g. emojis.
- Where Spark 4.2 or 4.3 changed a behavior, Velox follows Spark 4.3.0 by
  default, see [Spark 4.3](#spark-43). Configs restore the behaviors of
  earlier releases. [Spark 3.5.1](#spark-351) lists them and the differences
  from Spark 3.5.1 that remain.
