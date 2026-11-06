# ANSI mode

This document describes how Velox's Spark functions follow Spark's ANSI mode,
checked against Spark 4.3.0 with the Spark expression and aggregation fuzzers.
It lists the functions and casts whose behavior depends on ANSI mode, the
fixes, the upstream pull requests they build on, the ANSI cases that Velox
doesn't support and the fuzzer runs. [README.md](README.md#ansi-mode) describes
how to run the fuzzers in ANSI mode, and [REPORT.md](REPORT.md) the rest of the
fuzzer work.

## Configs

The Velox query config `spark.ansi_enabled`, false by default, corresponds to
Spark's `spark.sql.ansi.enabled`. A Velox function reads it only if Spark's
behavior depends on `spark.sql.ansi.enabled`. Functions that behave the same in
both modes ignore it, including those that always throw, e.g. `url_decode`.

`spark_fuzzer.py run --ansi on` sets both configs to true. The examples below
are results of Spark 4.3.0 with the UTC session time zone.

## Functions

With ANSI mode on, these functions throw where they wrap around, saturate or
return NULL with ANSI mode off.

| Function | Case | ANSI off | ANSI on | Velox |
| --- | --- | --- | --- | --- |
| `+`, `-`, `*` of integers | Overflow, e.g. `2147483647 + 1` | Wraps around | `ARITHMETIC_OVERFLOW` | Fixes 66 to 68 |
| `+`, `-`, `*` of decimals | Overflow | NULL | `NUMERIC_VALUE_OUT_OF_RANGE` | Fixes 66 to 68 |
| `/` of doubles | Zero divisor | NULL | `DIVIDE_BY_ZERO` | Fix 63 |
| `/` of decimals | Zero divisor, overflow | NULL | `DIVIDE_BY_ZERO`, `NUMERIC_VALUE_OUT_OF_RANGE` | facebookincubator/velox#19290 |
| `div` | Zero divisor | NULL | `DIVIDE_BY_ZERO` | Fix 64 |
| `div` | Overflow, e.g. `-9223372036854775808L div -1L` | Wraps around for integers, NULL for decimals | `ARITHMETIC_OVERFLOW` for integers, `NUMERIC_VALUE_OUT_OF_RANGE` for decimals | Fix 64 |
| `%` | Zero divisor | NULL | `REMAINDER_BY_ZERO` | Already followed |
| `pmod` | Zero divisor | NULL | `REMAINDER_BY_ZERO` | facebookincubator/velox#19231 |
| `abs`, `negative` | Overflow, e.g. `abs(-2147483648)` | Wraps around | `ARITHMETIC_OVERFLOW` | Already followed |
| `round` of integers | Overflow, e.g. `round(2147483647, -1)` | Wraps around | `ARITHMETIC_OVERFLOW` | facebookincubator/velox#19480 |
| `ceil`, `floor` of doubles | Out of the range of BIGINT, new in Spark 4.3 | Saturates | `ARITHMETIC_OVERFLOW` | facebookincubator/velox#20058 |
| `element_at` of arrays | Index out of bounds | NULL | `INVALID_ARRAY_INDEX_IN_ELEMENT_AT` | facebookincubator/velox#19332 |
| `elt` | Index out of bounds | NULL | `INVALID_ARRAY_INDEX` | Already followed |
| `conv` | Overflow | Saturates | `ARITHMETIC_OVERFLOW` | facebookincubator/velox#19331 |
| `next_day` | Invalid day of the week | NULL | `ILLEGAL_DAY_OF_WEEK` | facebookincubator/velox#19334 |
| `make_date` | Invalid field | NULL | `DATETIME_FIELD_OUT_OF_BOUNDS` | facebookincubator/velox#18764 |
| `make_timestamp`, `make_timestamp_ntz` | Invalid field | NULL | `DATETIME_FIELD_OUT_OF_BOUNDS` | Already followed |
| `to_timestamp`, `to_date`, `get_timestamp` in Velox | Unparsable string | NULL | `CANNOT_PARSE_TIMESTAMP` | facebookincubator/velox#19333 |
| `unix_timestamp`, `to_unix_timestamp` | Unparsable string | NULL | `CANNOT_PARSE_TIMESTAMP` | Already followed |
| `sum` of integers | Overflow | Wraps around | `ARITHMETIC_OVERFLOW` | facebookincubator/velox#20028 |
| `sum` of decimals | Overflow | NULL | `ARITHMETIC_OVERFLOW` | facebookincubator/velox#20028 |
| `avg` of decimals | Overflow of the sum | NULL | `ARITHMETIC_OVERFLOW` | facebookincubator/velox#17937, fix 70 |
| `cast` | See [Casts](#casts) | | | |

Spark turns ANSI mode into a parameter of two expressions, and Velox takes these
parameters as arguments:

- `size(NULL)` returns -1 if `spark.sql.legacy.sizeOfNull` is true and ANSI
  mode is off, and NULL otherwise. Spark's `Size` takes the result of this rule
  as a parameter, and Velox's `size` as its second argument. b0231d6e7
  translates `size(x, true)` to Spark SQL that returns -1 for NULL in both
  modes.
- Spark's `MakeDecimal` returns NULL on overflow with ANSI mode off and throws
  with ANSI mode on. Velox's `make_decimal` takes this choice as its
  `nullOnOverflow` argument. Spark SQL has no function for `MakeDecimal`, so
  the fuzzer can't test it.

These functions behave the same in both modes, and Velox ignores
`spark.ansi_enabled` for them:

- `get` of arrays, and `element_at` and `[]` of maps, return NULL for an index
  out of bounds or a missing key.
- `element_at` and `split_part` throw `INVALID_INDEX_OF_ZERO` for the index 0.
- `url_decode` throws `CANNOT_DECODE_URL` for invalid input, and `unhex`
  returns NULL.
- `make_ym_interval`, `timestamp_seconds` and `add_months` throw on overflow.
  `date_add` wraps around.
- `try_add`, `try_subtract`, `try_multiply`, `try_make_timestamp` and
  `try_make_timestamp_ntz` return NULL instead of throwing.

## Casts

With ANSI mode on, `cast` throws where it returns NULL, wraps around or
saturates with ANSI mode off:

| Cast | Case | ANSI off | ANSI on |
| --- | --- | --- | --- |
| String to numbers, BOOLEAN, DATE and TIMESTAMP | Invalid input, e.g. `cast('1.5' as int)` | NULL | `CAST_INVALID_INPUT` |
| Integers to narrower integers | Overflow, e.g. `cast(300 as tinyint)` | Wraps around | `CAST_OVERFLOW` |
| REAL and DOUBLE to integers | Out of range, NaN | Converts like Java, e.g. `cast(1e20D as int)` is 2147483647 and NaN is 0 | `CAST_OVERFLOW` |
| Decimals to integers | Overflow | Wraps around | `CAST_OVERFLOW` |
| TIMESTAMP to integers | Overflow | NULL | `CAST_OVERFLOW` |
| Numbers and TIMESTAMP to decimals | Overflow | NULL | `NUMERIC_VALUE_OUT_OF_RANGE` |
| REAL and DOUBLE to TIMESTAMP | Out of range | Saturates | `CAST_OVERFLOW` |
| REAL and DOUBLE to TIMESTAMP | NaN, infinity | NULL | `CAST_INVALID_INPUT` |
| Arrays, maps and structs | An element cast fails | The element is NULL | The error of the element cast |

Velox's Spark cast reads `spark.ansi_enabled` for these casts, and
facebookincubator/velox#19311 applies it to the elements of arrays, maps and
structs. `try_cast` returns NULL for these cases in both modes.

These casts never fail and return the same results in both modes:

- Integers to TIMESTAMP saturate.
- DOUBLE to REAL returns infinity out of the range of REAL, as does a string
  to REAL or DOUBLE out of range.
- Decimals to DOUBLE and REAL, TIMESTAMP to DATE, DOUBLE and REAL.
- REAL and DOUBLE to decimals return NULL for NaN and infinity.

With ANSI mode on, Spark rejects these casts during analysis with
`DATATYPE_MISMATCH`. Its `try_cast` rejects them in both modes, unless they are
nested in arrays, maps or structs, e.g.
`try_cast(array(date'2020-01-01') as array<int>)` returns `[NULL]`.

| Cast | ANSI off | Velox |
| --- | --- | --- |
| BOOLEAN to TIMESTAMP | 1 microsecond after the epoch for true | Already followed |
| TIMESTAMP to BOOLEAN | False for the epoch, true otherwise | Fix 89 |
| DATE to numbers and BOOLEAN | NULL | Fix 92 |
| Integers to VARBINARY | The big-endian bytes | Already followed |

Velox evaluates these casts as with ANSI mode off in both modes, since Spark
has no ANSI result for them.

## Unsupported cases

| Function | Case | Spark 4.3.0 | Velox |
| --- | --- | --- | --- |
| `arr[i]` | Index out of bounds | NULL with ANSI off, `INVALID_ARRAY_INDEX` with ANSI on | Velox has no Spark function for `arr[i]`. Its `get` returns NULL in both modes, as Spark's `get` does. facebookincubator/velox#20020 proposes `get_array_item`, which follows ANSI mode. |
| `get` of arrays | BIGINT index outside the INTEGER range | Casts the index to INTEGER. With ANSI off, the index wraps around, e.g. `get(array(1, 2, 3), 4294967297L)` returns 2. With ANSI on, it throws `CAST_OVERFLOW`. | Returns NULL in both modes. Spark's analyzer adds the cast, so Spark plans call `get` with an INTEGER index. The fuzzer skips the BIGINT signature. |
| `bround`, `parse_url`, `make_interval` | Overflow, invalid URL | Throw with ANSI on, e.g. `ARITHMETIC_OVERFLOW` for `bround(2147483647, -1)` | Velox doesn't have these functions. |
| `%`, `pmod` of decimals | Zero divisor | NULL with ANSI off, `REMAINDER_BY_ZERO` with ANSI on | Velox's `remainder` and `pmod` have no decimal signatures. facebookincubator/velox#19330 and facebookincubator/velox#19522 propose them with the ANSI errors. |
| `sum`, `avg` of decimals | A sum overflows within a partial aggregation and comes back into range | Checks the sum after each row. The DECIMAL(38, 0) values max, max and -max return NULL with ANSI off and throw `ARITHMETIC_OVERFLOW` with ANSI on. The order max, -max, max returns max. | Checks the sum of each partial aggregation, so it returns max for both orders. `SumAggregationTest.ansiDecimalOverflow` from facebookincubator/velox#20028 expects this. |
| `cast` from decimals to TINYINT, SMALLINT and INTEGER | Precision above 18, ANSI on or `try_cast` | Truncates the nearest double of the decimal, unless Spark stores the decimal in a long, e.g. the sum of two DECIMAL(18, 17) values | Fix 84 truncates the nearest double for every precision above 18, so `try_cast(c0 + c1 as int)` can differ for such sums. |
| `cast` from decimals to TIMESTAMP | More than 34 significant digits | The interpreted evaluation rounds the decimal to 34 digits first. The generated code keeps every digit. | Keeps every digit. The fuzzer generates precisions up to 34. |
| Casts that Spark rejects with ANSI on | | Analysis error | Evaluates them as with ANSI mode off |

The fuzzers can't verify these cases:

- With ANSI mode on, `sum` of integers throws if a partial sum overflows, in
  both Spark and Velox. The result depends on the order of the rows: the values
  9223372036854775807, 1 and -1 throw, and the order 9223372036854775807, -1
  and 1 returns 9223372036854775807. The engines add the rows in different
  orders.
- `try` and the `checked_*` functions have no Spark SQL equivalent, so the
  fuzzer doesn't choose them. When it retries a test case in `try`, Spark
  reports the retry as unsupported.
- Spark reports the casts that it rejects with ANSI mode on as unsupported in
  ANSI-on runs. The fuzzer still generates them, which wastes some iterations.

## Fixes

Each fix has a unit test that fails without it. The expected values come from
Spark 4.3.0. The fuzzer runs of the ANSI work also found bugs that don't depend
on ANSI mode, and some of the fixes correct them.

| # | Commit | Function | Fix |
| --- | --- | --- | --- |
| 62 | 1b0b9d8cb | `div` | Divide INTEGER -2147483648 by -1 as BIGINT instead of crashing. |
| 63 | 112616091 | `/` of doubles | Throw for a zero divisor in ANSI mode. |
| 64 | fb6b43de6 | `div` | Throw for a zero divisor and overflow in ANSI mode. |
| 65 | df7e1aaa7 | `div` of decimals | Return the quotient when rescaling exceeds 128 bits. |
| 66 | a381aae8b | `+` | Throw on overflow in ANSI mode. |
| 67 | e04728f26 | `-` | Throw on overflow in ANSI mode. |
| 68 | 56a301dab | `*` | Throw on overflow in ANSI mode. |
| 69 | bfaa77ac8 | `cast` from decimals to integers | Throw on overflow in ANSI mode. |
| 70 | 20e0212ac | `avg` of decimals | Return NULL when the sum exceeds the precision of the sum type. |
| 71 | ae0e5dc1d | `try_cast` from DOUBLE to REAL | Return infinity out of the range of REAL. |
| 72 | 2145026ef | `cast` to BOOLEAN | Cast NaN to true. |
| 73 | 9958d699c | `cast` from string to TIMESTAMP | Follow Spark's timestamp syntax, e.g. `'2020-01-01 10'`. |
| 74 | 451821927 | `cast` from string to TIMESTAMP | Resolve zone IDs as Spark does, e.g. `-18:00`. |
| 75 | bfd477896 | `cast` from string to TIMESTAMP | Move local times in DST gaps forward. |
| 76 | d8a854663 | `cast` from DATE to string | Add `+` to years after 9999. |
| 77 | 7e99c591c | datetime functions | Support the pattern letter `g`, the modified Julian day. |
| 78 | 538d879ca | datetime functions | Reject the patterns that Spark rejects. |
| 79 | d0b44cae8 | datetime functions | Support optional sections in patterns. |
| 80 | 879408ee6 | `cast` from DATE to TIMESTAMP | Throw on overflow in both modes. |
| 81 | 188a0f40a | `cast` from REAL and DOUBLE to integers | Saturate 2^63 and 2^31 with ANSI mode off. |
| 82 | 0440e2aaa | `cast` from REAL and DOUBLE to decimals | Use the digits of Java's `Double.toString`. |
| 83 | 947ff2b29 | `cast` from REAL and DOUBLE to decimals | Return NULL for NaN and infinity in ANSI mode. |
| 84 | eeff86191 | `cast` from decimals to integers | Truncate the nearest double of decimals with precision above 18 in ANSI mode and `try_cast`. |
| 85 | 81b3d5301 | `try` | Return NULL for casts from DATE to TIMESTAMP that overflow. |
| 86 | 5790cc6a0 | `cast` from string to decimals | Accept the digits of other scripts. |
| 87 | f731f8041 | `cast` from DOUBLE to TIMESTAMP | Multiply in double precision. |
| 88 | 0a941fae1 | `cast` from TIMESTAMP to DOUBLE and REAL | Support it. |
| 89 | b5070e73f | `cast` from TIMESTAMP to BOOLEAN | Support it. |
| 90 | a2519271c | `cast` from TIMESTAMP to decimals | Support it. |
| 91 | 43d25f003 | `cast` from decimals to TIMESTAMP | Support it. |
| 92 | 3f81deeaf | `cast` from DATE to numbers and BOOLEAN | Return NULL. |
| 93 | 3840dda59 | datetime functions | Read the pattern letter `x` as a zone offset. |

## Upstream pull requests

The branch includes these Velox pull requests, cherry-picked before the fixes.
They make Velox functions follow ANSI mode or Spark's casts.

| Pull request | Commits | Change |
| --- | --- | --- |
| facebookincubator/velox#19141 | 622c915ab | Correct shared decimal rounding and overflow. |
| facebookincubator/velox#18821 | ba6ba6e51 | Trim strings in casts as Spark does. |
| facebookincubator/velox#17647 | 7235477ea | Cast decimals to strings without scientific notation. |
| facebookincubator/velox#19311 | 39638b8c8 | Apply ANSI mode to casts of arrays, maps and structs. |
| facebookincubator/velox#20058 | c612eece7, 3908719e7 | Throw on `ceil` and `floor` overflow in ANSI mode. |
| facebookincubator/velox#19231 | fe3e660ad | Correct `pmod`, and throw for a zero divisor in ANSI mode. |
| facebookincubator/velox#19332 | 105b10a7b | Throw on `element_at` of arrays out of bounds in ANSI mode. |
| facebookincubator/velox#19334 | 4f161a874 | Throw for an invalid `next_day` day of the week in ANSI mode. |
| facebookincubator/velox#19331 | e2eecfcbf | Throw on `conv` overflow in ANSI mode. |
| facebookincubator/velox#18764 | 97633b0d8 | Throw for invalid `make_date` fields in ANSI mode. |
| facebookincubator/velox#20028 | 2d8fec0bb | Throw on `sum` overflow in ANSI mode. |
| facebookincubator/velox#19333 | cf98268ed | Throw for `get_timestamp` parse errors in ANSI mode. |
| facebookincubator/velox#19290 | d4297dbb3 | Throw for decimal `/` overflow and zero divisors in ANSI mode. |
| facebookincubator/velox#17937 | a2a41bfa8 | Throw on decimal `avg` overflow in ANSI mode. |
| facebookincubator/velox#19480 | 66cfd8f9d | Add `round` of integers, which throws on overflow in ANSI mode. |

The ANSI work started on Spark 4.1.4. Its version of these fixes was dropped
because a pull request does the same:

| Fix of the Spark 4.1.4 version | Replaced by |
| --- | --- |
| `pmod` throws for a zero divisor in ANSI mode | facebookincubator/velox#19231 |
| `pmod` keeps the sign of zero remainders of REAL and DOUBLE | Fix 56 |
| `element_at` of arrays throws out of bounds in ANSI mode | facebookincubator/velox#19332 |
| `next_day` throws for an invalid day in ANSI mode | facebookincubator/velox#19334 |
| `conv` throws on overflow in ANSI mode | facebookincubator/velox#19331 |
| `make_date` throws for invalid fields in ANSI mode | facebookincubator/velox#18764 |
| `sum` throws on overflow in ANSI mode | facebookincubator/velox#20028 |
| `get_timestamp` throws for parse errors in ANSI mode | facebookincubator/velox#19333 |
| Casts of arrays, maps and structs follow ANSI mode | facebookincubator/velox#19311 |
| `cast` from string to TIMESTAMP returns NULL out of the range of Spark | Fix 61 |
| `cast` from string trims whitespace as Spark does | facebookincubator/velox#18821 |
| `cast` from decimals to strings uses plain notation | facebookincubator/velox#17647 |

Two fixes keep only what the pull requests don't do. Fix 63 makes `/` of
doubles throw for a zero divisor, and facebookincubator/velox#19290 handles
decimals. Fix 70 checks the decimal sum of `avg` against the precision of the
sum type, and facebookincubator/velox#17937 makes the overflow throw. `ceil`,
`floor` and `round` needed no fix, because facebookincubator/velox#20058 and
facebookincubator/velox#19480 cover them.

## Spark 4.1.4 and 4.3.0

The ANSI fixes were first written against Spark 4.1.4. The examples of their
commit messages and 134 other queries return the same results in Spark 4.1.4
and 4.3.0, with one exception: with ANSI mode on, Spark 4.3 throws
`ARITHMETIC_OVERFLOW` for `ceil` and `floor` of doubles out of the range of
BIGINT, which facebookincubator/velox#20058 follows. Of the Spark expressions
that Velox has, only `ceil` and `floor` read ANSI mode in Spark 4.3.0 and not
in Spark 4.1.4.

## Fuzzer runs

These runs used Spark 4.3.0, with 10 seeds of 3 minutes each. A seed stops at
the first failure. The cast and expression runs used the build of 8064a9bd3,
and the aggregation runs the build of 3840dda59.
[REPORT.md](REPORT.md#fuzzer-runs) describes the counts.

| Run | Functions | ANSI | Seeds | Passed | Test cases | Verified | Unsupported |
| --- | --- | --- | --- | --- | --- | --- | --- |
| ansi43-cast-off | `cast`, `try_cast` | off | 10 | 10 | 11031 | 10816 | 214 |
| ansi43-cast-on | `cast`, `try_cast` | on | 10 | 10 | 8807 | 7590 | 608 |
| ansi43-expr-off | all | off | 10 | 9 | 5918 | 5277 | 272 |
| ansi43-expr-on | all | on | 10 | 10 | 5209 | 4179 | 307 |

Seed 722 of ansi43-expr-off failed for `date_format` with the pattern letter
`x`. Velox printed the week year, e.g. 1968, and Spark printed the zone offset,
e.g. -07. With fix 93, the failed iteration passes.

With ANSI mode on, Spark rejects the casts listed in [Casts](#casts), so more
test cases are unsupported. In both modes, the runs verified every function and
cast that the fuzzer generates, except:

- With ANSI mode on, casts to VARBINARY and to arrays, maps and structs of
  VARBINARY. In both modes, `try_cast` to VARBINARY. The fuzzer casts only
  integers to VARBINARY, which Spark rejects in these cases.
- `make_ym_interval` with ANSI mode on, which the fuzzer didn't choose. It
  throws on overflow in both modes.

The fuzzers skip the functions in `known-issues.txt` and the functions and
signatures in their own skip lists. Of these, the following depend on ANSI
mode:

- `unix_timestamp`, because it returns the current time without arguments.
  `to_unix_timestamp` has the same implementation, and the runs verified it.
- The `_deny_precision_loss` variants of decimal `+`, `-`, `*` and `/`, which
  Spark uses if `spark.sql.decimalOperations.allowPrecisionLoss` is false. They
  share the implementation of the variants that the runs verified.
- `get` of arrays with a BIGINT index, see [Unsupported cases](#unsupported-cases).

All seeds of the aggregation runs passed:

| Run | ANSI | Seeds | Passed | Iterations | Verified | Unsupported |
| --- | --- | --- | --- | --- | --- | --- |
| ansi43-agg-off | off | 10 | 10 | 4203 | 4185 | 18 |
| ansi43-agg-on | on | 10 | 10 | 4239 | 4146 | 26 |

The aggregation fuzzer ran 21 of the 27 aggregate functions that Velox
registers for Spark, and the other 6 don't depend on ANSI mode. With ANSI mode
on, `sum` threw in 67 iterations, and Spark threw too. The unsupported
iterations are `mode` with `DISTINCT`, which Spark rejects. The results of
`first`, `last`, `max_by`, `min_by`, `mode`, `skewness` and `kurtosis` depend
on the order of the rows, so the fuzzer doesn't compare them with Spark. These
functions behave the same in both modes.
