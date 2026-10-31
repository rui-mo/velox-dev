/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <gtest/gtest.h>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/sparksql/aggregates/Register.h"
#include "velox/functions/sparksql/fuzzer/SparkSql.h"
#include "velox/functions/sparksql/registration/Register.h"
#include "velox/functions/sparksql/window/WindowFunctionsRegistration.h"
#include "velox/parse/Expressions.h"
#include "velox/parse/ExpressionsParser.h"
#include "velox/parse/TypeResolver.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::functions::sparksql::fuzzer::test {
namespace {

class SparkSqlTest : public ::testing::Test,
                     public velox::test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
    registerFunctions("");
    aggregate::sparksql::registerAggregateFunctions("");
    window::sparksql::registerWindowFunctions("");
    parse::registerTypeResolver();
  }

  core::TypedExprPtr parse(const std::string& text) {
    parse::ParseOptions options;
    options.parseDecimalAsDouble = false;
    options.parseIntegerAsBigint = false;
    return core::Expressions::inferTypes(
        parse::DuckSqlExpressionsParser(options).parseExpr(text),
        kRowType,
        pool());
  }

  std::string toSql(const std::string& text) {
    return toSparkSql(parse(text));
  }

  static std::string toSql(const TypePtr& type, const Variant& value) {
    return toSparkSql(std::make_shared<core::ConstantTypedExpr>(type, value));
  }

  static std::string toSql(const VectorPtr& vector, vector_size_t index = 0) {
    return toSparkSql(
        std::make_shared<core::ConstantTypedExpr>(
            BaseVector::wrapInConstant(1, index, vector)));
  }

  static core::TypedExprPtr field(const std::string& name) {
    return std::make_shared<core::FieldAccessTypedExpr>(
        kRowType->findChild(name), name);
  }

  static core::TypedExprPtr call(
      const TypePtr& type,
      const std::string& name,
      std::vector<core::TypedExprPtr> inputs) {
    return std::make_shared<core::CallTypedExpr>(type, std::move(inputs), name);
  }

  template <typename T>
  static core::TypedExprPtr constant(const TypePtr& type, T value) {
    return std::make_shared<core::ConstantTypedExpr>(type, Variant(value));
  }

  // Returns the plan node of type 'T' at the root of 'plan'.
  template <typename T>
  static const T& node(const core::PlanNodePtr& plan) {
    const auto* typedNode = dynamic_cast<const T*>(plan.get());
    VELOX_CHECK_NOT_NULL(typedNode);
    return *typedNode;
  }

  inline static const RowTypePtr kRowType =
      ROW({"c0", "c1", "b", "s", "arr", "ts", "r", "dup", "a b"},
          {INTEGER(),
           INTEGER(),
           BOOLEAN(),
           VARCHAR(),
           ARRAY(INTEGER()),
           TIMESTAMP(),
           ROW({"x", "X"}, {INTEGER(), INTEGER()}),
           ROW({"x", "x"}, {INTEGER(), INTEGER()}),
           BIGINT()});
};

TEST_F(SparkSqlTest, identifiers) {
  EXPECT_EQ(toSparkIdentifier("c0"), "c0");
  EXPECT_EQ(toSparkIdentifier("_a_1"), "_a_1");
  EXPECT_EQ(toSparkIdentifier("a b"), "`a b`");
  EXPECT_EQ(toSparkIdentifier("1a"), "`1a`");
  EXPECT_EQ(toSparkIdentifier("a`b"), "`a``b`");
  EXPECT_EQ(toSparkIdentifier(""), "``");

  using Names = std::vector<std::string>;
  EXPECT_EQ(toSparkFieldNames(*ROW({"a", "B"}, BIGINT())), (Names{"a", "B"}));
  EXPECT_EQ(
      toSparkFieldNames(*ROW({"a", "A"}, BIGINT())), (Names{"col1", "col2"}));
  EXPECT_EQ(
      toSparkFieldNames(*ROW({"", "b"}, BIGINT())), (Names{"col1", "col2"}));
}

TEST_F(SparkSqlTest, types) {
  EXPECT_EQ(toSparkTypeSql(BOOLEAN()), "BOOLEAN");
  EXPECT_EQ(toSparkTypeSql(TINYINT()), "TINYINT");
  EXPECT_EQ(toSparkTypeSql(SMALLINT()), "SMALLINT");
  EXPECT_EQ(toSparkTypeSql(INTEGER()), "INT");
  EXPECT_EQ(toSparkTypeSql(BIGINT()), "BIGINT");
  EXPECT_EQ(toSparkTypeSql(REAL()), "FLOAT");
  EXPECT_EQ(toSparkTypeSql(DOUBLE()), "DOUBLE");
  EXPECT_EQ(toSparkTypeSql(VARCHAR()), "STRING");
  EXPECT_EQ(toSparkTypeSql(VARBINARY()), "BINARY");
  EXPECT_EQ(toSparkTypeSql(TIMESTAMP()), "TIMESTAMP");
  EXPECT_EQ(toSparkTypeSql(DATE()), "DATE");
  EXPECT_EQ(toSparkTypeSql(DECIMAL(10, 2)), "DECIMAL(10, 2)");
  EXPECT_EQ(toSparkTypeSql(DECIMAL(38, 0)), "DECIMAL(38, 0)");
  EXPECT_EQ(toSparkTypeSql(INTERVAL_YEAR_MONTH()), "INTERVAL YEAR TO MONTH");
  EXPECT_EQ(
      toSparkTypeSql(MAP(VARCHAR(), ARRAY(DATE()))),
      "MAP<STRING, ARRAY<DATE>>");
  EXPECT_EQ(
      toSparkTypeSql(ROW({"a", "b c"}, {INTEGER(), ROW({"x"}, {DOUBLE()})})),
      "STRUCT<`a`: INT, `b c`: STRUCT<`x`: DOUBLE>>");
  EXPECT_EQ(
      toSparkTypeSql(ROW({"a", "a"}, {INTEGER(), BOOLEAN()})),
      "STRUCT<`col1`: INT, `col2`: BOOLEAN>");

  VELOX_ASSERT_UNSUPPORTED_THROW(
      toSparkTypeSql(HUGEINT()), "Type is not supported in Spark SQL: HUGEINT");
  VELOX_ASSERT_UNSUPPORTED_THROW(
      toSparkTypeSql(ARRAY(INTERVAL_DAY_TIME())),
      "Type is not supported in Spark SQL: INTERVAL DAY TO SECOND");
  VELOX_ASSERT_UNSUPPORTED_THROW(
      toSparkTypeSql(UNKNOWN()), "Type is not supported in Spark SQL: UNKNOWN");
  VELOX_ASSERT_UNSUPPORTED_THROW(
      toSparkTypeSql(ROW({}, {})),
      "Spark does not support structs without fields");
}

TEST_F(SparkSqlTest, scalarConstants) {
  EXPECT_EQ(
      toSql(INTEGER(), Variant::null(TypeKind::INTEGER)), "CAST(NULL AS INT)");
  EXPECT_EQ(toSql(BOOLEAN(), Variant(true)), "true");
  EXPECT_EQ(toSql(TINYINT(), Variant(int8_t{5})), "5Y");
  EXPECT_EQ(toSql(TINYINT(), Variant(int8_t{-128})), "CAST(-128 AS TINYINT)");
  EXPECT_EQ(toSql(SMALLINT(), Variant(int16_t{7})), "7S");
  EXPECT_EQ(toSql(INTEGER(), Variant(int32_t{0})), "0");
  EXPECT_EQ(
      toSql(INTEGER(), Variant(std::numeric_limits<int32_t>::min())),
      "CAST(-2147483648 AS INT)");
  EXPECT_EQ(
      toSql(BIGINT(), Variant(std::numeric_limits<int64_t>::max())),
      "9223372036854775807L");
  EXPECT_EQ(
      toSql(BIGINT(), Variant(std::numeric_limits<int64_t>::min())),
      "CAST(-9223372036854775808 AS BIGINT)");
  EXPECT_EQ(toSql(REAL(), Variant(1.5f)), "CAST('1.5' AS FLOAT)");
  EXPECT_EQ(
      toSql(REAL(), Variant(std::numeric_limits<float>::quiet_NaN())),
      "CAST('NaN' AS FLOAT)");
  EXPECT_EQ(toSql(DOUBLE(), Variant(0.1)), "CAST('0.1' AS DOUBLE)");
  EXPECT_EQ(
      toSql(DOUBLE(), Variant(-std::numeric_limits<double>::infinity())),
      "CAST('-Infinity' AS DOUBLE)");
  EXPECT_EQ(
      toSql(DOUBLE(), Variant(std::numeric_limits<double>::infinity())),
      "CAST('Infinity' AS DOUBLE)");

  EXPECT_EQ(toSql(VARCHAR(), Variant("abc")), "'abc'");
  EXPECT_EQ(toSql(VARCHAR(), Variant("")), "''");
  EXPECT_EQ(toSql(VARCHAR(), Variant("it's a\\b")), "'it\\'s a\\\\b'");
  EXPECT_EQ(toSql(VARCHAR(), Variant("${x}")), "CAST(X'247B787D' AS STRING)");
  EXPECT_EQ(toSql(VARCHAR(), Variant("\u00e9")), "CAST(X'C3A9' AS STRING)");
  EXPECT_EQ(toSql(VARCHAR(), Variant("a\nb")), "CAST(X'610A62' AS STRING)");
  EXPECT_EQ(
      toSql(VARBINARY(), Variant::binary(std::string("\x01\xff", 2))),
      "X'01FF'");
  EXPECT_EQ(toSql(VARBINARY(), Variant::binary("")), "X''");

  EXPECT_EQ(
      toSql(TIMESTAMP(), Variant(Timestamp(1, 500'000))),
      "timestamp_micros(1000500L)");
  EXPECT_EQ(
      toSql(TIMESTAMP(), Variant(Timestamp(-1, 0))),
      "timestamp_micros(CAST(-1000000 AS BIGINT))");
  VELOX_ASSERT_UNSUPPORTED_THROW(
      toSql(TIMESTAMP(), Variant(Timestamp(0, 1))),
      "Spark timestamps have microsecond precision");
  VELOX_ASSERT_UNSUPPORTED_THROW(
      toSql(TIMESTAMP(), Variant(Timestamp(9'223'372'036'855, 0))),
      "Timestamp is out of Spark range");

  EXPECT_EQ(toSql(DATE(), Variant(18'000)), "date_from_unix_date(18000)");
  EXPECT_EQ(toSql(DATE(), Variant(-1)), "date_from_unix_date(CAST(-1 AS INT))");
  EXPECT_EQ(
      toSql(DECIMAL(5, 2), Variant(int64_t{12'345})),
      "CAST('123.45' AS DECIMAL(5, 2))");
  EXPECT_EQ(
      toSql(DECIMAL(20, 3), Variant(int128_t{-1})),
      "CAST('-0.001' AS DECIMAL(20, 3))");
  EXPECT_EQ(
      toSql(INTERVAL_YEAR_MONTH(), Variant(14)), "make_ym_interval(0, 14)");

  VELOX_ASSERT_UNSUPPORTED_THROW(
      toSql(INTERVAL_DAY_TIME(), Variant(int64_t{1})),
      "Type is not supported in Spark SQL");
  VELOX_ASSERT_UNSUPPORTED_THROW(
      toSql(HUGEINT(), Variant(int128_t{1})),
      "Type is not supported in Spark SQL");
}

TEST_F(SparkSqlTest, complexConstants) {
  auto arrays = makeNullableArrayVector<int32_t>(
      {{{1, std::nullopt}}, {{}}, std::nullopt});
  EXPECT_EQ(toSql(arrays, 0), "array(1, CAST(NULL AS INT))");
  EXPECT_EQ(toSql(arrays, 1), "CAST(array() AS ARRAY<INT>)");
  EXPECT_EQ(toSql(arrays, 2), "CAST(NULL AS ARRAY<INT>)");

  // Dictionary-encoded elements.
  auto elements = wrapInDictionary(
      makeIndicesInReverse(3), makeFlatVector<std::string>({"a", "b", "c"}));
  auto dictionaryArrays = makeArrayVector({0}, elements);
  EXPECT_EQ(toSql(dictionaryArrays), "array('c', 'b', 'a')");

  auto maps = makeMapVector(
      {0, 2},
      makeFlatVector<int32_t>({1, 2}),
      makeNullableFlatVector<std::string>({"a", std::nullopt}));
  EXPECT_EQ(toSql(maps, 0), "map(1, 'a', 2, CAST(NULL AS STRING))");
  EXPECT_EQ(toSql(maps, 1), "CAST(map() AS MAP<INT, STRING>)");

  auto duplicateKeys = makeMapVector<int32_t, int32_t>({{{1, 1}, {1, 2}}});
  VELOX_ASSERT_UNSUPPORTED_THROW(toSql(duplicateKeys), "Duplicate map key: 1");

  auto rows = makeRowVector(
      {"a", "b"},
      {makeFlatVector<int32_t>({1, 2}),
       makeNullableFlatVector<std::string>({"x", std::nullopt})});
  rows->setNull(1, true);
  EXPECT_EQ(toSql(rows, 0), "named_struct('a', 1, 'b', 'x')");
  EXPECT_EQ(toSql(rows, 1), "CAST(NULL AS STRUCT<`a`: INT, `b`: STRING>)");

  auto duplicateNames = makeRowVector(
      {"a", "a"}, {makeFlatVector<int32_t>({1}), makeFlatVector<int32_t>({2})});
  EXPECT_EQ(toSql(duplicateNames), "named_struct('col1', 1, 'col2', 2)");

  // Constants with complex values instead of vectors.
  EXPECT_EQ(
      toSql(
          ARRAY(BIGINT()),
          Variant::array({Variant(int64_t{1}), Variant(int64_t{2})})),
      "array(1L, 2L)");
  EXPECT_EQ(
      toSql(
          MAP(VARCHAR(), DOUBLE()),
          Variant::map({{Variant("k"), Variant(1.0)}})),
      "map('k', CAST('1' AS DOUBLE))");
  VELOX_ASSERT_UNSUPPORTED_THROW(
      toSql(
          MAP(VARCHAR(), DOUBLE()),
          Variant::map({{Variant::null(TypeKind::VARCHAR), Variant(1.0)}})),
      "Spark does not support null keys");
  EXPECT_EQ(
      toSql(
          ROW({"x", "y"}, {INTEGER(), ARRAY(DATE())}),
          Variant::row({Variant(1), Variant::array({Variant(0)})})),
      "named_struct('x', 1, 'y', array(date_from_unix_date(0)))");
}

TEST_F(SparkSqlTest, operators) {
  EXPECT_EQ(toSql("add(c0, c1)"), "(c0 + c1)");
  EXPECT_EQ(toSql("subtract(c0, 1)"), "(c0 - 1)");
  EXPECT_EQ(toSql("subtract(c0, -1)"), "(c0 - CAST(-1 AS INT))");
  EXPECT_EQ(toSql("multiply(c0, c1)"), "(c0 * c1)");
  EXPECT_EQ(toSql("remainder(c0, c1)"), "(c0 % c1)");
  EXPECT_EQ(toSql("div(c0, c1)"), "(c0 div c1)");
  EXPECT_EQ(toSql("equalto(c0, c1)"), "(c0 = c1)");
  EXPECT_EQ(toSql("lessthanorequal(c0, c1)"), "(c0 <= c1)");
  EXPECT_EQ(toSql("equalnullsafe(c0, c1)"), "(c0 <=> c1)");
  EXPECT_EQ(toSql("bitwise_xor(c0, c1)"), "(c0 ^ c1)");
  EXPECT_EQ(toSql("bitwise_not(c0)"), "(~ c0)");
  EXPECT_EQ(toSql("unaryminus(c0)"), "negative(c0)");
  EXPECT_EQ(toSql("b AND NOT b OR b"), "((b AND (NOT b)) OR b)");
  EXPECT_EQ(toSql("c0 BETWEEN 1 AND c1"), "(c0 BETWEEN 1 AND c1)");
  EXPECT_EQ(toSql("c0 IN (1, 2)"), "(c0 IN (1, 2))");
  EXPECT_EQ(toSql("\"a b\""), "`a b`");
}

TEST_F(SparkSqlTest, specialForms) {
  EXPECT_EQ(toSql("if(b, c0, c1)"), "IF(b, c0, c1)");
  EXPECT_EQ(
      toSparkSql(call(INTEGER(), "if", {field("b"), field("c0")})),
      "IF(b, c0, CAST(NULL AS INT))");
  EXPECT_EQ(
      toSql("CASE WHEN b THEN c0 WHEN NOT b THEN 1 ELSE c1 END"),
      "CASE WHEN b THEN c0 WHEN (NOT b) THEN 1 ELSE c1 END");
  EXPECT_EQ(toSql("coalesce(c0, c1, 2)"), "coalesce(c0, c1, 2)");
  EXPECT_EQ(toSql("cast(c0 as bigint)"), "CAST(c0 AS BIGINT)");
  EXPECT_EQ(toSql("try_cast(s as tinyint)"), "TRY_CAST(s AS TINYINT)");
  EXPECT_EQ(
      toSparkSql(
          std::make_shared<core::NullIfTypedExpr>(
              field("c0"), field("c1"), INTEGER())),
      "nullif(c0, c1)");
  EXPECT_EQ(
      toSparkSql(
          std::make_shared<core::ConcatTypedExpr>(
              std::vector<std::string>{"a", "a"},
              std::vector<core::TypedExprPtr>{field("c0"), field("s")})),
      "named_struct('col1', c0, 'col2', s)");

  // Dereferences.
  EXPECT_EQ(
      toSparkSql(
          std::make_shared<core::FieldAccessTypedExpr>(
              INTEGER(), field("r"), "X")),
      "(r).`col2`");
  EXPECT_EQ(
      toSparkSql(
          std::make_shared<core::DereferenceTypedExpr>(
              INTEGER(), field("dup"), 1)),
      "(dup).`col2`");
  VELOX_ASSERT_UNSUPPORTED_THROW(
      toSparkSql(
          std::make_shared<core::FieldAccessTypedExpr>(
              INTEGER(), field("dup"), "x")),
      "Struct field name is ambiguous: x");

  // Lambdas.
  EXPECT_EQ(
      toSql("transform(arr, x -> add(x, c0))"),
      "transform(arr, x -> (x + c0))");
  EXPECT_EQ(
      toSql("zip_with(arr, arr, (x, y) -> multiply(x, y))"),
      "zip_with(arr, arr, (x, y) -> (x * y))");
}

TEST_F(SparkSqlTest, functions) {
  EXPECT_EQ(toSql("like(s, 'a%')"), "(s LIKE 'a%')");
  EXPECT_EQ(toSql("like(s, 'a#%', '#')"), "(s LIKE 'a#%' ESCAPE '#')");
  EXPECT_EQ(toSql("like(s, 'a%', '\\')"), "(s LIKE 'a%' ESCAPE '\\\\')");
  VELOX_ASSERT_UNSUPPORTED_THROW(
      toSparkSql(call(
          BOOLEAN(),
          "like",
          {field("s"), constant(VARCHAR(), "a"), field("s")})),
      "Spark requires a constant escape character");

  EXPECT_EQ(toSql("size(arr, true)"), "size(arr)");
  EXPECT_EQ(
      toSql("size(arr, false)"),
      "IF(arr IS NULL, CAST(NULL AS INT), size(arr))");
  EXPECT_EQ(
      toSql("array_insert(arr, 1, c0, false)"), "array_insert(arr, 1, c0)");
  VELOX_ASSERT_UNSUPPORTED_THROW(
      toSql("array_insert(arr, 1, c0, true)"),
      "Spark SQL only supports array_insert with legacyNegativeIndex false");

  EXPECT_EQ(toSql("ltrim(s)"), "ltrim(s)");
  EXPECT_EQ(toSql("trim('x', s)"), "TRIM(BOTH 'x' FROM s)");
  EXPECT_EQ(toSql("btrim('x', s)"), "TRIM(BOTH 'x' FROM s)");
  EXPECT_EQ(toSql("ltrim('x', s)"), "TRIM(LEADING 'x' FROM s)");
  EXPECT_EQ(toSql("rtrim(s, s)"), "TRIM(TRAILING s FROM s)");

  EXPECT_EQ(toSql("timestampadd('day', c0, ts)"), "timestampadd(DAY, c0, ts)");
  EXPECT_EQ(
      toSql("timestampdiff('Microsecond', ts, ts)"),
      "timestampdiff(MICROSECOND, ts, ts)");
  VELOX_ASSERT_UNSUPPORTED_THROW(
      toSql("timestampadd('fortnight', c0, ts)"),
      "Unit is not supported by Spark: FORTNIGHT");

  EXPECT_EQ(
      toSql("year_of_week(cast(ts as date))"),
      "extract(YEAROFWEEK FROM CAST(ts AS DATE))");
  EXPECT_EQ(toSql("get_timestamp(s, 'yyyy')"), "to_timestamp(s, 'yyyy')");
  EXPECT_EQ(
      toSql("array_sort(arr, (x, y) -> if(lessthan(x, y), 1, 0))"),
      "array_sort(arr, (x, y) -> IF((x < y), 1, 0))");
  VELOX_ASSERT_UNSUPPORTED_THROW(
      toSql("array_sort(arr, x -> add(x, 1))"),
      "Spark does not support array_sort with a key function");

  for (const auto& name :
       {"rand", "uuid", "checked_add", "sum_extract_bigint", "might_contain"}) {
    VELOX_ASSERT_UNSUPPORTED_THROW(
        toSparkSql(call(BIGINT(), name, {})),
        fmt::format("Function is not supported in Spark SQL: {}", name));
  }
  EXPECT_EQ(
      toSparkSql(call(VARCHAR(), "regexp_extract", {field("s"), field("s")})),
      "regexp_extract(s, s)");
}

TEST_F(SparkSqlTest, aggregates) {
  auto data = makeRowVector(
      {"c0", "c1", "m"},
      {makeFlatVector<int32_t>({}),
       makeFlatVector<int32_t>({}),
       makeFlatVector<bool>({})});
  const auto plan = exec::test::PlanBuilder()
                        .values({data})
                        .singleAggregation(
                            {"c1"},
                            {"sum(c0)",
                             "sum(distinct c0)",
                             "first_ignore_null(c0)",
                             "last(c0)",
                             "collect_list(c0)"},
                            {"", "", "m", "", ""})
                        .planNode();
  const auto& aggregates = node<core::AggregationNode>(plan).aggregates();
  std::vector<std::string> sqls;
  for (const auto& aggregate : aggregates) {
    sqls.push_back(toSparkAggregateSql(aggregate));
  }
  EXPECT_EQ(
      sqls,
      (std::vector<std::string>{
          "sum(c0)",
          "sum(DISTINCT c0)",
          "first(c0, true) FILTER (WHERE m)",
          "last(c0)",
          "collect_list(c0)"}));

  auto aggregate = aggregates[0];
  aggregate.sortingKeys = {
      std::make_shared<core::FieldAccessTypedExpr>(INTEGER(), "c1")};
  aggregate.sortingOrders = {core::kAscNullsLast};
  VELOX_ASSERT_UNSUPPORTED_THROW(
      toSparkAggregateSql(aggregate),
      "Spark does not support aggregate functions with sorting keys");

  aggregate = aggregates[0];
  aggregate.call = std::make_shared<core::CallTypedExpr>(
      BIGINT(), std::vector<core::TypedExprPtr>{field("c0")}, "sum_partial");
  VELOX_ASSERT_UNSUPPORTED_THROW(
      toSparkAggregateSql(aggregate),
      "Aggregate function is not supported in Spark SQL: sum_partial");
}

TEST_F(SparkSqlTest, windows) {
  auto data = makeRowVector(
      {"c0", "c1"}, {makeFlatVector<int32_t>({}), makeFlatVector<int32_t>({})});
  parse::ParseOptions options;
  options.parseIntegerAsBigint = false;
  const auto plan =
      exec::test::PlanBuilder()
          .setParseOptions(options)
          .values({data})
          .window(
              {"row_number() over (partition by c0 order by c1)",
               "sum(c1) over (partition by c0 order by c1 "
               "rows between 2 preceding and current row)",
               "max(c1) over (partition by c0 order by c1 "
               "range between unbounded preceding and current row)",
               "nth_value(c1, 2 IGNORE NULLS) over (partition by c0 order by c1 "
               "rows between current row and 3 following)",
               // DuckDB parses this frame as a RANGE frame.
               "first_ignore_null(c1) over (partition by c0 order by c1 "
               "rows between unbounded preceding and unbounded following)"})
          .planNode();
  const auto& functions = node<core::WindowNode>(plan).windowFunctions();
  std::vector<std::string> sqls;
  for (const auto& function : functions) {
    auto frame = toSparkWindowFrameSql(function);
    sqls.push_back(
        toSparkWindowFunctionSql(function) + (frame.empty() ? "" : " ") +
        frame);
  }
  EXPECT_EQ(
      sqls,
      (std::vector<std::string>{
          "row_number()",
          "sum(c1) ROWS BETWEEN 2 PRECEDING AND CURRENT ROW",
          "max(c1) RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW",
          "nth_value(c1, 2) IGNORE NULLS ROWS BETWEEN CURRENT ROW AND 3 FOLLOWING",
          "first(c1, true) RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING"}));

  auto function = functions[1];
  function.frame.type = core::WindowNode::WindowType::kRange;
  function.frame.startValue = field("c1");
  VELOX_ASSERT_UNSUPPORTED_THROW(
      toSparkWindowFrameSql(function),
      "Spark SQL generation does not support k RANGE frames");

  function = functions[1];
  function.frame.startValue = field("c1");
  VELOX_ASSERT_UNSUPPORTED_THROW(
      toSparkWindowFrameSql(function),
      "Spark only supports constant frame offsets");

  function = functions[1];
  function.frame.startValue = constant(BIGINT(), int64_t{1} << 40);
  VELOX_ASSERT_UNSUPPORTED_THROW(
      toSparkWindowFrameSql(function), "Frame offset is out of range");

  function = functions[1];
  function.ignoreNulls = true;
  VELOX_ASSERT_UNSUPPORTED_THROW(
      toSparkWindowFunctionSql(function),
      "Spark does not support IGNORE NULLS for sum");
}

} // namespace
} // namespace facebook::velox::functions::sparksql::fuzzer::test
