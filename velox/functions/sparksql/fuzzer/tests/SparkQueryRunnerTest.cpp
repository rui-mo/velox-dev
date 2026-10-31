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

#include <numeric>

#include <folly/init/Init.h>
#include <google/protobuf/unknown_field_set.h>
#include <gtest/gtest.h>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/dwio/parquet/RegisterParquetWriter.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/Macros.h"
#include "velox/functions/Registerer.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/sparksql/aggregates/Register.h"
#include "velox/functions/sparksql/fuzzer/SparkQueryRunner.h"
#include "velox/functions/sparksql/registration/Register.h"
#include "velox/functions/sparksql/window/WindowFunctionsRegistration.h"
#include "velox/parse/ExpressionsParser.h"
#include "velox/parse/TypeResolver.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook;
using namespace facebook::velox;
using namespace facebook::velox::test;

namespace facebook::velox::functions::sparksql::test {
namespace {

using exec::test::ReferenceQueryErrorCode;

// A function that does not exist in Spark.
template <typename T>
struct VeloxOnlyFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  void call(int64_t& result, const int64_t& input) {
    result = input;
  }
};

std::string serialize(const google::protobuf::UnknownFieldSet& fields) {
  std::string serialized;
  VELOX_CHECK(fields.SerializeToString(&serialized));
  return serialized;
}

// Returns a gRPC status like the ones of Spark Connect errors. The details are
// a google.rpc.Status with a google.rpc.ErrorInfo that has the Java exception
// class as reason, and the exception class hierarchy, condition and SQLSTATE
// in its metadata. Empty metadata values are not added.
grpc::Status makeSparkError(
    const std::string& message,
    const std::string& sqlState,
    const std::string& exceptionClass = "org.apache.spark.SparkException",
    const std::string& exceptionClasses = "",
    const std::string& condition = "") {
  google::protobuf::UnknownFieldSet errorInfo;
  errorInfo.AddLengthDelimited(1, exceptionClass);
  errorInfo.AddLengthDelimited(2, "org.apache.spark");
  for (const auto& [key, value] :
       std::vector<std::pair<std::string, std::string>>{
           {"classes", exceptionClasses},
           {"errorClass", condition},
           {"sqlState", sqlState}}) {
    if (value.empty()) {
      continue;
    }
    google::protobuf::UnknownFieldSet metadataEntry;
    metadataEntry.AddLengthDelimited(1, key);
    metadataEntry.AddLengthDelimited(2, value);
    errorInfo.AddLengthDelimited(3, serialize(metadataEntry));
  }
  google::protobuf::UnknownFieldSet any;
  any.AddLengthDelimited(1, "type.googleapis.com/google.rpc.ErrorInfo");
  any.AddLengthDelimited(2, serialize(errorInfo));
  google::protobuf::UnknownFieldSet rpcStatus;
  rpcStatus.AddVarint(1, grpc::StatusCode::INTERNAL);
  rpcStatus.AddLengthDelimited(2, message);
  rpcStatus.AddLengthDelimited(3, serialize(any));
  return grpc::Status(
      grpc::StatusCode::INTERNAL, message, serialize(rpcStatus));
}

class SparkQueryRunnerTest : public ::testing::Test,
                             public velox::test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
    parquet::registerParquetWriterFactory();
  }

  void SetUp() override {
    velox::functions::sparksql::registerFunctions("");
    velox::aggregate::prestosql::registerAllAggregateFunctions();
    velox::functions::aggregate::sparksql::registerAggregateFunctions("");
    velox::functions::window::sparksql::registerWindowFunctions("");
    velox::parse::registerTypeResolver();
    registerFunction<VeloxOnlyFunction, int64_t, int64_t>(
        {"velox_only_function"});
  }
};

// This test requires a Spark coordinator running at localhost, so disable it
// by default.
TEST_F(SparkQueryRunnerTest, DISABLED_basic) {
  auto aggregatePool = rootPool_->addAggregateChild("basic");
  auto queryRunner = std::make_unique<fuzzer::SparkQueryRunner>(
      aggregatePool.get(), "localhost:15002", "test", "basic");

  auto input = makeRowVector({
      makeConstant<int64_t>(1, 25),
  });
  auto plan = exec::test::PlanBuilder()
                  .values({input})
                  .singleAggregation({}, {"count(c0)"})
                  .planNode();
  auto sparkResults = queryRunner->executeAndReturnVector(plan);
  auto expected = makeRowVector({
      makeConstant<int64_t>(25, 1),
  });
  ASSERT_EQ(sparkResults.second, exec::test::ReferenceQueryErrorCode::kSuccess);
  exec::test::assertEqualResults(sparkResults.first.value(), {expected});

  input = makeRowVector({
      makeFlatVector<int64_t>({0, 1, 2, 3, 4, 0, 1, 2, 3, 4, 0, 1, 2,
                               3, 4, 0, 1, 2, 3, 4, 0, 1, 2, 3, 4}),
      makeFlatVector<int64_t>({1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1,
                               1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}),
  });
  plan = exec::test::PlanBuilder()
             .values({input})
             .singleAggregation({"c0"}, {"count(c1)"})
             .planNode();
  sparkResults = queryRunner->executeAndReturnVector(plan);
  expected = makeRowVector({
      makeFlatVector<int64_t>({0, 1, 2, 3, 4}),
      makeFlatVector<int64_t>({5, 5, 5, 5, 5}),
  });
  ASSERT_EQ(sparkResults.second, exec::test::ReferenceQueryErrorCode::kSuccess);
  exec::test::assertEqualResults(sparkResults.first.value(), {expected});

  // The temporary views of the input tables are dropped after the queries.
  const auto views = queryRunner->execute("SHOW VIEWS");
  const auto numViews = std::accumulate(
      views.begin(), views.end(), 0, [](auto sum, const auto& batch) {
        return sum + batch->size();
      });
  EXPECT_EQ(numViews, 0);
}

// This test requires a Spark coordinator running at localhost, so disable it
// by default.
TEST_F(SparkQueryRunnerTest, DISABLED_decimal) {
  auto aggregatePool = rootPool_->addAggregateChild("decimal");
  auto queryRunner = std::make_unique<fuzzer::SparkQueryRunner>(
      aggregatePool.get(), "localhost:15002", "test", "decimal");
  auto input = makeRowVector({
      makeConstant<int128_t>(123456789, 25, DECIMAL(34, 2)),
  });
  auto plan =
      exec::test::PlanBuilder().values({input}).project({"abs(c0)"}).planNode();
  auto sparkResults = queryRunner->executeAndReturnVector(plan);
  ASSERT_EQ(sparkResults.second, exec::test::ReferenceQueryErrorCode::kSuccess);
  exec::test::assertEqualResults(sparkResults.first.value(), {input});
}

// This test requires a Spark coordinator running at localhost, so disable it
// by default.
TEST_F(SparkQueryRunnerTest, DISABLED_fuzzer) {
  auto data = makeRowVector({
      makeFlatVector<int64_t>({1, 2, 3, 4, 5}),
      makeNullableFlatVector<int64_t>({std::nullopt, 1, 2, std::nullopt, 4, 5}),
  });

  auto plan = exec::test::PlanBuilder()
                  .values({data})
                  .singleAggregation({}, {"sum(c0)", "collect_list(c1)"})
                  .project({"a0", "array_sort(a1)"})
                  .planNode();

  auto aggregatePool = rootPool_->addAggregateChild("fuzzer");
  auto queryRunner = std::make_unique<fuzzer::SparkQueryRunner>(
      aggregatePool.get(), "localhost:15002", "test", "fuzzer");
  auto sql = queryRunner->toSql(plan);
  ASSERT_TRUE(sql.has_value());

  auto sparkResults = queryRunner->executeAndReturnVector(plan);
  auto veloxResults = exec::test::AssertQueryBuilder(plan).copyResults(pool());
  exec::test::assertEqualResults(sparkResults.first.value(), {veloxResults});
}

// This test requires a Spark coordinator running at localhost, so disable it
// by default.
TEST_F(SparkQueryRunnerTest, DISABLED_errors) {
  auto aggregatePool = rootPool_->addAggregateChild("errors");
  auto queryRunner = std::make_unique<fuzzer::SparkQueryRunner>(
      aggregatePool.get(),
      "localhost:15002",
      "test",
      "errors",
      std::chrono::seconds(5));
  auto input = makeRowVector({
      makeFlatVector<int64_t>({1, 2, 3}),
      makeFlatVector<std::string>({"a", "b", "c"}),
  });

  // Analysis error.
  auto plan = exec::test::PlanBuilder()
                  .values({input})
                  .project({"velox_only_function(c0)"})
                  .planNode();
  auto result = queryRunner->executeAndReturnVector(plan);
  EXPECT_EQ(result.second, ReferenceQueryErrorCode::kReferenceQueryUnsupported);
  EXPECT_FALSE(result.first.has_value());

  // Runtime error.
  plan = exec::test::PlanBuilder()
             .values({input})
             .project({"raise_error(c1)"})
             .planNode();
  result = queryRunner->executeAndReturnVector(plan);
  EXPECT_EQ(result.second, ReferenceQueryErrorCode::kReferenceQueryFail);
  EXPECT_FALSE(result.first.has_value());

  // View creation error. Spark column names are case insensitive.
  auto duplicateColumns = makeRowVector(
      {"a", "A"}, {makeFlatVector<int64_t>({1}), makeFlatVector<int64_t>({2})});
  plan = exec::test::PlanBuilder()
             .values({duplicateColumns})
             .project({"a"})
             .planNode();
  result = queryRunner->executeAndReturnVector(plan);
  EXPECT_EQ(result.second, ReferenceQueryErrorCode::kReferenceQueryUnsupported);
  EXPECT_FALSE(result.first.has_value());

  VELOX_ASSERT_THROW(
      queryRunner->execute("SELEC 1"),
      "[PARSE_SYNTAX_ERROR] Syntax error at or near 'SELEC'");

  VELOX_ASSERT_THROW(
      queryRunner->execute(
          "SELECT reflect('java.lang.Thread', 'sleep', 60000L)"),
      "Query timed out after 5 seconds");

  // The session can run queries after an error.
  auto results = queryRunner->execute("SELECT 1");
  exec::test::assertEqualResults(
      results, {makeRowVector({makeFlatVector<int32_t>({1})})});
}

// This test requires a Spark coordinator running at localhost, so disable it
// by default.
TEST_F(SparkQueryRunnerTest, DISABLED_timestamp) {
  auto aggregatePool = rootPool_->addAggregateChild("timestamp");
  auto queryRunner = std::make_unique<fuzzer::SparkQueryRunner>(
      aggregatePool.get(), "localhost:15002", "test", "timestamp");
  auto input = makeRowVector({
      makeNullableFlatVector<Timestamp>(
          {Timestamp(0, 0),
           Timestamp(1'700'000'000, 123'456'000),
           Timestamp(-2'000'000'000, 0),
           std::nullopt}),
      makeNullableFlatVector<int32_t>(
          {0, 19'000, -20'000, std::nullopt}, DATE()),
  });

  auto plan = exec::test::PlanBuilder()
                  .values({input})
                  .project({"c0", "c1", "hour(c0)"})
                  .planNode();
  auto result = queryRunner->executeAndReturnVector(plan);
  ASSERT_EQ(result.second, ReferenceQueryErrorCode::kSuccess);
  // Spark reads the timestamps as TIMESTAMP, not TIMESTAMP_NTZ, so hour() is
  // in the America/Los_Angeles session time zone.
  auto expected = makeRowVector({
      input->childAt(0),
      input->childAt(1),
      makeNullableFlatVector<int32_t>({16, 14, 12, std::nullopt}),
  });
  exec::test::assertEqualResults(result.first.value(), {expected});
}

// This test requires a Spark coordinator running at localhost, so disable it
// by default.
TEST_F(SparkQueryRunnerTest, DISABLED_nullRows) {
  auto aggregatePool = rootPool_->addAggregateChild("nullRows");
  auto queryRunner = std::make_unique<fuzzer::SparkQueryRunner>(
      aggregatePool.get(), "localhost:15002", "test", "nullRows");
  // The child of the null row cannot be converted to microseconds.
  auto row = makeRowVector(
      {makeFlatVector<Timestamp>({Timestamp::max(), Timestamp(0, 0)})});
  row->setNull(0, true);
  auto input = makeRowVector({row});

  auto plan =
      exec::test::PlanBuilder().values({input}).project({"c0"}).planNode();
  auto result = queryRunner->executeAndReturnVector(plan);
  ASSERT_EQ(result.second, ReferenceQueryErrorCode::kSuccess);
  exec::test::assertEqualResults(result.first.value(), {input});
}

// Verifies that with the projection session configs, Spark doesn't fail the
// query when a constant argument throws on rows that default null behavior
// skips, as in Velox. This test requires a Spark coordinator running at
// localhost, so disable it by default.
TEST_F(SparkQueryRunnerTest, DISABLED_constantFolding) {
  auto aggregatePool = rootPool_->addAggregateChild("constantFolding");
  auto queryRunner = std::make_unique<fuzzer::SparkQueryRunner>(
      aggregatePool.get(),
      "localhost:15002",
      "test",
      "constantFolding",
      fuzzer::SparkQueryRunner::kDefaultQueryTimeout,
      fuzzer::SparkQueryRunner::projectionSessionConfigs());
  const auto mapType = MAP(VARCHAR(), BIGINT());
  auto input = makeRowVector({
      makeNullableMapVector<std::string, int64_t>({std::nullopt, std::nullopt}),
  });

  // map_from_entries throws on the null key. Velox skips it since 'c0' is null
  // in all rows.
  const auto entries = makeArrayOfRowVector(
      ROW({VARCHAR(), BIGINT()}),
      {{Variant::row({Variant::null(TypeKind::VARCHAR), 1LL})}});
  const auto mapFromEntries = std::make_shared<core::CallTypedExpr>(
      mapType,
      "map_from_entries",
      std::make_shared<core::ConstantTypedExpr>(
          BaseVector::wrapInConstant(1, 0, entries)));
  auto plan =
      exec::test::PlanBuilder(pool())
          .values({input})
          .projectExpressions({std::make_shared<core::CallTypedExpr>(
              mapType,
              "map_concat",
              std::make_shared<core::FieldAccessTypedExpr>(mapType, "c0"),
              mapFromEntries)})
          .planNode();
  auto result = queryRunner->executeAndReturnVector(plan);
  ASSERT_EQ(result.second, ReferenceQueryErrorCode::kSuccess);
  auto veloxResults = exec::test::AssertQueryBuilder(plan).copyResults(pool());
  exec::test::assertEqualResults(result.first.value(), {veloxResults});
}

// Verifies that with the projection session configs, Spark doesn't fail the
// query when a common subexpression throws on rows that default null behavior
// skips, as in Velox. This test requires a Spark coordinator running at
// localhost, so disable it by default.
TEST_F(SparkQueryRunnerTest, DISABLED_subexpressionElimination) {
  auto aggregatePool = rootPool_->addAggregateChild("subexpressionElimination");
  auto queryRunner = std::make_unique<fuzzer::SparkQueryRunner>(
      aggregatePool.get(),
      "localhost:15002",
      "test",
      "subexpressionElimination",
      fuzzer::SparkQueryRunner::kDefaultQueryTimeout,
      fuzzer::SparkQueryRunner::projectionSessionConfigs());
  auto input = makeRowVector({
      makeFlatVector<int32_t>({0, 100}),
      makeNullableMapVector<int8_t, int64_t>({{{{2, 10}}}, std::nullopt}),
  });

  // bit_get throws on position 100. Velox skips it since 'c1' is null in that
  // row.
  auto plan =
      exec::test::PlanBuilder()
          .values({input})
          .project({"element_at(c1, add(bit_get(1, c0), bit_get(1, c0)))"})
          .planNode();
  auto result = queryRunner->executeAndReturnVector(plan);
  ASSERT_EQ(result.second, ReferenceQueryErrorCode::kSuccess);
  auto expected = makeRowVector({
      makeNullableFlatVector<int64_t>({10, std::nullopt}),
  });
  exec::test::assertEqualResults(result.first.value(), {expected});
  exec::test::assertEqualResults(
      {exec::test::AssertQueryBuilder(plan).copyResults(pool())}, {expected});
}

// Verifies that with the projection session configs, Spark doesn't fold
// constants through try_multiply, which returns null on overflow, as in Velox.
// This test requires a Spark coordinator running at localhost, so disable it by
// default.
TEST_F(SparkQueryRunnerTest, DISABLED_reorderAssociativeOperator) {
  auto aggregatePool =
      rootPool_->addAggregateChild("reorderAssociativeOperator");
  auto queryRunner = std::make_unique<fuzzer::SparkQueryRunner>(
      aggregatePool.get(),
      "localhost:15002",
      "test",
      "reorderAssociativeOperator",
      fuzzer::SparkQueryRunner::kDefaultQueryTimeout,
      fuzzer::SparkQueryRunner::projectionSessionConfigs());
  auto input = makeRowVector({makeFlatVector<int16_t>({2, 23231})});

  // 13593 * try_multiply(15837, c0). 15837 * 23231 overflows SMALLINT.
  const auto smallintLiteral = [](int16_t value) {
    return std::make_shared<core::ConstantTypedExpr>(SMALLINT(), value);
  };
  auto plan = exec::test::PlanBuilder(pool())
                  .values({input})
                  .projectExpressions({std::make_shared<core::CallTypedExpr>(
                      SMALLINT(),
                      "multiply",
                      smallintLiteral(13593),
                      std::make_shared<core::CallTypedExpr>(
                          SMALLINT(),
                          "try_multiply",
                          smallintLiteral(15837),
                          std::make_shared<core::FieldAccessTypedExpr>(
                              SMALLINT(), "c0")))})
                  .planNode();
  auto result = queryRunner->executeAndReturnVector(plan);
  ASSERT_EQ(result.second, ReferenceQueryErrorCode::kSuccess);
  auto expected = makeRowVector({
      makeNullableFlatVector<int16_t>({-26838, std::nullopt}),
  });
  exec::test::assertEqualResults(result.first.value(), {expected});
  exec::test::assertEqualResults(
      {exec::test::AssertQueryBuilder(plan).copyResults(pool())}, {expected});
}

// Verifies that Spark accepts the SQL generated for the expressions and returns
// the same results as Velox. This test requires a Spark coordinator running at
// localhost, so disable it by default.
TEST_F(SparkQueryRunnerTest, DISABLED_expressions) {
  auto aggregatePool = rootPool_->addAggregateChild("expressions");
  auto queryRunner = std::make_unique<fuzzer::SparkQueryRunner>(
      aggregatePool.get(), "localhost:15002", "test", "expressions");

  // Spark cannot read struct fields with empty or case-insensitively duplicate
  // names, and reads intervals as integers from Parquet.
  auto input = makeRowVector(
      {"id", "c0", "c1", "b", "s", "arr", "r", "e", "iv", "dec", "a b"},
      {makeFlatVector<int64_t>({0, 1, 2, 3, 4}),
       makeNullableFlatVector<int32_t>(
           {1, -2, std::nullopt, 4, std::numeric_limits<int32_t>::max()}),
       makeNullableFlatVector<int32_t>({3, 0, 5, std::nullopt, 1}),
       makeNullableFlatVector<bool>({true, false, std::nullopt, true, false}),
       makeNullableFlatVector<std::string>(
           {"abc", "a%c", std::nullopt, "x\u00e9x", " a "}),
       makeNullableArrayVector<int32_t>(
           {{{1, 2}}, {{}}, std::nullopt, {{3, std::nullopt}}, {{5}}}),
       makeRowVector(
           {"x", "X"},
           {makeFlatVector<int32_t>({1, 2, 3, 4, 5}),
            makeFlatVector<int32_t>({6, 7, 8, 9, 10})}),
       makeRowVector(
           {"", ""},
           {makeFlatVector<int32_t>({1, 2, 3, 4, 5}),
            makeFlatVector<std::string>({"a", "b", "c", "d", "e"})}),
       makeNullableFlatVector<int32_t>(
           {0, 14, std::nullopt, -3, 1'000}, INTERVAL_YEAR_MONTH()),
       makeNullableFlatVector<int64_t>(
           {12'345, -1, std::nullopt, 0, 99'999}, DECIMAL(10, 2)),
       makeFlatVector<int64_t>({10, 20, 30, 40, 50})});

  parse::ParseOptions options;
  options.parseDecimalAsDouble = false;
  options.parseIntegerAsBigint = false;
  auto verify = [&](const core::PlanNodePtr& plan) {
    SCOPED_TRACE(queryRunner->toSql(plan).value_or("<unsupported>"));
    auto sparkResults = queryRunner->executeAndReturnVector(plan);
    ASSERT_EQ(sparkResults.second, ReferenceQueryErrorCode::kSuccess);
    auto veloxResults =
        exec::test::AssertQueryBuilder(plan).copyResults(pool());
    exec::test::assertEqualResults(sparkResults.first.value(), {veloxResults});
  };

  const std::vector<std::string> expressions = {
      "add(c0, c1)",
      "subtract(c0, -1)",
      "c0 IN (1, 4)",
      "if(b, c0, c1)",
      "CASE WHEN b THEN s WHEN NOT b THEN 'it''s' END",
      "coalesce(c0, c1, 2)",
      "like(s, 'a#%c', '#')",
      "like(s, 'a%')",
      "size(arr, true)",
      "size(arr, false)",
      "array_insert(arr, 1, c0, false)",
      "trim('a', s)",
      "ltrim(' ', s)",
      "transform(arr, x -> add(x, c0))",
      "zip_with(arr, arr, (x, y) -> multiply(x, y))",
      "\"a b\"",
      "e",
      "iv",
      "dec",
      "add(dec, 1.5)",
      "cast(c0 as varchar)",
      "try_cast(s as integer)",
      "cast(null as integer)",
      "concat(s, '\u00e9${x}')",
      "array_contains(arr, -3)",
      // Duplicate map keys.
      "str_to_map(concat(s, ',', s), ',', ':')",
  };
  for (const auto& expression : expressions) {
    verify(
        exec::test::PlanBuilder(pool())
            .setParseOptions(options)
            .values({input})
            .project({expression})
            .planNode());
  }

  const auto rowType = asRowType(input->type());
  const auto r = std::make_shared<core::FieldAccessTypedExpr>(
      rowType->findChild("r"), "r");
  verify(
      exec::test::PlanBuilder(pool())
          .values({input})
          .projectExpressions(
              {std::make_shared<core::FieldAccessTypedExpr>(INTEGER(), r, "X"),
               std::make_shared<core::DereferenceTypedExpr>(INTEGER(), r, 0),
               std::make_shared<core::ConstantTypedExpr>(
                   VARBINARY(), Variant::binary(""))})
          .planNode());

  verify(
      exec::test::PlanBuilder(pool())
          .values({input})
          .singleAggregation(
              {"b"},
              {"sum(c0)", "count(c1)", "max(s)", "sum(distinct c1)", "count()"},
              {"", "b", "", "", ""})
          .planNode());

  verify(
      exec::test::PlanBuilder(pool())
          .setParseOptions(options)
          .values({input})
          .window(
              {"row_number() over (partition by b order by id)",
               "sum(c0) over (partition by b order by id "
               "rows between 1 preceding and current row)",
               "nth_value(c0, 2) over (partition by b order by id "
               "rows between unbounded preceding and unbounded following)",
               "first_ignore_null(c1) over (partition by b order by id "
               "rows between unbounded preceding and current row)"})
          .planNode());
}

TEST_F(SparkQueryRunnerTest, classifyError) {
  using fuzzer::SparkQueryRunner;
  // UNRESOLVED_ROUTINE.
  EXPECT_EQ(
      SparkQueryRunner::classifyError(
          makeSparkError("[UNRESOLVED_ROUTINE] ... SQLSTATE: 42883", "42883")),
      ReferenceQueryErrorCode::kReferenceQueryUnsupported);
  // The message is truncated before the SQLSTATE.
  EXPECT_EQ(
      SparkQueryRunner::classifyError(makeSparkError(
          "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] ...", "42K09")),
      ReferenceQueryErrorCode::kReferenceQueryUnsupported);
  EXPECT_EQ(
      SparkQueryRunner::classifyError(
          makeSparkError("[UNSUPPORTED_FEATURE] ...", "0A000")),
      ReferenceQueryErrorCode::kReferenceQueryUnsupported);
  EXPECT_EQ(
      SparkQueryRunner::classifyError(
          makeSparkError("[COLLECTION_SIZE_LIMIT_EXCEEDED] ...", "54000")),
      ReferenceQueryErrorCode::kReferenceQueryUnsupported);
  EXPECT_EQ(
      SparkQueryRunner::classifyError(
          makeSparkError("[INTERNAL_ERROR] ...", "XX000")),
      ReferenceQueryErrorCode::kReferenceQueryUnsupported);
  EXPECT_EQ(
      SparkQueryRunner::classifyError(
          makeSparkError("[DIVIDE_BY_ZERO] ...", "22012")),
      ReferenceQueryErrorCode::kReferenceQueryFail);
  EXPECT_EQ(
      SparkQueryRunner::classifyError(
          makeSparkError("[USER_RAISED_EXCEPTION] ...", "P0001")),
      ReferenceQueryErrorCode::kReferenceQueryFail);

  // Statuses without details.
  EXPECT_EQ(
      SparkQueryRunner::classifyError(
          grpc::Status(
              grpc::StatusCode::INTERNAL,
              "[UNRESOLVED_ROUTINE] ... SQLSTATE: 42883")),
      ReferenceQueryErrorCode::kReferenceQueryUnsupported);
  EXPECT_EQ(
      SparkQueryRunner::classifyError(
          grpc::Status(
              grpc::StatusCode::INTERNAL,
              "[DIVIDE_BY_ZERO] ... SQLSTATE: 22012")),
      ReferenceQueryErrorCode::kReferenceQueryFail);
  EXPECT_EQ(
      SparkQueryRunner::classifyError(
          grpc::Status(
              grpc::StatusCode::INTERNAL, "java.lang.ArithmeticException")),
      ReferenceQueryErrorCode::kReferenceQueryFail);

  // Legacy analysis errors have no SQLSTATE.
  EXPECT_EQ(
      SparkQueryRunner::classifyError(makeSparkError(
          "Window function row_number() requires window to be ordered, ...",
          "",
          "org.apache.spark.sql.catalyst.ExtendedAnalysisException",
          R"(["org.apache.spark.sql.catalyst.ExtendedAnalysisException",)"
          R"("org.apache.spark.sql.AnalysisException","java.lang.Exception"])",
          "_LEGACY_ERROR_TEMP_1037")),
      ReferenceQueryErrorCode::kReferenceQueryUnsupported);
  // Errors other than PARSE_SYNTAX_ERROR also have SQLSTATE 42601.
  EXPECT_EQ(
      SparkQueryRunner::classifyError(makeSparkError(
          "[INVALID_FORMAT.UNEXPECTED_TOKEN] The format is invalid: 'X9'. ...",
          "42601",
          "org.apache.spark.sql.AnalysisException",
          "",
          "INVALID_FORMAT.UNEXPECTED_TOKEN")),
      ReferenceQueryErrorCode::kReferenceQueryUnsupported);
  // A Spark executor ran out of memory.
  EXPECT_EQ(
      SparkQueryRunner::classifyError(makeSparkError(
          "Job aborted due to stage failure: Task 3 in stage 0.0 failed 1 "
          "times, most recent failure: Lost task 3.0 in stage 0.0 (TID 3) "
          "(executor 0): java.lang.OutOfMemoryError: Java heap space",
          "")),
      ReferenceQueryErrorCode::kReferenceQueryUnsupported);
  // The Spark driver ran out of memory.
  EXPECT_EQ(
      SparkQueryRunner::classifyError(
          grpc::Status(grpc::StatusCode::UNKNOWN, "Java heap space")),
      ReferenceQueryErrorCode::kReferenceQueryUnsupported);

  VELOX_ASSERT_THROW(
      SparkQueryRunner::classifyError(makeSparkError(
          "\n[PARSE_SYNTAX_ERROR] Syntax error at or near 'SELEC'.",
          "42601",
          "org.apache.spark.sql.catalyst.parser.ParseException")),
      "Spark failed to parse the query: \n[PARSE_SYNTAX_ERROR]");
  VELOX_ASSERT_THROW(
      SparkQueryRunner::classifyError(
          grpc::Status(
              grpc::StatusCode::INTERNAL,
              "\n[PARSE_SYNTAX_ERROR] Syntax error at or near 'SELEC'.")),
      "Spark failed to parse the query: \n[PARSE_SYNTAX_ERROR]");
  VELOX_ASSERT_THROW(
      SparkQueryRunner::classifyError(makeSparkError(
          "Cannot call methods on a stopped SparkContext.",
          "",
          "java.lang.IllegalStateException")),
      "Spark Connect server is not usable");
}

TEST_F(SparkQueryRunnerTest, isSupported) {
  auto aggregatePool = rootPool_->addAggregateChild("isSupported");
  auto queryRunner = std::make_unique<fuzzer::SparkQueryRunner>(
      aggregatePool.get(), "unused", "unused", "unused");

  EXPECT_TRUE(queryRunner->isSupported(*exec::FunctionSignatureBuilder()
                                            .typeVariable("K")
                                            .typeVariable("V")
                                            .returnType("map(K, array(V))")
                                            .argumentType("array(row(K, V))")
                                            .build()));
  EXPECT_TRUE(queryRunner->isSupported(
      *exec::FunctionSignatureBuilder()
           .integerVariable("a_precision")
           .integerVariable("a_scale")
           .returnType("boolean")
           .argumentType("decimal(a_precision, a_scale)")
           .argumentType("function(decimal(a_precision, a_scale), boolean)")
           .build()));
  EXPECT_TRUE(
      queryRunner->isSupported(*exec::FunctionSignatureBuilder()
                                    .returnType("interval year to month")
                                    .argumentType("integer")
                                    .build()));
  EXPECT_FALSE(queryRunner->isSupported(*exec::FunctionSignatureBuilder()
                                             .returnType("unknown")
                                             .argumentType("varchar")
                                             .build()));
  EXPECT_FALSE(queryRunner->isSupported(*exec::FunctionSignatureBuilder()
                                             .returnType("bigint")
                                             .argumentType("array(hugeint)")
                                             .build()));
  EXPECT_FALSE(
      queryRunner->isSupported(*exec::FunctionSignatureBuilder()
                                    .returnType("interval day to second")
                                    .argumentType("bigint")
                                    .build()));
}

TEST_F(SparkQueryRunnerTest, toSql) {
  auto aggregatePool = rootPool_->addAggregateChild("toSql");
  auto queryRunner = std::make_unique<fuzzer::SparkQueryRunner>(
      aggregatePool.get(), "unused", "unused", "unused");

  auto dataType = ROW({"c0", "c1", "c2"}, {DOUBLE(), DOUBLE(), BOOLEAN()});
  auto plan = exec::test::PlanBuilder()
                  .tableScan("tmp", dataType)
                  .singleAggregation({"c1"}, {"avg(c0)"})
                  .planNode();
  EXPECT_EQ(
      queryRunner->toSql(plan),
      "SELECT c1, avg(c0) AS a0 FROM (tmp) GROUP BY c1");

  plan = exec::test::PlanBuilder()
             .tableScan("tmp", dataType)
             .singleAggregation({"c1"}, {"sum(c0)"})
             .project({"a0 / c1"})
             .planNode();
  EXPECT_EQ(
      queryRunner->toSql(plan),
      "SELECT (a0 / c1) AS p0 FROM (SELECT c1, sum(c0) AS a0 FROM (tmp) GROUP BY c1)");

  plan = exec::test::PlanBuilder()
             .tableScan("tmp", dataType)
             .singleAggregation({}, {"avg(c0)", "avg(c1)"}, {"c2"})
             .planNode();
  EXPECT_EQ(
      queryRunner->toSql(plan),
      "SELECT avg(c0) FILTER (WHERE c2) AS a0, avg(c1) AS a1 FROM (tmp)");

  auto data =
      makeRowVector({makeFlatVector<int64_t>({}), makeFlatVector<int64_t>({})});
  plan = exec::test::PlanBuilder()
             .values({data})
             .singleAggregation({}, {"sum(distinct c0)"})
             .planNode();
  EXPECT_EQ(
      queryRunner->toSql(plan), "SELECT sum(DISTINCT c0) AS a0 FROM (t_0)");

  plan = exec::test::PlanBuilder()
             .values({data})
             .partialAggregation({}, {"sum(c0)"})
             .planNode();
  EXPECT_EQ(queryRunner->toSql(plan), std::nullopt);

  plan = exec::test::PlanBuilder()
             .values({data})
             .window(
                 {"sum(c1) over (partition by c0 order by c1 desc nulls first "
                  "rows between 1 preceding and current row)",
                  "rank() over (partition by c0 order by c1 desc nulls first)"})
             .planNode();
  EXPECT_EQ(
      queryRunner->toSql(plan),
      "SELECT c0, c1, sum(c1) OVER (PARTITION BY c0 ORDER BY c1 DESC NULLS "
      "FIRST ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS w0, rank() OVER "
      "(PARTITION BY c0 ORDER BY c1 DESC NULLS FIRST) AS w1 FROM (t_0)");

  // Intervals are read from Parquet as integers.
  data = makeRowVector(
      {"c0", "a b"},
      {makeFlatVector<int32_t>({}, INTERVAL_YEAR_MONTH()),
       makeFlatVector<int64_t>({})});
  plan = exec::test::PlanBuilder().values({data}).project({"c0"}).planNode();
  EXPECT_EQ(
      queryRunner->toSql(plan),
      "SELECT c0 AS c0 FROM (SELECT make_ym_interval(0, c0) AS c0, `a b` FROM t_0)");

  data = makeRowVector({makeArrayVector<int32_t>({}, INTERVAL_YEAR_MONTH())});
  plan = exec::test::PlanBuilder().values({data}).project({"c0"}).planNode();
  EXPECT_EQ(queryRunner->toSql(plan), std::nullopt);

  data = makeRowVector({makeFlatVector<int64_t>({}, INTERVAL_DAY_TIME())});
  plan = exec::test::PlanBuilder().values({data}).project({"c0"}).planNode();
  EXPECT_EQ(queryRunner->toSql(plan), std::nullopt);

  data = makeRowVector({makeFlatVector<int64_t>({})});
  plan =
      exec::test::PlanBuilder().values({data}).project({"rand()"}).planNode();
  EXPECT_EQ(queryRunner->toSql(plan), std::nullopt);
}

TEST_F(SparkQueryRunnerTest, isConstantExprSupported) {
  auto aggregatePool = rootPool_->addAggregateChild("isConstantExprSupported");
  auto queryRunner = std::make_unique<fuzzer::SparkQueryRunner>(
      aggregatePool.get(), "unused", "unused", "unused");
  EXPECT_TRUE(queryRunner->isConstantExprSupported(
      std::make_shared<core::ConstantTypedExpr>(
          BIGINT(), Variant(int64_t{1}))));
  EXPECT_TRUE(queryRunner->isConstantExprSupported(
      std::make_shared<core::ConstantTypedExpr>(
          makeArrayVector<int32_t>({{1, 2}}))));
  EXPECT_FALSE(queryRunner->isConstantExprSupported(
      std::make_shared<core::ConstantTypedExpr>(
          TIMESTAMP(), Variant(Timestamp(0, 1)))));
  EXPECT_FALSE(queryRunner->isConstantExprSupported(
      std::make_shared<core::ConstantTypedExpr>(
          HUGEINT(), Variant(int128_t{1}))));
}

} // namespace
} // namespace facebook::velox::functions::sparksql::test

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init{&argc, &argv, false};
  return RUN_ALL_TESTS();
}
