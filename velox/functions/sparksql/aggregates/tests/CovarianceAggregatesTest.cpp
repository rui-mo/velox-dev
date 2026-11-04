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

#include <fmt/ranges.h>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/lib/aggregates/tests/utils/AggregationTestBase.h"
#include "velox/functions/sparksql/SparkQueryConfig.h"
#include "velox/functions/sparksql/aggregates/Register.h"

using namespace facebook::velox::exec::test;
using namespace facebook::velox::functions::aggregate::test;

namespace facebook::velox::functions::aggregate::sparksql::test {

using functions::sparksql::SparkQueryConfig;

namespace {

class CovarianceAggregatesTest : public AggregationTestBase {
 protected:
  void SetUp() override {
    AggregationTestBase::SetUp();
    registerAggregateFunctions("spark_");
  }

  void testCovarianceAggResult(
      const std::string& agg,
      const RowVectorPtr& input,
      const RowVectorPtr& expected,
      bool legacy = false) {
    auto plan =
        PlanBuilder()
            .values({input})
            .singleAggregation({}, {fmt::format("spark_{}(c0, c1)", agg)})
            .planNode();
    AssertQueryBuilder(plan)
        .config(
            SparkQueryConfig::qualify(
                SparkQueryConfig::kLegacyStatisticalAggregate),
            legacy ? "true" : "false")
        .assertResults({expected});
  }
};

TEST_F(CovarianceAggregatesTest, corr) {
  auto agg = "corr";
  auto input = makeRowVector(
      {makeFlatVector<double>({1, 2, 3}), makeFlatVector<double>({1, 2, 3})});
  auto expected =
      makeRowVector({makeFlatVector<double>(std::vector<double>{1.0})});
  testCovarianceAggResult(agg, input, expected);

  input = makeRowVector(
      {makeFlatVector<double>({1, 3, 5}), makeFlatVector<double>({2, 4, 6})});
  expected = makeRowVector({makeFlatVector<double>(std::vector<double>{1.0})});
  testCovarianceAggResult(agg, input, expected);

  // Output NULL when m2x equals 0.
  input = makeRowVector(
      {makeFlatVector<double>({1, 1, 1}), makeFlatVector<double>({2, 2, 2})});
  expected = makeRowVector({makeNullableFlatVector<double>(
      std::vector<std::optional<double>>{std::nullopt})});
  testCovarianceAggResult(agg, input, expected);

  // Output NULL when count equals 1.
  input = makeRowVector({makeFlatVector<double>(1), makeFlatVector<double>(1)});
  expected = makeRowVector({makeNullableFlatVector<double>(
      std::vector<std::optional<double>>{std::nullopt})});
  testCovarianceAggResult(agg, input, expected);

  // Output NULL when count equals 0.
  input = makeRowVector(
      {makeNullableFlatVector<double>(
           std::vector<std::optional<double>>{std::nullopt}),
       makeNullableFlatVector<double>(
           std::vector<std::optional<double>>{std::nullopt})});
  expected = makeRowVector({makeNullableFlatVector<double>(
      std::vector<std::optional<double>>{std::nullopt})});
  testCovarianceAggResult(agg, input, expected);

  // Output NULL when m2x equals 0 for legacy aggregate.
  input = makeRowVector(
      {makeFlatVector<double>({1, 1, 1}), makeFlatVector<double>({2, 2, 2})});
  expected = makeRowVector({makeNullableFlatVector<double>(
      std::vector<std::optional<double>>{std::nullopt})});
  testCovarianceAggResult(agg, input, expected, true);

  // Output NaN when count equals 1 for legacy aggregate.
  input = makeRowVector({makeFlatVector<double>(1), makeFlatVector<double>(1)});
  expected = makeRowVector({makeFlatVector<double>(
      std::vector<double>{std::numeric_limits<double>::quiet_NaN()})});
  testCovarianceAggResult(agg, input, expected, true);

  // Output NULL when m2x or m2y equals 0, even if the other is NaN, and NaN
  // when m2x or m2y is NaN otherwise.
  constexpr auto kInf = std::numeric_limits<double>::infinity();
  constexpr auto kNaN = std::numeric_limits<double>::quiet_NaN();
  const std::vector<std::tuple<
      std::vector<double>,
      std::vector<double>,
      std::optional<double>>>
      testCases = {
          {{0, 0, 0, 0, 0, 0}, {kNaN, 0, 0, 0, 0, 0}, std::nullopt},
          {{kNaN, 1}, {2, 2}, std::nullopt},
          {{kInf, 1, 3}, {2, 2, 2}, std::nullopt},
          {{kNaN, 1, 2}, {1, 2, 3}, kNaN},
          {{1, 2, 3}, {kInf, 2, 3}, kNaN},
      };
  for (const auto& [x, y, result] : testCases) {
    SCOPED_TRACE(
        fmt::format("x: {}, y: {}", fmt::join(x, ", "), fmt::join(y, ", ")));
    input =
        makeRowVector({makeFlatVector<double>(x), makeFlatVector<double>(y)});
    expected = makeRowVector({makeNullableFlatVector<double>(
        std::vector<std::optional<double>>{result})});
    testCovarianceAggResult(agg, input, expected);
    testCovarianceAggResult(agg, input, expected, true);
  }

  // Output NULL when count equals 0 for legacy aggregate.
  input = makeRowVector(
      {makeNullableFlatVector<double>(
           std::vector<std::optional<double>>{std::nullopt}),
       makeNullableFlatVector<double>(
           std::vector<std::optional<double>>{std::nullopt})});
  expected = makeRowVector({makeNullableFlatVector<double>(
      std::vector<std::optional<double>>{std::nullopt})});
  testCovarianceAggResult(agg, input, expected, true);
}

TEST_F(CovarianceAggregatesTest, covarSamp) {
  auto agg = "covar_samp";
  auto input = makeRowVector(
      {makeFlatVector<double>({1, 2, 3}), makeFlatVector<double>({1, 2, 3})});
  auto expected =
      makeRowVector({makeFlatVector<double>(std::vector<double>{1.0})});
  testCovarianceAggResult(agg, input, expected);

  input = makeRowVector(
      {makeFlatVector<double>({1, 3, 5}), makeFlatVector<double>({2, 4, 6})});
  expected = makeRowVector({makeFlatVector<double>(std::vector<double>{4.0})});
  testCovarianceAggResult(agg, input, expected);

  // Output NULL when count equals 0.
  input = makeRowVector(
      {makeNullableFlatVector<double>(
           std::vector<std::optional<double>>{std::nullopt}),
       makeNullableFlatVector<double>(
           std::vector<std::optional<double>>{std::nullopt})});
  expected = makeRowVector({makeNullableFlatVector<double>(
      std::vector<std::optional<double>>{std::nullopt})});
  testCovarianceAggResult(agg, input, expected);

  // Output NULL when count equals 1.
  input = makeRowVector({makeFlatVector<double>(1), makeFlatVector<double>(1)});
  expected = makeRowVector({makeNullableFlatVector<double>(
      std::vector<std::optional<double>>{std::nullopt})});
  testCovarianceAggResult(agg, input, expected);

  // Output NULL when count equals 0 for legacy aggregate.
  input = makeRowVector(
      {makeNullableFlatVector<double>(
           std::vector<std::optional<double>>{std::nullopt}),
       makeNullableFlatVector<double>(
           std::vector<std::optional<double>>{std::nullopt})});
  expected = makeRowVector({makeNullableFlatVector<double>(
      std::vector<std::optional<double>>{std::nullopt})});
  testCovarianceAggResult(agg, input, expected, true);

  // Output NaN when m2 equals 1 for legacy aggregate.
  input = makeRowVector({makeFlatVector<double>(1), makeFlatVector<double>(1)});
  expected = makeRowVector({makeFlatVector<double>(
      std::vector<double>{std::numeric_limits<double>::quiet_NaN()})});
  testCovarianceAggResult(agg, input, expected, true);

  // Output ±inf when c2 overflows or x has inf. When the first x is inf, c2 is
  // NaN because the first pair adds 'inf * 0' to it.
  constexpr auto kInf = std::numeric_limits<double>::infinity();
  constexpr auto kNaN = std::numeric_limits<double>::quiet_NaN();
  const std::vector<
      std::tuple<std::vector<double>, std::vector<double>, double>>
      testCases = {
          {{22, kInf}, {0.688, 0.225}, -kInf},
          {{kInf, 22}, {0.225, 0.688}, kNaN},
          {{1e200, -1e200}, {1e200, -1e200}, kInf},
          {{1e200, -1e200}, {-1e200, 1e200}, -kInf},
      };
  for (const auto& [x, y, result] : testCases) {
    SCOPED_TRACE(
        fmt::format("x: {}, y: {}", fmt::join(x, ", "), fmt::join(y, ", ")));
    input =
        makeRowVector({makeFlatVector<double>(x), makeFlatVector<double>(y)});
    expected =
        makeRowVector({makeFlatVector<double>(std::vector<double>{result})});
    testCovarianceAggResult(agg, input, expected);
    testCovarianceAggResult(agg, input, expected, true);
  }
}

TEST_F(CovarianceAggregatesTest, legacyMergeEmptyBuffer) {
  // The final aggregation merges the partial result into an empty
  // accumulator. With the legacy config, the merge multiplies terms of the
  // partial result by the zero count of the accumulator, which makes the
  // co-moment NaN if a mean is infinite or large.
  const auto test = [&](const std::string& agg,
                        const std::vector<double>& x,
                        const std::vector<double>& y,
                        bool legacy,
                        std::optional<double> expected) {
    SCOPED_TRACE(
        fmt::format(
            "{}, x: {}, y: {}, legacy: {}",
            agg,
            fmt::join(x, ", "),
            fmt::join(y, ", "),
            legacy));
    auto plan =
        PlanBuilder()
            .values({makeRowVector(
                {makeFlatVector<double>(x), makeFlatVector<double>(y)})})
            .partialAggregation({}, {fmt::format("spark_{}(c0, c1)", agg)})
            .finalAggregation()
            .planNode();
    AssertQueryBuilder(plan)
        .config(
            SparkQueryConfig::qualify(
                SparkQueryConfig::kLegacyCovarianceMergeEmptyBuffer),
            legacy ? "true" : "false")
        .assertResults(makeRowVector({makeNullableFlatVector<double>(
            std::vector<std::optional<double>>{expected})}));
  };

  constexpr auto kInf = std::numeric_limits<double>::infinity();
  constexpr auto kNaN = std::numeric_limits<double>::quiet_NaN();
  test("covar_samp", {22, kInf}, {0.688, 0.225}, false, -kInf);
  test("covar_samp", {22, kInf}, {0.688, 0.225}, true, kNaN);
  test("covar_samp", {1e300, 1e300}, {1e300, 1e300}, false, 0);
  test("covar_samp", {1e300, 1e300}, {1e300, 1e300}, true, kNaN);
  test("covar_samp", {1e200, -1e200}, {1e200, -1e200}, true, kInf);
  test("covar_samp", {1, 3, 5}, {2, 4, 6}, true, 4);
  test("corr", {1e300, 1e300}, {1, 2}, false, std::nullopt);
  test("corr", {1e300, 1e300}, {1, 2}, true, kNaN);
  test("corr", {1, 2, 3}, {1, 2, 3}, true, 1);
}

TEST_F(CovarianceAggregatesTest, corrLegacyZeroVariance) {
  // With the legacy config, corr divides the co-moment by sqrt(m2x * m2y)
  // even if a variance is zero. A zero divisor makes the result NULL, or
  // throws in ANSI mode, and a NaN divisor makes it NaN.
  constexpr auto kInf = std::numeric_limits<double>::infinity();
  constexpr auto kNaN = std::numeric_limits<double>::quiet_NaN();
  const auto makePlan = [&](const VectorPtr& x, const VectorPtr& y) {
    return PlanBuilder()
        .values({makeRowVector({x, y})})
        .singleAggregation({}, {"spark_corr(c0, c1)"})
        .planNode();
  };
  const auto makeConfigs = [](bool ansi, bool legacyStatisticalAggregate) {
    return std::unordered_map<std::string, std::string>{
        {SparkQueryConfig::qualify(SparkQueryConfig::kLegacyCorrZeroVariance),
         "true"},
        {SparkQueryConfig::qualify(SparkQueryConfig::kAnsiEnabled),
         ansi ? "true" : "false"},
        {SparkQueryConfig::qualify(
             SparkQueryConfig::kLegacyStatisticalAggregate),
         legacyStatisticalAggregate ? "true" : "false"},
    };
  };
  const auto test = [&](const std::vector<double>& x,
                        const std::vector<double>& y,
                        bool ansi,
                        std::optional<double> expected,
                        bool legacyStatisticalAggregate = false) {
    SCOPED_TRACE(
        fmt::format(
            "x: {}, y: {}, ansi: {}, legacyStatisticalAggregate: {}",
            fmt::join(x, ", "),
            fmt::join(y, ", "),
            ansi,
            legacyStatisticalAggregate));
    AssertQueryBuilder(
        makePlan(makeFlatVector<double>(x), makeFlatVector<double>(y)))
        .configs(makeConfigs(ansi, legacyStatisticalAggregate))
        .assertResults(makeRowVector({makeNullableFlatVector<double>(
            std::vector<std::optional<double>>{expected})}));
  };

  for (const bool ansi : {false, true}) {
    test({kNaN, 1}, {2, 2}, ansi, kNaN);
    test({kInf, 1}, {2, 2}, ansi, kNaN);
    test({1, 2, 3}, {1, 2, 3}, ansi, 1);
    test({1}, {2}, ansi, std::nullopt);
    test({1}, {2}, ansi, kNaN, true);

    const auto nulls = makeNullableFlatVector<double>(
        std::vector<std::optional<double>>{std::nullopt});
    AssertQueryBuilder(makePlan(nulls, nulls))
        .configs(makeConfigs(ansi, false))
        .assertResults(makeRowVector({nulls}));
  }
  test({1, 1}, {2, 3}, false, std::nullopt);
  test({1, 1}, {2, 3}, false, std::nullopt, true);
  VELOX_ASSERT_USER_THROW(
      AssertQueryBuilder(
          makePlan(
              makeFlatVector<double>({1, 1}), makeFlatVector<double>({2, 3})))
          .configs(makeConfigs(true, false))
          .copyResults(pool()),
      "Division by zero");
}

} // namespace
} // namespace facebook::velox::functions::aggregate::sparksql::test
