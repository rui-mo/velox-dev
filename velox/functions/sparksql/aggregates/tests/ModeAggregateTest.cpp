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

#include "velox/exec/Aggregate.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/lib/aggregates/tests/utils/AggregationTestBase.h"
#include "velox/functions/sparksql/aggregates/Register.h"

using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::functions::aggregate::test;

namespace facebook::velox::functions::aggregate::sparksql::test {

namespace {

class ModeAggregateTest : public AggregationTestBase {
 protected:
  void SetUp() override {
    AggregationTestBase::SetUp();
    registerAggregateFunctions("");
  }

  void testModeWithDuck(const VectorPtr& vector1, const VectorPtr& vector2) {
    ASSERT_EQ(vector1->size(), vector2->size());

    auto num = vector1->size();
    auto reverseIndices = makeIndicesInReverse(num);

    auto vectors = makeRowVector(
        {vector1,
         vector2,
         wrapInDictionary(reverseIndices, num, vector1),
         wrapInDictionary(reverseIndices, num, vector2)});

    createDuckDbTable({vectors});
    testAggregations(
        {vectors},
        {"c0"},
        {"mode(c1)"},
        "SELECT c0, mode(c1) FROM tmp GROUP BY c0");

    testAggregations(
        {vectors},
        {"c0"},
        {"mode(c3)"},
        "SELECT c0, mode(c3) FROM tmp GROUP BY c0");

    testAggregations(
        {vectors},
        {"c2"},
        {"mode(c1)"},
        "SELECT c2, mode(c1) FROM tmp GROUP BY c2");

    testAggregations(
        {vectors},
        {"c2"},
        {"mode(c3)"},
        "SELECT c2, mode(c3) FROM tmp GROUP BY c2");
  }

  void testGlobalModeWithDuck(const VectorPtr& vector) {
    auto num = vector->size();
    auto reverseIndices = makeIndicesInReverse(num);

    auto vectors =
        makeRowVector({vector, wrapInDictionary(reverseIndices, num, vector)});

    createDuckDbTable({vectors});
    testAggregations({vectors}, {}, {"mode(c0)"}, "SELECT mode(c0) FROM tmp");

    testAggregations({vectors}, {}, {"mode(c1)"}, "SELECT mode(c1) FROM tmp");
  }

  void testMode(
      const std::string& expression,
      const std::vector<std::string>& groupKeys,
      const VectorPtr& vector1,
      const VectorPtr& vector2,
      const RowVectorPtr& expected) {
    auto vectors = makeRowVector({vector1, vector2});
    testAggregations({vectors}, groupKeys, {expression}, {expected});
  }
};

TEST_F(ModeAggregateTest, groupByInteger) {
  vector_size_t num = 37;

  auto vector1 = makeFlatVector<int32_t>(
      num, [](vector_size_t row) { return row % 3; }, nullEvery(4));
  auto vector2 = makeFlatVector<int32_t>(
      num, [](vector_size_t row) { return row % 2; }, nullEvery(5));

  testModeWithDuck(vector1, vector2);

  // Test when some group-by keys have only null values.
  auto vector3 =
      makeNullableFlatVector<int64_t>({1, 1, 1, 2, 2, 2, 3, 3, std::nullopt});
  auto vector4 = makeNullableFlatVector<int64_t>(
      {10, 10, 10, 20, std::nullopt, 20, std::nullopt, std::nullopt, 20});

  testModeWithDuck(vector3, vector4);
}

TEST_F(ModeAggregateTest, groupByDouble) {
  vector_size_t num = 37;

  auto vector1 = makeFlatVector<int32_t>(
      num, [](vector_size_t row) { return row % 3; }, nullEvery(4));
  auto vector2 = makeFlatVector<double>(
      num, [](vector_size_t row) { return row % 2 + 0.05; }, nullEvery(5));

  testModeWithDuck(vector1, vector2);
}

TEST_F(ModeAggregateTest, groupByBoolean) {
  vector_size_t num = 37;

  auto vector1 = makeFlatVector<int32_t>(
      num, [](vector_size_t row) { return row % 3; }, nullEvery(4));
  auto vector2 = makeFlatVector<bool>(
      num, [](vector_size_t row) { return row % 2 == 0; }, nullEvery(5));

  auto expected = makeRowVector(
      {makeNullableFlatVector<int32_t>({std::nullopt, 0, 1, 2}),
       makeFlatVector<bool>({true, false, false, false})});

  testMode("mode(c1)", {"c0"}, vector1, vector2, expected);
}

TEST_F(ModeAggregateTest, groupByTimestamp) {
  vector_size_t num = 10;

  auto vector1 = makeFlatVector<int32_t>(
      num, [](vector_size_t row) { return row % 3; }, nullEvery(4));
  auto vector2 = makeFlatVector<Timestamp>(
      num,
      [](vector_size_t row) { return Timestamp{row % 2, 17'123'456}; },
      nullEvery(5));

  auto expected = makeRowVector(
      {makeNullableFlatVector<int32_t>({std::nullopt, 0, 1, 2}),
       makeFlatVector<Timestamp>(
           {Timestamp{0, 17'123'456},
            Timestamp{1, 17'123'456},
            Timestamp{1, 17'123'456},
            Timestamp{0, 17'123'456}})});

  testMode("mode(c1)", {"c0"}, vector1, vector2, expected);
}

TEST_F(ModeAggregateTest, groupByDate) {
  vector_size_t num = 10;

  auto vector1 = makeFlatVector<int32_t>(
      num, [](vector_size_t row) { return row % 3; }, nullEvery(4));
  auto vector2 = makeFlatVector<int32_t>(
      num, [](vector_size_t row) { return row % 2; }, nullEvery(5), DATE());

  auto expected = makeRowVector(
      {makeNullableFlatVector<int32_t>({std::nullopt, 0, 1, 2}),
       makeFlatVector<int32_t>({0, 1, 1, 0}, DATE())});

  testMode("mode(c1)", {"c0"}, vector1, vector2, expected);
}

TEST_F(ModeAggregateTest, groupByInterval) {
  vector_size_t num = 30;

  auto vector1 = makeFlatVector<int32_t>(num, [](auto row) { return row; });
  auto vector2 = makeFlatVector<int64_t>(
      num,
      [](auto row) { return row % 5 == 0 ? 2 : row + 1; },
      nullEvery(5),
      INTERVAL_DAY_TIME());

  testModeWithDuck(vector1, vector2);
}

TEST_F(ModeAggregateTest, groupByString) {
  std::vector<std::string> strings = {
      "grapes",
      "oranges",
      "sweet fruits: apple",
      "sweet fruits: banana",
      "sweet fruits: papaya",
  };

  auto keys = makeFlatVector<int16_t>(
      1'002, [](auto row) { return row % 5; }, nullEvery(5));
  auto data = makeFlatVector<StringView>(
      1'002,
      [&](auto row) { return StringView(strings[row % strings.size()]); },
      nullEvery(5));
  testModeWithDuck(keys, data);
}

TEST_F(ModeAggregateTest, globalInteger) {
  vector_size_t num = 30;
  auto vector = makeFlatVector<int32_t>(
      num, [](vector_size_t row) { return row % 7; }, nullEvery(7));

  testGlobalModeWithDuck(vector);
}

TEST_F(ModeAggregateTest, globalDecimal) {
  vector_size_t num = 10;
  auto longDecimalType = DECIMAL(20, 2);
  auto vector1 = makeFlatVector<int128_t>(
      num,
      [](vector_size_t row) { return row % 4; },
      nullEvery(7),
      longDecimalType);

  testGlobalModeWithDuck(vector1);

  auto shortDecimalType = DECIMAL(6, 2);
  auto vector2 = makeFlatVector<int64_t>(
      num,
      [](vector_size_t row) { return row % 4; },
      nullEvery(7),
      shortDecimalType);

  testGlobalModeWithDuck(vector2);
}

TEST_F(ModeAggregateTest, globalUnknown) {
  auto vector = makeAllNullFlatVector<UnknownValue>(6);

  auto expected = makeRowVector({
      BaseVector::createNullConstant(UNKNOWN(), 1, pool()),
  });

  testMode("mode(c1)", {}, vector, vector, expected);
}

TEST_F(ModeAggregateTest, globalDouble) {
  vector_size_t num = 32;
  auto vector = makeFlatVector<double>(
      num, [](vector_size_t row) { return row % 5 + 0.05; }, nullEvery(5));

  testGlobalModeWithDuck(vector);
}

TEST_F(ModeAggregateTest, globalBoolean) {
  auto vector =
      makeNullableFlatVector<bool>({false, false, true, std::nullopt});

  auto expected =
      makeRowVector({makeFlatVector<bool>(std::vector<bool>{false})});

  testMode("mode(c1)", {}, vector, vector, expected);
}

TEST_F(ModeAggregateTest, globalTimestamp) {
  vector_size_t num = 10;
  auto vector = makeFlatVector<Timestamp>(
      num,
      [](vector_size_t row) { return Timestamp{row % 4, 100}; },
      nullEvery(7));

  auto expected = makeRowVector(
      {makeFlatVector<Timestamp>(std::vector<Timestamp>{Timestamp{1, 100}})});

  testMode("mode(c1)", {}, vector, vector, expected);
}

TEST_F(ModeAggregateTest, globalDate) {
  vector_size_t num = 10;
  auto vector = makeFlatVector<int32_t>(
      num, [](vector_size_t row) { return row % 4; }, nullEvery(7), DATE());

  auto expected =
      makeRowVector({makeFlatVector<int32_t>(std::vector<int32_t>{1}, DATE())});

  testMode("mode(c1)", {}, vector, vector, expected);
}

TEST_F(ModeAggregateTest, globalInterval) {
  auto vector = makeFlatVector<int64_t>(
      51, [](auto row) { return row % 7; }, nullEvery(7), INTERVAL_DAY_TIME());

  testGlobalModeWithDuck(vector);
}

TEST_F(ModeAggregateTest, globalEmpty) {
  auto vector = makeFlatVector<int32_t>({});

  auto expected = makeRowVector({
      BaseVector::createNullConstant(INTEGER(), 1, pool()),
  });

  testMode("mode(c1)", {}, vector, vector, expected);
}

TEST_F(ModeAggregateTest, globalString) {
  std::vector<std::string> strings = {
      "grapes",
      "oranges",
      "sweet fruits: apple",
      "sweet fruits: banana",
      "sweet fruits: papaya",
  };

  auto data = makeFlatVector<StringView>(1'001, [&](auto row) {
    return StringView(strings[row % strings.size()]);
  });
  testGlobalModeWithDuck(data);

  // Some nulls.
  data = makeFlatVector<StringView>(
      1'002,
      [&](auto row) { return StringView(strings[row % strings.size()]); },
      nullEvery(strings.size()));
  testGlobalModeWithDuck(data);

  // All nulls.
  testGlobalModeWithDuck(makeAllNullFlatVector<StringView>(1'000));

  // Lots of unique strings.
  std::string scratch;
  data = makeFlatVector<StringView>(
      1'002,
      [&](auto row) {
        scratch = std::string(50 + row % 1'000, 'A' + (row % 10));
        return StringView(scratch);
      },
      nullEvery(10));
  testGlobalModeWithDuck(data);
}

TEST_F(ModeAggregateTest, globalNaNs) {
  // Verify that NaNs with different binary representations are considered equal
  // and deduplicated.
  static const auto kNaN = std::numeric_limits<double>::quiet_NaN();
  static const auto kSNaN = std::numeric_limits<double>::signaling_NaN();
  auto vector = makeFlatVector<double>({1, kNaN, kSNaN, 2, 3, kNaN, kSNaN, 3});

  auto expected =
      makeRowVector({makeFlatVector<double>(std::vector<double>{kNaN})});

  testMode("mode(c1)", {}, vector, vector, expected);
}

TEST_F(ModeAggregateTest, allNulls) {
  auto vector = makeAllNullFlatVector<int32_t>(3);

  auto expected = makeRowVector({makeAllNullFlatVector<int32_t>(1)});

  testMode("mode(c1)", {}, vector, vector, expected);

  vector = makeAllNullFlatVector<int32_t>(0);

  testMode("mode(c1)", {}, vector, vector, expected);
}

TEST_F(ModeAggregateTest, arrays) {
  auto input = makeRowVector({
      makeFlatVector<int64_t>({0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1}),
      makeArrayVectorFromJson<int32_t>({
          "[1, 2, 3]",
          "[1, 2]",
          "[]",
          "[1, 2]",
          "[]",
          "[1, null, 2, null]",
          "[1, null, 2, null]",
          "[]",
          "[1, null, 2, null]",
          "null",
          "[1, null, 2, null]",
          "null",
      }),
  });

  auto expected = makeRowVector({
      makeArrayVectorFromJson<int32_t>({"[1, null, 2, null]"}),
  });

  testAggregations({input}, {}, {"mode(c1)"}, {expected});

  // Group by.
  expected = makeRowVector({
      makeFlatVector<int64_t>({0, 1}),
      makeArrayVectorFromJson<int32_t>({
          "[1, null, 2, null]",
          "[1, 2]",
      }),
  });
  testAggregations({input}, {"c0"}, {"mode(c1)"}, {expected});
}

TEST_F(ModeAggregateTest, maps) {
  auto input = makeRowVector({
      makeFlatVector<int64_t>({0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1}),
      makeMapVectorFromJson<int32_t, int32_t>({
          "{1: 10, 2: 20, 3: 30}",
          "{1: 10, 2: 20}",
          "{}",
          "{1: 10, 2: 20}",
          "{}",
          "{1: 10, 2: 20, 3: null}",
          "{1: 10, 2: 20, 3: null}",
          "{}",
          "{1: 10, 2: 20, 3: null}",
          "null",
          "{1: 10, 2: 20, 3: null}",
          "null",
      }),
  });

  auto expected = makeRowVector({
      makeMapVectorFromJson<int32_t, int32_t>({"{1: 10, 2: 20, 3: null}"}),
  });

  testAggregations({input}, {}, {"mode(c1)"}, {expected});

  // Group by.
  expected = makeRowVector({
      makeFlatVector<int64_t>({0, 1}),
      makeMapVectorFromJson<int32_t, int32_t>({
          "{1: 10, 2: 20, 3: null}",
          "{1: 10, 2: 20}",
      }),
  });
  testAggregations({input}, {"c0"}, {"mode(c1)"}, {expected});
}

TEST_F(ModeAggregateTest, rows) {
  auto input = makeRowVector({
      makeRowVector({
          makeFlatVector<int32_t>({
              1,
              1,
              2,
              1,
          }),
          makeNullableFlatVector<int32_t>({
              std::nullopt,
              1,
              2,
              1,
          }),
      }),
  });

  auto expected = makeRowVector({
      makeRowVector({
          makeConstant(1, 1),
          makeConstant(1, 1),
      }),
  });

  testAggregations({input}, {}, {"mode(c0)"}, {expected});

  // Group by.
  auto input1 = makeRowVector({
      makeFlatVector<int64_t>({1, 0, 1, 0, 1, 1}),
      makeRowVector({
          makeFlatVector<int32_t>({
              1,
              1,
              2,
              1,
              2,
              2,
          }),
          makeNullableFlatVector<int32_t>({
              std::nullopt,
              1,
              2,
              1,
              1,
              2,
          }),
      }),
  });
  auto expected1 = makeRowVector({
      makeFlatVector<int64_t>({0, 1}),
      makeRowVector({
          makeFlatVector<int32_t>({1, 2}),
          makeFlatVector<int32_t>({1, 2}),
      }),
  });
  testAggregations({input1}, {"c0"}, {"mode(c1)"}, {expected1});
}

TEST_F(ModeAggregateTest, negativeZero) {
  // -0.0 is counted with 0.0, which makes them the most frequent value. Spark
  // counts every map separately, so all the maps hold a zero.
  auto data = makeRowVector({
      makeFlatVector<double>({-0.0, 0.0, 9.0}),
      makeFlatVector<float>({-0.0f, 0.0f, 9.0f}),
      makeArrayVector<double>({{-0.0}, {0.0}, {9.0}}),
      makeRowVector({makeFlatVector<double>({-0.0, 0.0, 9.0})}),
      makeMapVector<int32_t, double>({{{1, -0.0}}, {{1, 0.0}}, {{1, -0.0}}}),
  });
  const std::vector<std::string> aggregates = {
      "mode(c0)", "mode(c1)", "mode(c2)", "mode(c3)", "mode(c4)"};

  // -0.0 is returned as 0.0, also when nested in arrays, structs and map
  // values.
  const auto assertZero = [&](const core::PlanNodePtr& plan) {
    const auto result = AssertQueryBuilder(plan).copyResults(pool());
    const auto expectZero = [](double value) {
      EXPECT_EQ(value, 0.0);
      EXPECT_FALSE(std::signbit(value));
    };
    expectZero(result->childAt(0)->as<SimpleVector<double>>()->valueAt(0));
    expectZero(result->childAt(1)->as<SimpleVector<float>>()->valueAt(0));
    const auto* array = result->childAt(2)->as<ArrayVector>();
    expectZero(array->elements()->as<SimpleVector<double>>()->valueAt(
        array->offsetAt(0)));
    expectZero(result->childAt(3)
                   ->as<RowVector>()
                   ->childAt(0)
                   ->as<SimpleVector<double>>()
                   ->valueAt(0));
    const auto* map = result->childAt(4)->as<MapVector>();
    expectZero(map->mapValues()->as<SimpleVector<double>>()->valueAt(
        map->offsetAt(0)));
  };

  assertZero(
      PlanBuilder()
          .values({data})
          .singleAggregation({}, aggregates)
          .planNode());
  assertZero(
      PlanBuilder()
          .values({data})
          .partialAggregation({}, aggregates)
          .finalAggregation()
          .planNode());
}

} // namespace
} // namespace facebook::velox::functions::aggregate::sparksql::test
