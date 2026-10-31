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

#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"

namespace facebook::velox::functions::sparksql::test {
namespace {

using namespace facebook::velox::test;

class MapZipWithTest : public SparkFunctionBaseTest {
 protected:
  // Returns the keys and the values of map_zip_with(c0, c1), which list the
  // entries in the order of the result map.
  std::pair<VectorPtr, VectorPtr> zipKeysAndValues(const RowVectorPtr& data) {
    static const std::string kZip =
        "map_zip_with(c0, c1, (k, v1, v2) -> add(coalesce(v1, 0), coalesce(v2, 0)))";
    return {
        evaluate(fmt::format("map_keys({})", kZip), data),
        evaluate(fmt::format("map_values({})", kZip), data)};
  }
};

// The result lists the keys of the first map in their order, then the keys
// only in the second map in their order.
TEST_F(MapZipWithTest, keyOrder) {
  // makeMapVectorFromJson does not keep the key order.
  auto data = makeRowVector({
      makeNullableMapVector<int64_t, int64_t>({
          {{{3, 30}, {1, 10}}},
          {{}},
          {{{5, 50}, {4, 40}}},
          std::nullopt,
      }),
      makeMapVector<int64_t, int64_t>({
          {{2, 200}, {1, 100}, {0, 0}},
          {{2, 200}, {1, 100}},
          {},
          {{1, 100}},
      }),
  });
  auto [keys, values] = zipKeysAndValues(data);
  assertEqualVectors(
      makeArrayVectorFromJson<int64_t>({
          "[3, 1, 2, 0]",
          "[2, 1]",
          "[5, 4]",
          "null",
      }),
      keys);
  assertEqualVectors(
      makeArrayVectorFromJson<int64_t>({
          "[30, 110, 200, 0]",
          "[200, 100]",
          "[50, 40]",
          "null",
      }),
      values);
}

TEST_F(MapZipWithTest, complexKeyOrder) {
  auto data = makeRowVector({
      makeMapVector(
          {0},
          makeArrayVectorFromJson<int64_t>({"[2]", "[1]"}),
          makeFlatVector<int64_t>({10, 20})),
      makeMapVector(
          {0},
          makeArrayVectorFromJson<int64_t>({"[0]", "[2]"}),
          makeFlatVector<int64_t>({30, 40})),
  });
  auto [keys, values] = zipKeysAndValues(data);
  assertEqualVectors(
      makeNestedArrayVectorFromJson<int64_t>({"[[2], [1], [0]]"}), keys);
  assertEqualVectors(
      makeArrayVectorFromJson<int64_t>({"[50, 20, 30]"}), values);
}

} // namespace
} // namespace facebook::velox::functions::sparksql::test
