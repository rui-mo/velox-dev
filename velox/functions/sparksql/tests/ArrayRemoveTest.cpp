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

using namespace facebook::velox::test;

namespace facebook::velox::functions::sparksql::test {
namespace {

class ArrayRemoveTest : public SparkFunctionBaseTest {};

TEST_F(ArrayRemoveTest, nestedNulls) {
  // Nested nulls are compared as values, i.e. null equals null.
  auto arrays = makeNestedArrayVectorFromJson<int32_t>({
      "[[1, null], [1, 2], null, [null], [1]]",
      "[[null], [1, null]]",
  });
  auto elements = makeArrayVectorFromJson<int32_t>({"[1, null]", "[null]"});
  auto expected = makeNestedArrayVectorFromJson<int32_t>({
      "[[1, 2], null, [null], [1]]",
      "[[1, null]]",
  });
  auto result =
      evaluate("array_remove(c0, c1)", makeRowVector({arrays, elements}));
  assertEqualVectors(expected, result);

  auto rows = makeRowVector(
      {makeNullableFlatVector<int32_t>({1, 1, std::nullopt, 2}),
       makeNullableFlatVector<std::string>(
           {std::nullopt, "a", std::nullopt, "b"})},
      nullEvery(4, 3));
  arrays = makeArrayVector({0}, rows);
  auto element = makeRowVector(
      {makeNullableFlatVector<int32_t>({std::nullopt}),
       makeNullableFlatVector<std::string>({std::nullopt})});
  expected = makeArrayVector(
      {0},
      makeRowVector(
          {makeFlatVector<int32_t>({1, 1, 2}),
           makeNullableFlatVector<std::string>({std::nullopt, "a", "b"})},
          nullEvery(3, 2)));
  result = evaluate("array_remove(c0, c1)", makeRowVector({arrays, element}));
  assertEqualVectors(expected, result);
}

} // namespace
} // namespace facebook::velox::functions::sparksql::test
