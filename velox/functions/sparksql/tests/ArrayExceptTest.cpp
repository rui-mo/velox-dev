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

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"

using namespace facebook::velox::test;

namespace facebook::velox::functions::sparksql::test {
namespace {

class ArrayExceptTest : public SparkFunctionBaseTest {};

TEST_F(ArrayExceptTest, elementsContainingMaps) {
  auto maps = makeMapVectorFromJson<int64_t, int64_t>({"{1: 2}"});
  for (const auto& elements : std::vector<VectorPtr>{
           maps, makeArrayVector({0}, maps), makeRowVector({maps})}) {
    auto arrays = makeArrayVector({0}, elements);
    VELOX_ASSERT_USER_THROW(
        evaluate("array_except(c0, c1)", makeRowVector({arrays, arrays})),
        fmt::format(
            "Scalar function signature is not supported: array_except({}, {})",
            arrays->type()->toString(),
            arrays->type()->toString()));
  }
}

} // namespace
} // namespace facebook::velox::functions::sparksql::test
