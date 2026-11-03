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
#pragma once

#include <boost/random/uniform_int_distribution.hpp>
#include <string>

#include "velox/exec/fuzzer/InputGenerator.h"
#include "velox/functions/sparksql/aggregates/BitmapConstructAggAggregate.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::functions::sparksql::fuzzer {

/// Generates the inputs that bitmap_construct_agg and bitmap_or_agg accept:
/// bit positions in [0, 32768) and 4096-byte bitmaps. Both functions throw for
/// almost all random values, which leaves nothing to compare with Spark.
class BitmapAggregateInputGenerator : public exec::test::InputGenerator {
 public:
  std::vector<VectorPtr> generate(
      const std::vector<TypePtr>& types,
      VectorFuzzer& fuzzer,
      FuzzerGenerator& rng,
      memory::MemoryPool* pool) override {
    VELOX_CHECK_EQ(types.size(), 1);
    const auto size = fuzzer.getOptions().vectorSize;
    const auto nullRatio = fuzzer.getOptions().nullRatio;

    if (types[0]->isBigint()) {
      auto positions =
          BaseVector::create<FlatVector<int64_t>>(BIGINT(), size, pool);
      for (auto i = 0; i < size; ++i) {
        if (fuzzer.coinToss(nullRatio)) {
          positions->setNull(i, true);
        } else {
          positions->set(i, randomPosition(rng));
        }
      }
      return {positions};
    }

    VELOX_CHECK(
        types[0]->isVarbinary(), "Unexpected type: {}", types[0]->toString());
    auto bitmaps =
        BaseVector::create<FlatVector<StringView>>(VARBINARY(), size, pool);
    std::string bitmap;
    for (auto i = 0; i < size; ++i) {
      if (fuzzer.coinToss(nullRatio)) {
        bitmaps->setNull(i, true);
        continue;
      }
      // Sets a few bits, so the results of groups differ.
      bitmap.assign(aggregate::sparksql::kBitmapNumBytes, '\0');
      const auto numBits =
          boost::random::uniform_int_distribution<int32_t>(0, 16)(rng);
      for (auto j = 0; j < numBits; ++j) {
        const auto position = randomPosition(rng);
        bitmap[position / 8] |= static_cast<char>(1 << (position % 8));
      }
      bitmaps->set(i, StringView(bitmap));
    }
    return {bitmaps};
  }

  void reset() override {}

 private:
  static int64_t randomPosition(FuzzerGenerator& rng) {
    return boost::random::uniform_int_distribution<int64_t>(
        0, aggregate::sparksql::kBitmapNumBits - 1)(rng);
  }
};

} // namespace facebook::velox::functions::sparksql::fuzzer
