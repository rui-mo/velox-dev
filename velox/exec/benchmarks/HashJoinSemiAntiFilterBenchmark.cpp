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

#include <fmt/format.h>
#include <folly/Benchmark.h>
#include <folly/init/Init.h>

#include <algorithm>
#include <random>
#include <tuple>

#include "velox/common/memory/Memory.h"
#include "velox/core/QueryConfig.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::test;

// Benchmarks anti, left semi filter and left semi project joins with a filter
// that reads only probe columns. Such a filter gives the same result for every
// build row with the same key, so the hash table keeps one row per key.
// Compares filters that pass all and 1/4 of the probe rows with no filter, for
// 1, 4 and 32 build rows per key. Some keys only appear on one side. Reports
// the number of output rows as 'outputRows'.
//
// Runs the task serially with 4,096-row input and output batches, which are
// Gluten's defaults.
namespace {

constexpr vector_size_t kBatchRows = 4'096;

struct BenchmarkParams {
  core::JoinType joinType;
  // Number of distinct keys on both sides.
  int32_t numMatchedKeys;
  // Number of distinct keys that only appear once on the build side.
  int32_t numBuildOnlyKeys;
  // Number of distinct keys that only appear once on the probe side.
  int32_t numProbeOnlyKeys;
  // Number of build rows per key on both sides.
  int32_t buildDuplicates;
  // Number of probe rows per key on both sides.
  int32_t probeDuplicates;
  // Join filter. Empty means no filter.
  std::string filter;
  std::vector<std::string> outputLayout;
  std::string title;
};

struct BenchmarkData {
  std::vector<RowVectorPtr> buildVectors;
  std::vector<RowVectorPtr> probeVectors;
};

struct BenchmarkCase {
  BenchmarkParams params;
  BenchmarkData data;
};

class HashJoinSemiAntiFilterBenchmark : public VectorTestBase {
 public:
  BenchmarkData prepareData(const BenchmarkParams& params) {
    const auto buildKeys = makeKeys(
        params.numMatchedKeys,
        params.buildDuplicates,
        params.numMatchedKeys,
        params.numBuildOnlyKeys,
        /*seed=*/31);
    const auto probeKeys = makeKeys(
        params.numMatchedKeys,
        params.probeDuplicates,
        params.numMatchedKeys + params.numBuildOnlyKeys,
        params.numProbeOnlyKeys,
        /*seed=*/97);

    return {
        .buildVectors = makeBatches(buildKeys, 0),
        .probeVectors = makeBatches(probeKeys, 10'000'000)};
  }

  // Returns the number of output rows.
  uint64_t run(const BenchmarkParams& params, const BenchmarkData& data) {
    auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();

    auto plan = PlanBuilder(planNodeIdGenerator, pool_.get())
                    .values(data.probeVectors)
                    .project({"c0 AS t0", "c1 AS t1"})
                    .hashJoin(
                        {"t0"},
                        {"u0"},
                        PlanBuilder(planNodeIdGenerator, pool_.get())
                            .values(data.buildVectors)
                            .project({"c0 AS u0", "c1 AS u1"})
                            .planNode(),
                        params.filter,
                        params.outputLayout,
                        params.joinType)
                    .planNode();

    return AssertQueryBuilder(plan)
        .maxDrivers(1)
        .serialExecution(true)
        .config(core::QueryConfig::kPreferredOutputBatchRows, kBatchRows)
        .config(core::QueryConfig::kMaxOutputBatchRows, kBatchRows)
        .countResults();
  }

 private:
  // Returns keys [0, numMatchedKeys) repeated 'duplicates' times and keys
  // [onlyKeysStart, onlyKeysStart + numOnlyKeys) once, in a deterministic
  // random order.
  static std::vector<int64_t> makeKeys(
      int32_t numMatchedKeys,
      int32_t duplicates,
      int64_t onlyKeysStart,
      int32_t numOnlyKeys,
      uint32_t seed) {
    VELOX_CHECK_GT(numMatchedKeys, 0);
    VELOX_CHECK_GT(duplicates, 0);
    VELOX_CHECK_GE(numOnlyKeys, 0);

    const int64_t numRepeatedKeys = numMatchedKeys * duplicates;
    std::vector<int64_t> keys(numRepeatedKeys + numOnlyKeys);
    for (int64_t i = 0; i < numRepeatedKeys; ++i) {
      keys[i] = i % numMatchedKeys;
    }
    for (int32_t i = 0; i < numOnlyKeys; ++i) {
      keys[numRepeatedKeys + i] = onlyKeysStart + i;
    }

    std::mt19937 random(seed);
    std::shuffle(keys.begin(), keys.end(), random);
    return keys;
  }

  // Splits 'keys' into batches of 'kBatchRows' rows with the key in column
  // 'c0' and a distinct non-negative payload starting at 'payloadBase' in
  // column 'c1'.
  std::vector<RowVectorPtr> makeBatches(
      const std::vector<int64_t>& keys,
      int64_t payloadBase) {
    std::vector<RowVectorPtr> batches;
    const int64_t numRows = keys.size();
    for (int64_t start = 0; start < numRows; start += kBatchRows) {
      const auto size = static_cast<vector_size_t>(
          std::min<int64_t>(kBatchRows, numRows - start));
      batches.push_back(makeRowVector({
          makeFlatVector<int64_t>(
              size, [&](vector_size_t row) { return keys[start + row]; }),
          makeFlatVector<int64_t>(
              size,
              [&](vector_size_t row) { return payloadBase + start + row; }),
      }));
    }
    return batches;
  }
};

} // namespace

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv};
  memory::MemoryManager::initialize(memory::MemoryManager::Options{});
  functions::prestosql::registerAllScalarFunctions();
  parse::registerTypeResolver();

  auto benchmark = std::make_unique<HashJoinSemiAntiFilterBenchmark>();

  const std::vector<
      std::tuple<core::JoinType, std::string, std::vector<std::string>>>
      joinTypes = {
          {core::JoinType::kAnti, "anti", {"t1"}},
          {core::JoinType::kLeftSemiFilter, "left_semi_filter", {"t1"}},
          {core::JoinType::kLeftSemiProject,
           "left_semi_project",
           {"t1", "match"}},
      };
  // Probe payloads are non-negative, so 't1 >= 0' passes every row.
  const std::vector<std::pair<std::string, std::string>> filters = {
      {"", "no_filter"},
      {"t1 >= 0", "with_filter"},
      {"t1 % 4 = 0", "selective_filter"},
  };
  const std::vector<int32_t> buildDuplicates = {1, 4, 32};
  constexpr int32_t kProbeDuplicates = 128;

  std::vector<BenchmarkCase> benchmarkCases;
  for (const auto& [joinType, joinName, outputLayout] : joinTypes) {
    for (const auto duplicates : buildDuplicates) {
      for (const auto& [filter, filterName] : filters) {
        BenchmarkParams params{
            .joinType = joinType,
            .numMatchedKeys = 1'000,
            .numBuildOnlyKeys = 1'000,
            .numProbeOnlyKeys = 1'000,
            .buildDuplicates = duplicates,
            .probeDuplicates = kProbeDuplicates,
            .filter = filter,
            .outputLayout = outputLayout,
            .title = fmt::format(
                "{}_{}_dup{}x{}",
                joinName,
                filterName,
                duplicates,
                kProbeDuplicates)};
        auto data = benchmark->prepareData(params);
        benchmarkCases.push_back({std::move(params), std::move(data)});
      }
    }
  }

  for (const auto& benchmarkCase : benchmarkCases) {
    folly::addBenchmark(
        __FILE__,
        benchmarkCase.params.title,
        [&benchmark, &benchmarkCase](folly::UserCounters& counters) {
          const auto outputRows =
              benchmark->run(benchmarkCase.params, benchmarkCase.data);
          counters["outputRows"] = static_cast<int64_t>(outputRows);
          return 1;
        });
  }

  folly::runBenchmarks();
  return 0;
}
