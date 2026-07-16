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

#include <folly/Benchmark.h>
#include <folly/init/Init.h>

#include "velox/common/memory/Memory.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::test;

namespace {

struct BenchmarkParams {
  int32_t numProbeRows;
  int32_t numBuildRows;
  int32_t numProbeBatches;
  int32_t probeKeyNullPct;
  int32_t buildFilterNullPct;
  std::string title;
};

struct BenchmarkData {
  std::vector<RowVectorPtr> probeVectors;
  std::vector<RowVectorPtr> buildVectors;
};

struct BenchmarkCase {
  BenchmarkParams params;
  BenchmarkData data;
};

class HashJoinNullAwareAntiFilterBenchmark : public VectorTestBase {
 public:
  BenchmarkData prepareData(const BenchmarkParams& params) {
    BenchmarkData data;
    data.probeVectors = makeProbeVectors(params);
    data.buildVectors = makeBuildVectors(params);
    return data;
  }

  int64_t run(const BenchmarkData& data) {
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
                        "t1 = u1",
                        {"t0"},
                        core::JoinType::kAnti,
                        true)
                    .planNode();

    auto results = AssertQueryBuilder(plan).maxDrivers(1).copyResults(pool());
    return results->size();
  }

 private:
  std::vector<RowVectorPtr> makeProbeVectors(const BenchmarkParams& params) {
    std::vector<RowVectorPtr> vectors;
    vectors.reserve(params.numProbeBatches);

    const int32_t batchSize = params.numProbeRows / params.numProbeBatches;
    int32_t start = 0;
    for (auto i = 0; i < params.numProbeBatches; ++i) {
      const auto size = i == params.numProbeBatches - 1
          ? params.numProbeRows - start
          : batchSize;
      vectors.push_back(makeRowVector(
          {"c0", "c1"},
          {
              makeFlatVector<int64_t>(
                  size,
                  [&](auto row) {
                    return static_cast<int64_t>((start + row) % 100'000);
                  },
                  [&](auto row) {
                    return ((start + row) % 100) < params.probeKeyNullPct;
                  }),
              makeFlatVector<int64_t>(
                  size,
                  [&](auto row) {
                    return static_cast<int64_t>((start + row) % 1'000);
                  }),
          }));
      start += size;
    }
    return vectors;
  }

  std::vector<RowVectorPtr> makeBuildVectors(const BenchmarkParams& params) {
    return {
        makeRowVector(
            {"c0", "c1"},
            {
                makeFlatVector<int64_t>(
                    params.numBuildRows,
                    [](auto row) {
                      return static_cast<int64_t>(row % 100'000);
                    }),
                makeFlatVector<int64_t>(
                    params.numBuildRows,
                    [](auto row) { return static_cast<int64_t>(row % 1'000); },
                    [&](auto row) {
                      return (row % 100) < params.buildFilterNullPct;
                    }),
            }),
    };
  }
};

} // namespace

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv};
  memory::MemoryManager::initialize(memory::MemoryManager::Options{});
  functions::prestosql::registerAllScalarFunctions();
  parse::registerTypeResolver();

  auto benchmark = std::make_unique<HashJoinNullAwareAntiFilterBenchmark>();
  std::vector<BenchmarkCase> benchmarkCases;

  const std::vector<BenchmarkParams> params = {
      {.numProbeRows = 20'000,
       .numBuildRows = 200'000,
       .numProbeBatches = 4,
       .probeKeyNullPct = 90,
       .buildFilterNullPct = 90,
       .title = "null_aware_anti_filter_probe_null90_build_filter_null90"},
      {.numProbeRows = 20'000,
       .numBuildRows = 200'000,
       .numProbeBatches = 4,
       .probeKeyNullPct = 95,
       .buildFilterNullPct = 99,
       .title = "null_aware_anti_filter_probe_null95_build_filter_null99"},
  };

  for (const auto& param : params) {
    benchmarkCases.push_back({param, benchmark->prepareData(param)});
  }

  for (const auto& benchmarkCase : benchmarkCases) {
    folly::addBenchmark(
        __FILE__, benchmarkCase.params.title, [&benchmark, &benchmarkCase]() {
          auto outputRows = benchmark->run(benchmarkCase.data);
          folly::doNotOptimizeAway(outputRows);
          return 1;
        });
  }

  folly::runBenchmarks();
  return 0;
}
