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
  int32_t numProbeBatches;
  int32_t numBuildRows;
  std::string filter;
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

class HashJoinNullAwareFilterBenchmark : public VectorTestBase {
 public:
  BenchmarkData prepareData(const BenchmarkParams& params) {
    BenchmarkData data;
    data.probeVectors = makeProbeVectors(params);
    data.buildVectors = makeBuildVectors(params);
    return data;
  }

  int64_t run(const BenchmarkParams& params, const BenchmarkData& data) {
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
                        {"t0", "t1"},
                        core::JoinType::kAnti,
                        true /* nullAware */)
                    .planNode();

    auto results = AssertQueryBuilder(plan).maxDrivers(1).copyResults(pool());
    return results->size();
  }

 private:
  std::vector<RowVectorPtr> makeProbeVectors(const BenchmarkParams& params) {
    std::vector<RowVectorPtr> batches;
    batches.reserve(params.numProbeBatches);

    const auto batchSize = params.numProbeRows / params.numProbeBatches;
    for (auto batch = 0; batch < params.numProbeBatches; ++batch) {
      const auto start = batch * batchSize;
      batches.push_back(makeRowVector({
          makeFlatVector<int64_t>(
              batchSize,
              [](vector_size_t /* row */) { return 0; },
              [](vector_size_t /* row */) { return true; }),
          makeFlatVector<int64_t>(
              batchSize, [&](vector_size_t row) { return start + row; }),
      }));
    }

    return batches;
  }

  std::vector<RowVectorPtr> makeBuildVectors(const BenchmarkParams& params) {
    return {makeRowVector({
        makeFlatVector<int64_t>(
            params.numBuildRows, [](vector_size_t row) { return row + 1; }),
        makeFlatVector<int64_t>(
            params.numBuildRows, [](vector_size_t row) { return row; }),
    })};
  }
};

} // namespace

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv};
  memory::MemoryManager::initialize(memory::MemoryManager::Options{});
  functions::prestosql::registerAllScalarFunctions();
  parse::registerTypeResolver();

  auto benchmark = std::make_unique<HashJoinNullAwareFilterBenchmark>();
  std::vector<BenchmarkCase> benchmarkCases;

  const std::vector<BenchmarkParams> params = {
      {.numProbeRows = 4'096,
       .numProbeBatches = 4,
       .numBuildRows = 32'768,
       .filter = "t1 < u1",
       .title = "all_null_probe_lt_large_build"},
      {.numProbeRows = 4'096,
       .numProbeBatches = 4,
       .numBuildRows = 32'768,
       .filter = "t1 > u1",
       .title = "all_null_probe_gt_large_build"},
      {.numProbeRows = 4'096,
       .numProbeBatches = 4,
       .numBuildRows = 32'768,
       .filter = "t1 = u1",
       .title = "all_null_probe_eq_large_build"},
      {.numProbeRows = 4'096,
       .numProbeBatches = 4,
       .numBuildRows = 32'768,
       .filter = "t1 <> u1",
       .title = "all_null_probe_neq_large_build"},
  };

  for (const auto& param : params) {
    benchmarkCases.push_back({param, benchmark->prepareData(param)});
  }

  for (const auto& benchmarkCase : benchmarkCases) {
    folly::addBenchmark(
        __FILE__, benchmarkCase.params.title, [&benchmark, &benchmarkCase]() {
          auto outputRows =
              benchmark->run(benchmarkCase.params, benchmarkCase.data);
          folly::doNotOptimizeAway(outputRows);
          return 1;
        });
  }

  folly::runBenchmarks();
  return 0;
}
