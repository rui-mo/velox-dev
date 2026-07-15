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

#include <fmt/core.h>
#include <folly/Benchmark.h>
#include <folly/init/Init.h>

#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;

namespace {

struct BenchmarkCase {
  std::string name;
  core::PlanNodePtr plan;
  std::shared_ptr<TempFilePath> filePath;
};

class AggregationPushdownBenchmark : public HiveConnectorTestBase {
 public:
  void TestBody() override {}

  void initialize() {
    SetUp();
    rowType_ =
        ROW({"c0", "c1", "mask01", "mask10"},
            {BIGINT(), BIGINT(), BOOLEAN(), BOOLEAN()});
    filePath_ = TempFilePath::create();
    writeToFile(filePath_->getPath(), makeInput());
  }

  void addBenchmarks() {
    addBenchmark(
        "no_mask_pushdown",
        PlanBuilder()
            .tableScan(rowType_)
            .singleAggregation({"c0"}, {"sum(c1)"})
            .planNode());

    addMaskedBenchmarks("mask01", "1pct");
    addMaskedBenchmarks("mask10", "10pct");
  }

 private:
  std::vector<RowVectorPtr> makeInput() {
    constexpr int32_t kNumVectors = 50;
    constexpr vector_size_t kRowsPerVector = 10'000;

    std::vector<RowVectorPtr> vectors;
    vectors.reserve(kNumVectors);
    for (auto batch = 0; batch < kNumVectors; ++batch) {
      vectors.push_back(makeRowVector(
          rowType_->names(),
          {
              makeFlatVector<int64_t>(
                  kRowsPerVector, [](auto row) { return row % 1'024; }),
              makeFlatVector<int64_t>(
                  kRowsPerVector,
                  [batch](auto row) { return batch * kRowsPerVector + row; }),
              makeFlatVector<bool>(
                  kRowsPerVector, [](auto row) { return row % 100 == 0; }),
              makeFlatVector<bool>(
                  kRowsPerVector, [](auto row) { return row % 10 == 0; }),
          }));
    }
    return vectors;
  }

  void addMaskedBenchmarks(
      const std::string& maskColumn,
      const std::string& nameSuffix) {
    addBenchmark(
        fmt::format("{}_pushdown", nameSuffix),
        PlanBuilder()
            .tableScan(rowType_)
            .singleAggregation({"c0"}, {"sum(c1)"}, {maskColumn})
            .planNode());

    // Force c1 to load before aggregation. This keeps the same SQL semantics,
    // but blocks lazy aggregation pushdown for comparison.
    addBenchmark(
        fmt::format("{}_blocked_by_project", nameSuffix),
        PlanBuilder()
            .tableScan(rowType_)
            .project({"c0", "c1 + 0 AS c1", maskColumn})
            .singleAggregation({"c0"}, {"sum(c1)"}, {maskColumn})
            .planNode());
  }

  void addBenchmark(std::string name, core::PlanNodePtr plan) {
    cases_.push_back(
        std::make_unique<BenchmarkCase>(BenchmarkCase{
            .name = std::move(name),
            .plan = std::move(plan),
            .filePath = filePath_}));
    auto* testCase = cases_.back().get();
    folly::addBenchmark(__FILE__, testCase->name, [testCase]() {
      AssertQueryBuilder(testCase->plan)
          .splits(
              HiveConnectorTestBase::makeHiveConnectorSplits(
                  {testCase->filePath}))
          .serialExecution(true)
          .countResults();
      return 1;
    });
  }

  RowTypePtr rowType_;
  std::shared_ptr<TempFilePath> filePath_;
  std::vector<std::unique_ptr<BenchmarkCase>> cases_;
};

} // namespace

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);

  OperatorTestBase::SetUpTestCase();
  {
    AggregationPushdownBenchmark benchmark;
    benchmark.initialize();
    benchmark.addBenchmarks();
    folly::runBenchmarks();
    benchmark.TearDown();
  }
  OperatorTestBase::TearDownTestCase();

  return 0;
}
