
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

#include "velox/exec/RowContainer.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::test;

namespace {
struct BenchmarkParams {
  vector_size_t numRows;
  int32_t numColumns;
  bool sparse;

  std::string title() const {
    return fmt::format(
        "rows:{},cols:{},{}", numRows, numColumns, sparse ? "sparse" : "dense");
  }
};

template <typename T>
concept SupportsNewRows = requires(T& rowContainer, folly::Range<char**> rows) {
  rowContainer.newRows(rows);
};

template <typename T>
concept SupportsStoreWithRowNumbers = requires(
    T& rowContainer,
    const DecodedVector& decoded,
    folly::Range<char**> rows,
    folly::Range<const vector_size_t*> inputRows) {
  rowContainer.store(decoded, rows, inputRows, 0);
};

template <typename T>
void allocateRows(T& rowContainer, folly::Range<char**> rows) {
  if constexpr (SupportsNewRows<T>) {
    rowContainer.newRows(rows);
  } else {
    for (auto& row : rows) {
      row = rowContainer.newRow();
    }
  }
}

template <typename T>
void storeColumn(
    T& rowContainer,
    const DecodedVector& decoded,
    folly::Range<char**> rows,
    folly::Range<const vector_size_t*> inputRows,
    int32_t column,
    bool denseInputRows) {
  if constexpr (SupportsStoreWithRowNumbers<T>) {
    rowContainer.store(decoded, rows, inputRows, column);
  } else {
    if (denseInputRows) {
      rowContainer.store(decoded, rows, column);
    } else {
      for (auto i = 0; i < rows.size(); ++i) {
        rowContainer.store(decoded, inputRows[i], rows[i], column);
      }
    }
  }
}

class RowContainerBatchAppendBenchmark : public VectorTestBase {
 public:
  void setup(const BenchmarkParams& params) {
    setupInternal(params);
  }

  void rowAtATime(uint32_t iterations) {
    for (auto i = 0; i < iterations; ++i) {
      auto rowContainer = makeRowContainer();
      const auto nextOffset = rowContainer->nextOffset();
      for (auto rowIndex : inputRows_) {
        auto* row = rowContainer->newRow();
        *reinterpret_cast<char**>(row + nextOffset) = nullptr;
        for (auto column = 0; column < decoded_.size(); ++column) {
          rowContainer->store(*decoded_[column], rowIndex, row, column);
        }
      }
      folly::doNotOptimizeAway(rowContainer->numRows());
    }
  }

  void batch(uint32_t iterations) {
    for (auto i = 0; i < iterations; ++i) {
      auto rowContainer = makeRowContainer();
      allocateRows(*rowContainer, folly::Range(rows_.data(), rows_.size()));

      const auto nextOffset = rowContainer->nextOffset();
      for (auto* row : rows_) {
        *reinterpret_cast<char**>(row + nextOffset) = nullptr;
      }

      const folly::Range<char**> rows(rows_.data(), rows_.size());
      const folly::Range<const vector_size_t*> inputRows(
          inputRows_.data(), inputRows_.size());
      for (auto column = 0; column < decoded_.size(); ++column) {
        storeColumn(
            *rowContainer,
            *decoded_[column],
            rows,
            inputRows,
            column,
            denseInputRows_);
      }
      folly::doNotOptimizeAway(rowContainer->numRows());
    }
  }

 private:
  void setupInternal(const BenchmarkParams& params) {
    types_.assign(params.numColumns, BIGINT());

    std::vector<VectorPtr> children;
    children.reserve(params.numColumns);
    for (auto column = 0; column < params.numColumns; ++column) {
      children.push_back(
          makeFlatVector<int64_t>(params.numRows, [column](auto row) {
            return row * 131 + column;
          }));
    }
    input_ = makeRowVector(std::move(children));

    activeRows_ = std::make_unique<SelectivityVector>(params.numRows);
    if (params.sparse) {
      for (auto row = 0; row < params.numRows; row += 2) {
        activeRows_->setValid(row, false);
      }
      activeRows_->updateBounds();
    }

    decoded_.clear();
    decoded_.reserve(params.numColumns);
    for (auto column = 0; column < params.numColumns; ++column) {
      decoded_.push_back(
          std::make_unique<DecodedVector>(
              *input_->childAt(column), *activeRows_));
    }

    inputRows_.clear();
    inputRows_.reserve(activeRows_->countSelected());
    activeRows_->applyToSelected(
        [&](auto rowIndex) { inputRows_.push_back(rowIndex); });
    rows_.resize(inputRows_.size());
    denseInputRows_ = inputRows_.size() == params.numRows;
  }

  std::unique_ptr<RowContainer> makeRowContainer() {
    return std::make_unique<RowContainer>(
        types_,
        false, // nullableKeys
        std::vector<Accumulator>{},
        std::vector<TypePtr>{},
        true, // hasNext
        true, // isJoinBuild
        false, // hasProbedFlag
        false, // hasCountFlag
        false, // hasNormalizedKey
        false, // useListRowIndex
        pool());
  }

  RowVectorPtr input_;
  std::unique_ptr<SelectivityVector> activeRows_;
  std::vector<TypePtr> types_;
  std::vector<std::unique_ptr<DecodedVector>> decoded_;
  std::vector<vector_size_t> inputRows_;
  std::vector<char*> rows_;
  bool denseInputRows_{false};
};
} // namespace

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv};
  memory::MemoryManager::Options options;
  options.useMmapAllocator = true;
  options.allocatorCapacity = 4UL << 30;
  options.useMmapArena = true;
  options.mmapArenaCapacityRatio = 1;
  memory::MemoryManager::initialize(options);

  std::vector<BenchmarkParams> params = {
      {10'000, 2, false},
      {10'000, 8, false},
      {10'000, 8, true},
      {50'000, 2, false},
      {50'000, 8, false},
      {50'000, 8, true},
  };

  for (const auto& param : params) {
    folly::addBenchmark(
        __FILE__,
        fmt::format("rowAtATime/{}", param.title()),
        [param](uint32_t iterations) {
          folly::BenchmarkSuspender suspender;
          auto benchmark = std::make_unique<RowContainerBatchAppendBenchmark>();
          benchmark->setup(param);
          suspender.dismiss();
          benchmark->rowAtATime(iterations);
          suspender.rehire();
          return iterations;
        });
    folly::addBenchmark(
        __FILE__,
        fmt::format("batch/{}", param.title()),
        [param](uint32_t iterations) {
          folly::BenchmarkSuspender suspender;
          auto benchmark = std::make_unique<RowContainerBatchAppendBenchmark>();
          benchmark->setup(param);
          suspender.dismiss();
          benchmark->batch(iterations);
          suspender.rehire();
          return iterations;
        });
  }

  folly::runBenchmarks();
  return 0;
}
