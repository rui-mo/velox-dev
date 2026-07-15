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

#include <utility>

#include "velox/exec/Aggregate.h"
#include "velox/vector/AggregationHook.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/LazyVector.h"

namespace facebook::velox::functions::aggregate {

template <typename TInput, typename TAccumulator, typename TResult>
class SimpleNumericAggregate : public exec::Aggregate {
 protected:
  explicit SimpleNumericAggregate(TypePtr resultType) : Aggregate(resultType) {}

 public:
  void extractAccumulators(char** groups, int32_t numGroups, VectorPtr* result)
      override {
    extractValues(groups, numGroups, result);
  }

 protected:
  template <typename T>
  static constexpr bool kMayPushdown = !std::is_same_v<T, int128_t> &&
      !std::is_same_v<T, Timestamp> && !std::is_same_v<T, UnknownValue>;

  // TData is either TAccumulator or TResult, which in most cases are the same,
  // but for sum(real) can differ.
  template <typename TData = TResult, typename ExtractOneValue>
  void doExtractValues(
      char** groups,
      int32_t numGroups,
      VectorPtr* result,
      ExtractOneValue extractOneValue) {
    VELOX_CHECK_EQ((*result)->encoding(), VectorEncoding::Simple::FLAT);
    auto vector = (*result)->as<FlatVector<TData>>();
    VELOX_CHECK(
        vector,
        "Unexpected type of the result vector: {}",
        (*result)->type()->toString());
    VELOX_CHECK_EQ(vector->elementSize(), sizeof(TData));
    vector->resize(numGroups);
    uint64_t* rawNulls = getRawNulls(vector);
    if constexpr (std::is_same_v<TData, bool>) {
      uint64_t* rawValues = vector->template mutableRawValues<uint64_t>();
      for (int32_t i = 0; i < numGroups; ++i) {
        char* group = groups[i];
        if (isNull(group)) {
          vector->setNull(i, true);
        } else {
          clearNull(rawNulls, i);
          bits::setBit(rawValues, i, extractOneValue(group));
        }
      }
    } else {
      TData* rawValues = vector->mutableRawValues();
      for (int32_t i = 0; i < numGroups; ++i) {
        char* group = groups[i];
        if (isNull(group)) {
          vector->setNull(i, true);
        } else {
          clearNull(rawNulls, i);
          rawValues[i] = extractOneValue(group);
        }
      }
    }
  }

  // TData is used to store the updated group states. It can be either
  // TAccumulator or TResult, which in most cases are the same, but for
  // sum(real) can differ. TValue is used to decode the update input 'args'.
  // It can be either TAccumulator or TInput, which is most cases are the same
  // but for sum(real) can differ.
  template <
      bool tableHasNulls,
      typename TData = TResult,
      typename TValue = TInput,
      typename UpdateSingleValue>
  void updateGroups(
      char** groups,
      const SelectivityVector& rows,
      const VectorPtr& arg,
      UpdateSingleValue updateSingleValue,
      bool mayPushdown) {
    DecodedVector decoded(*arg, rows, !mayPushdown);
    if constexpr (kMayPushdown<TData>) {
      if (mayPushdown &&
          decoded.base()->encoding() == VectorEncoding::Simple::LAZY &&
          !arg->type()->isDecimal()) {
        auto* lazy = decoded.base()->asChecked<const LazyVector>();
        if (lazy->supportsHook()) {
          pushdown<
              velox::aggregate::SimpleCallableHook<TData, UpdateSingleValue>>(
              groups, rows, arg, updateSingleValue);
          return;
        }
        decoded.decode(*arg, rows);
      }
    }

    if (decoded.isConstantMapping()) {
      if (!decoded.isNullAt(0)) {
        auto value = decoded.valueAt<TValue>(0);
        rows.applyToSelected([&](vector_size_t i) {
          updateNonNullValue<tableHasNulls, TData>(
              groups[i], TData(value), updateSingleValue);
        });
      }
    } else if (decoded.mayHaveNulls()) {
      rows.applyToSelected([&](vector_size_t i) {
        if (decoded.isNullAt(i)) {
          return;
        }
        updateNonNullValue<tableHasNulls, TData>(
            groups[i], TData(decoded.valueAt<TValue>(i)), updateSingleValue);
      });
    } else if (decoded.isIdentityMapping() && !std::is_same_v<TValue, bool>) {
      auto data = decoded.data<TValue>();
      rows.applyToSelected([&](vector_size_t i) {
        updateNonNullValue<tableHasNulls, TData>(
            groups[i], TData(data[i]), updateSingleValue);
      });
    } else {
      rows.applyToSelected([&](vector_size_t i) {
        updateNonNullValue<tableHasNulls, TData>(
            groups[i], TData(decoded.valueAt<TValue>(i)), updateSingleValue);
      });
    }
  }

  // TData is used to store the updated group state. It can be either
  // TAccumulator or TResult, which in most cases are the same, but for
  // sum(real) can differ. TValue is used to decode the update input 'args'.
  // It can be either TAccumulator or TInput, which is most cases are the same
  // but for sum(real) can differ.
  template <
      typename TData = TResult,
      typename TValue = TInput,
      typename UpdateSingle,
      typename UpdateDuplicate>
  void updateOneGroup(
      char* group,
      const SelectivityVector& rows,
      const VectorPtr& arg,
      UpdateSingle updateSingleValue,
      UpdateDuplicate updateDuplicateValues,
      bool mayPushdown,
      TData initialValue) {
    DecodedVector decoded(*arg, rows, !mayPushdown);
    if constexpr (kMayPushdown<TData>) {
      if (mayPushdown &&
          decoded.base()->encoding() == VectorEncoding::Simple::LAZY &&
          !arg->type()->isDecimal()) {
        auto* lazy = decoded.base()->asChecked<const LazyVector>();
        if (lazy->supportsHook()) {
          pushdownSingleGroup<
              velox::aggregate::SimpleCallableHook<TData, UpdateSingle>>(
              group, rows, arg, updateSingleValue);
          return;
        }
        decoded.decode(*arg, rows);
      }
    }

    // Do row by row if not all rows are selected.
    if (decoded.isConstantMapping()) {
      if (!decoded.isNullAt(0)) {
        updateDuplicateValues(
            initialValue,
            TData(decoded.valueAt<TValue>(0)),
            rows.countSelected());
        updateNonNullValue<true, TData>(group, initialValue, updateSingleValue);
      }
    } else if (decoded.mayHaveNulls()) {
      rows.applyToSelected([&](vector_size_t i) {
        if (decoded.isNullAt(i)) {
          return;
        }
        updateNonNullValue<true, TData>(
            group, TData(decoded.valueAt<TValue>(i)), updateSingleValue);
      });
    } else if (decoded.isIdentityMapping() && !std::is_same_v<TValue, bool>) {
      auto data = decoded.data<TValue>();
      rows.applyToSelected([&](vector_size_t i) {
        updateNonNullValue<true, TData>(
            group, TData(data[i]), updateSingleValue);
      });
    } else {
      rows.applyToSelected([&](vector_size_t i) {
        updateNonNullValue<true, TData>(
            group, TData(decoded.valueAt<TValue>(i)), updateSingleValue);
      });
    }
  }

  template <typename THook, typename... Args>
  void pushdown(
      char** groups,
      const SelectivityVector& rows,
      const VectorPtr& arg,
      Args&&... args) {
    DecodedVector decoded(*arg, rows, false);
    const auto pushdownRows = preparePushdownRows(
        rows, decoded, arg->size(), groups, [&](vector_size_t row) {
          return groups[row];
        });
    THook hook(
        exec::Aggregate::offset_,
        exec::Aggregate::nullByte_,
        exec::Aggregate::nullMask_,
        pushdownRows.groups,
        &this->exec::Aggregate::numNulls_,
        std::forward<Args>(args)...);
    auto* lazy = decoded.base()->asChecked<const LazyVector>();
    VELOX_CHECK(lazy->supportsHook());
    lazy->load(RowSet(pushdownRows.indices, pushdownRows.size), &hook);
  }

  template <typename THook, typename... Args>
  void pushdownSingleGroup(
      char* group,
      const SelectivityVector& rows,
      const VectorPtr& arg,
      Args&&... args) {
    DecodedVector decoded(*arg, rows, false);
    const auto pushdownRows = preparePushdownRows(
        rows, decoded, arg->size(), nullptr, [&](vector_size_t /*row*/) {
          return group;
        });
    THook hook(
        exec::Aggregate::offset_,
        exec::Aggregate::nullByte_,
        exec::Aggregate::nullMask_,
        pushdownRows.groups,
        &this->exec::Aggregate::numNulls_,
        std::forward<Args>(args)...);
    auto* lazy = decoded.base()->asChecked<const LazyVector>();
    VELOX_CHECK(lazy->supportsHook());
    lazy->load(RowSet(pushdownRows.indices, pushdownRows.size), &hook);
  }

 private:
  struct PushdownRows {
    const vector_size_t* indices;
    vector_size_t size;
    char** groups;
  };

  template <typename GroupAt>
  PushdownRows preparePushdownRows(
      const SelectivityVector& rows,
      const DecodedVector& decoded,
      vector_size_t inputSize,
      char** denseGroups,
      GroupAt groupAt) {
    const vector_size_t* indices = decoded.indices();
    char** hookGroups = denseGroups;
    vector_size_t numIndices{inputSize};

    // ValueHook sees load output ordinals. When 'rows' is selective, compact
    // both the lazy load RowSet and the group pointers so ordinal i addresses
    // the i-th selected row in both arrays.
    if (!rows.isAllSelected()) {
      const auto numSelected = rows.countSelected();
      if (numSelected != inputSize) {
        pushdownCustomIndices_.resize(numSelected);
        pushdownCustomGroups_.resize(numSelected);
        vector_size_t targetIndex{0};
        rows.applyToSelected([&](vector_size_t row) {
          pushdownCustomIndices_[targetIndex] = indices[row];
          pushdownCustomGroups_[targetIndex] = groupAt(row);
          ++targetIndex;
        });
        return {
            pushdownCustomIndices_.data(),
            numSelected,
            pushdownCustomGroups_.data()};
      }
    }

    if (hookGroups == nullptr) {
      pushdownCustomGroups_.resize(inputSize);
      for (vector_size_t row = 0; row < inputSize; ++row) {
        pushdownCustomGroups_[row] = groupAt(row);
      }
      hookGroups = pushdownCustomGroups_.data();
    }

    return {indices, numIndices, hookGroups};
  }

  // TData is either TAccumulator or TResult, which in most cases are the same,
  // but for sum(real) can differ.
  template <
      bool tableHasNulls,
      typename TDataType = TAccumulator,
      typename Update>
  inline void
  updateNonNullValue(char* group, TDataType value, Update updateValue) {
    if constexpr (tableHasNulls) {
      exec::Aggregate::clearNull(group);
    }
    updateValue(*exec::Aggregate::value<TDataType>(group), value);
  }
};

} // namespace facebook::velox::functions::aggregate
