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

#include "velox/functions/Macros.h"
#include "velox/type/FloatingPointUtil.h"

namespace facebook::velox::functions::sparksql {

/// array_remove(array(E), element) -> array(E)
/// Removes all elements that equal 'element' from the array. Null elements are
/// kept. Unlike Presto, nulls nested in complex type elements are compared as
/// values, i.e. null equals null.
template <typename TExec>
struct ArrayRemoveFunction {
  VELOX_DEFINE_FUNCTION_TYPES(TExec);

  // Fast path for primitives.
  template <typename Out, typename In, typename E>
  void call(Out& out, const In& array, E element) {
    for (const auto& item : array) {
      if (!item.has_value()) {
        out.add_null();
        continue;
      }
      bool equal;
      if constexpr (std::is_floating_point_v<E>) {
        equal =
            util::floating_point::NaNAwareEquals<E>{}(element, item.value());
      } else {
        equal = element == item.value();
      }
      if (!equal) {
        out.push_back(item.value());
      }
    }
  }

  // Generic implementation.
  void call(
      out_type<Array<Generic<T1>>>& out,
      const arg_type<Array<Generic<T1>>>& array,
      const arg_type<Generic<T1>>& element) {
    static constexpr CompareFlags kFlags =
        CompareFlags::equality(CompareFlags::NullHandlingMode::kNullAsValue);
    for (const auto& item : array) {
      if (!item.has_value() || element.compare(item.value(), kFlags) != 0) {
        out.push_back(item);
      }
    }
  }
};

} // namespace facebook::velox::functions::sparksql
