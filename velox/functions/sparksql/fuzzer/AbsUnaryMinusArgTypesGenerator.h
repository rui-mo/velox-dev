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

#include "velox/expression/fuzzer/ArgTypesGenerator.h"

namespace facebook::velox::functions::sparksql::fuzzer {

/// Generates argument types for abs and unaryminus, whose argument type is the
/// same as the return type. Spark negates decimals with Java's DECIMAL128
/// math context, which rounds values to 34 significant digits, while Velox
/// negates them exactly. Decimal types whose precision exceeds 34 are not
/// generated to avoid such mismatches.
class AbsUnaryMinusArgTypesGenerator : public velox::fuzzer::ArgTypesGenerator {
 public:
  std::vector<TypePtr> generateArgs(
      const exec::FunctionSignature& /*signature*/,
      const TypePtr& returnType,
      FuzzerGenerator& /*rng*/) override {
    if (returnType->isDecimal() &&
        getDecimalPrecisionScale(*returnType).first > kMaxPrecision) {
      return {};
    }
    return {returnType};
  }

 private:
  // The precision of Java's MathContext.DECIMAL128.
  static constexpr uint8_t kMaxPrecision = 34;
};

} // namespace facebook::velox::functions::sparksql::fuzzer
