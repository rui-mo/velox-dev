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
#include "velox/expression/fuzzer/SparkSpecialFormSignatureGenerator.h"

namespace facebook::velox::fuzzer {

std::vector<exec::FunctionSignaturePtr>
SparkSpecialFormSignatureGenerator::getSignaturesForCast() const {
  std::vector<exec::FunctionSignaturePtr> signatures =
      getCommonSignaturesForCast();

  // Spark string-to-time casts use TIME_MICRO_UTC.
  signatures.push_back(makeCastSignature("varchar", "TIME MICRO UTC"));

  // Cast integer types as varbinary is supported in Spark.
  for (auto fromType : {"tinyint", "smallint", "integer", "bigint"}) {
    signatures.push_back(makeCastSignature(fromType, "varbinary"));
  }

  // Cast the below types as timestamp is supported in Spark.
  for (auto fromType :
       {"boolean",
        "tinyint",
        "smallint",
        "integer",
        "bigint",
        "real",
        "double"}) {
    signatures.push_back(makeCastSignature(fromType, "timestamp"));
  }

  // Spark casts a timestamp to floating-point, boolean and decimal types.
  for (auto toType : {"real", "double", "boolean"}) {
    addCastFromTimestampSignature(toType, signatures);
  }
  for (const auto& toType : kDecimalTypes_) {
    addCastFromTimestampSignature(toType, signatures);
  }

  // Spark casts a date to boolean and numeric types as NULL. 'kIntegralTypes_'
  // includes boolean.
  for (const auto& toType : kIntegralTypes_) {
    addCastFromDateSignature(toType, signatures);
  }
  for (const auto& toType : kFloatingPointTypes_) {
    addCastFromDateSignature(toType, signatures);
  }
  for (const auto& toType : kDecimalTypes_) {
    addCastFromDateSignature(toType, signatures);
  }

  // Spark's interpreted evaluation rounds a decimal with more than 34
  // significant digits before casting it to timestamp. Velox keeps every digit,
  // as Spark's generated code does, so the precision is limited to 34.
  for (auto fromType :
       {"DECIMAL(6, 0)",
        "DECIMAL(18, 6)",
        "DECIMAL(34, 2)",
        "DECIMAL(34, 18)",
        "DECIMAL(34, 34)"}) {
    signatures.push_back(makeCastSignature(fromType, "timestamp"));
  }

  // For each supported translation pair T --> U, add signatures of array(T) -->
  // array(U), map(varchar, T) --> map(varchar, U), row(T) --> row(U).
  auto size = signatures.size();
  for (auto i = 0; i < size; ++i) {
    auto from = signatures[i]->argumentTypes()[0].baseName();
    auto to = signatures[i]->returnType().baseName();

    // Signature parsing of nested decimal type is not supported.
    if (from == "DECIMAL" || to == "DECIMAL") {
      continue;
    }

    signatures.push_back(makeCastSignature(
        fmt::format("array({})", from), fmt::format("array({})", to)));

    signatures.push_back(makeCastSignature(
        fmt::format("map(varchar, {})", from),
        fmt::format("map(varchar, {})", to)));

    signatures.push_back(makeCastSignature(
        fmt::format("row({})", from), fmt::format("row({})", to)));
  }

  // Cast timestamp as integer types is supported in Spark.
  for (auto toType : {"tinyint", "smallint", "integer", "bigint"}) {
    signatures.push_back(makeCastSignature("timestamp", toType));
  }

  return signatures;
}

const std::unordered_map<std::string, std::vector<exec::FunctionSignaturePtr>>&
SparkSpecialFormSignatureGenerator::getSignatures() const {
  const static std::
      unordered_map<std::string, std::vector<exec::FunctionSignaturePtr>>
          kSpecialForms{
              {"and", getSignaturesForAnd()},
              {"or", getSignaturesForOr()},
              {"coalesce", getSignaturesForCoalesce()},
              {"if", getSignaturesForIf()},
              {"switch", getSignaturesForSwitch()},
              {"cast", getSignaturesForCast()},
              {"concat_ws", getSignaturesForConcatWs()}};
  return kSpecialForms;
}

std::vector<exec::FunctionSignaturePtr>
SparkSpecialFormSignatureGenerator::getSignaturesForConcatWs() const {
  // Signature: concat_ws (separator, input, ...) -> output:
  // varchar, varchar, varchar, ... -> varchar
  return {facebook::velox::exec::FunctionSignatureBuilder()
              .argumentType("varchar")
              .variableArity("varchar")
              .returnType("varchar")
              .build()};
}

} // namespace facebook::velox::fuzzer
