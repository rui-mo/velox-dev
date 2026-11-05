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

#include "velox/expression/fuzzer/ArgValuesGenerators.h"

#include <cctype>
#include <cmath>
#include <limits>

#include "velox/common/fuzzer/ConstrainedGenerators.h"
#include "velox/common/fuzzer/Utils.h"
#include "velox/core/Expressions.h"
#include "velox/functions/prestosql/S2CellOperations.h"
#include "velox/functions/prestosql/types/JsonType.h"
#include "velox/type/DecimalUtil.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

namespace facebook::velox::fuzzer {

namespace {

const std::vector<TypePtr>& jsonScalarTypes() {
  static const std::vector<TypePtr> kScalarTypes{
      BOOLEAN(),
      TINYINT(),
      SMALLINT(),
      INTEGER(),
      BIGINT(),
      REAL(),
      DOUBLE(),
      VARCHAR(),
      VARBINARY(),
      TIMESTAMP(),
  };
  return kScalarTypes;
}

} // namespace

std::vector<core::TypedExprPtr> JsonParseArgValuesGenerator::generate(
    const CallableSignature& signature,
    const VectorFuzzer::Options& options,
    FuzzerGenerator& rng,
    ExpressionFuzzerState& state) {
  VELOX_CHECK_EQ(signature.args.size(), 1);
  populateInputTypesAndNames(signature, state);

  // Populate state.customInputGenerators_ for the argument that requires custom
  // generation. A nullptr should be added at state.customInputGenerators_[i] if
  // the i-th argument does not require custom input generation.
  const auto representedType =
      facebook::velox::randType(rng, jsonScalarTypes(), 3);
  const auto seed = rand<uint32_t>(rng);
  const auto nullRatio = options.nullRatio;
  state.customInputGenerators_.emplace_back(
      std::make_shared<fuzzer::JsonInputGenerator>(
          seed,
          signature.args[0],
          nullRatio,
          fuzzer::getRandomInputGenerator(seed, representedType, nullRatio),
          true));

  // Populate inputExpressions_ for the argument that requires custom
  // generation. A nullptr should be added at inputExpressions[i] if the i-th
  // argument does not require custom input generation.
  std::vector<core::TypedExprPtr> inputExpressions{
      signature.args.size(), nullptr};
  inputExpressions[0] = std::make_shared<core::FieldAccessTypedExpr>(
      signature.args[0], state.inputRowNames_.back());
  return inputExpressions;
};

std::vector<core::TypedExprPtr> StringEscapeArgValuesGenerator::generate(
    const CallableSignature& signature,
    const VectorFuzzer::Options& options,
    FuzzerGenerator& rng,
    ExpressionFuzzerState& state) {
  VELOX_CHECK_EQ(signature.args.size(), 1);
  populateInputTypesAndNames(signature, state);

  const auto representedType = facebook::velox::randType(rng, 0);
  const auto seed = static_cast<size_t>(rand<uint32_t>(rng));
  const auto nullRatio = options.nullRatio;

  state.customInputGenerators_.emplace_back(
      std::make_shared<RandomInputGenerator<StringView>>(
          seed,
          signature.args[0],
          nullRatio,
          20,
          std::vector<UTF8CharList>{
              UTF8CharList::ASCII,
              UTF8CharList::UNICODE_CASE_SENSITIVE,
              UTF8CharList::EXTENDED_UNICODE,
              UTF8CharList::MATHEMATICAL_SYMBOLS},
          RandomStrVariationOptions{
              0.5, // controlCharacterProbability
              0.5, // escapeStringProbability
              0.1, // truncateProbability
          }));

  // Populate inputExpressions_ for the argument that requires custom
  // generation. A nullptr should be added at inputExpressions[i] if the i-th
  // argument does not require custom input generation.
  std::vector<core::TypedExprPtr> inputExpressions{
      signature.args.size(), nullptr};
  inputExpressions[0] = std::make_shared<core::FieldAccessTypedExpr>(
      signature.args[0], state.inputRowNames_.back());
  return inputExpressions;
};

std::vector<core::TypedExprPtr> PhoneNumberArgValuesGenerator::generate(
    const CallableSignature& signature,
    const VectorFuzzer::Options& options,
    FuzzerGenerator& rng,
    ExpressionFuzzerState& state) {
  populateInputTypesAndNames(signature, state);

  const auto representedType = facebook::velox::randType(rng, 0);
  const auto seed = rand<uint32_t>(rng);
  const auto nullRatio = options.nullRatio;
  std::vector<core::TypedExprPtr> inputExpressions;

  if (functionName_ == "fb_phone_region_code") {
    // Populate state.customInputGenerators_ and inputExpressions with custom
    // input generator for required phone number string field
    VELOX_CHECK_GE(signature.args.size(), 1);
    state.customInputGenerators_.emplace_back(
        std::make_shared<fuzzer::PhoneNumberInputGenerator>(
            seed, signature.args[0], nullRatio));

    VELOX_CHECK_GE(state.inputRowNames_.size(), 1);
    inputExpressions.emplace_back(
        std::make_shared<core::FieldAccessTypedExpr>(
            signature.args[0],
            signature.args.size() == 1
                ? state.inputRowNames_.back()
                : state.inputRowNames_[state.inputRowNames_.size() - 2]));

    //  Populate state.customInputGenerators_ and inputExpressions with nullptr
    //  for inputs that do not require custom input generators
    if (signature.args.size() == 2) {
      state.customInputGenerators_.emplace_back(nullptr);
      inputExpressions.emplace_back(nullptr);
    }
  } else if (functionName_ == "fb_canonicalize_phone_number") {
    VELOX_CHECK_GE(signature.args.size(), 1);
    // Populate state.customInputGenerators_ and inputExpressions with custom
    // input generator for required phone number string field
    state.customInputGenerators_.emplace_back(
        std::make_shared<fuzzer::PhoneNumberInputGenerator>(
            seed, signature.args[0], nullRatio));

    VELOX_CHECK_GE(state.inputRowNames_.size(), signature.args.size());
    inputExpressions.emplace_back(
        std::make_shared<core::FieldAccessTypedExpr>(
            signature.args[0],
            signature.args.size() == 1
                ? state.inputRowNames_.back()
                : state.inputRowNames_
                      [state.inputRowNames_.size() - signature.args.size()]));

    for (int i = 1; i < signature.args.size(); i++) {
      if (i == 1) {
        // For country code, use a random string generator with ASCII, UTF8
        // specifying max length to be greater than 2 (max length of country
        // code)
        state.customInputGenerators_.emplace_back(
            std::make_shared<RandomInputGenerator<StringView>>(
                seed,
                signature.args[i],
                nullRatio,
                4,
                std::vector<UTF8CharList>{
                    UTF8CharList::ASCII,
                    UTF8CharList::UNICODE_CASE_SENSITIVE}));
      } else {
        // For phoneFormat, use a random string generator with ASCII, UTF8,
        // MATHEMATICAL_SYMBOLS. Specify max length to be greater than 12 to
        // have enough randomization yet not too long
        state.customInputGenerators_.emplace_back(
            std::make_shared<RandomInputGenerator<StringView>>(
                seed,
                signature.args[i],
                nullRatio,
                12,
                std::vector<UTF8CharList>{
                    UTF8CharList::ASCII,
                    UTF8CharList::UNICODE_CASE_SENSITIVE,
                    UTF8CharList::EXTENDED_UNICODE,
                    UTF8CharList::MATHEMATICAL_SYMBOLS}));
      }
      VELOX_CHECK_GE(state.inputRowNames_.size(), signature.args.size() - i);
      inputExpressions.emplace_back(
          std::make_shared<core::FieldAccessTypedExpr>(
              signature.args[i],
              state.inputRowNames_
                  [state.inputRowNames_.size() + i - signature.args.size()]));
    }
  }
  return inputExpressions;
}

std::vector<core::TypedExprPtr> JsonExtractArgValuesGenerator::generate(
    const CallableSignature& signature,
    const VectorFuzzer::Options& options,
    FuzzerGenerator& rng,
    ExpressionFuzzerState& state) {
  VELOX_CHECK_EQ(signature.args.size(), 2);
  std::vector<core::TypedExprPtr> inputExpressions;

  for (auto i = 0; i < signature.args.size(); ++i) {
    state.inputRowTypes_.emplace_back(signature.args[i]);
    state.inputRowNames_.emplace_back(
        fmt::format("c{}", state.inputRowTypes_.size() - 1));
    inputExpressions.push_back(
        std::make_shared<core::FieldAccessTypedExpr>(
            signature.args[i], state.inputRowNames_.back()));
  }

  const auto nullRatio = options.nullRatio;
  const auto seed = rand<uint32_t>(rng);
  const auto maxContainerSize = rand<uint32_t>(rng, 0, 10);
  TypePtr representedType;
  std::vector<variant> mapKeys;
  // 60% of times generate VARCHAR map keys, otherwise generate BIGINT map keys.
  if (coinToss(rng, 0.6)) {
    representedType = facebook::velox::fuzzer::randType(
        rng, jsonScalarTypes(), 3, {VARCHAR()});
    RandomInputGenerator<StringView> mapKeyGenerator{seed, VARCHAR(), 0.0, 10};
    for (auto i = 0; i < maxContainerSize + 2; ++i) {
      mapKeys.push_back(mapKeyGenerator.generate());
    }
  } else {
    representedType = facebook::velox::fuzzer::randType(
        rng, jsonScalarTypes(), 3, {BIGINT()});
    RandomInputGenerator<int64_t> mapKeyGenerator{seed, BIGINT(), 0.0};
    for (auto i = 0; i < maxContainerSize + 2; ++i) {
      mapKeys.push_back(mapKeyGenerator.generate());
    }
  }

  state.customInputGenerators_.emplace_back(
      std::make_shared<fuzzer::JsonInputGenerator>(
          seed,
          signature.args[0],
          nullRatio,
          fuzzer::getRandomInputGenerator(
              seed, representedType, nullRatio, mapKeys, maxContainerSize),
          true));
  state.customInputGenerators_.emplace_back(
      std::make_shared<fuzzer::JsonPathGenerator>(
          seed,
          signature.args[1],
          nullRatio,
          representedType,
          mapKeys,
          maxContainerSize,
          true));
  return inputExpressions;
}

std::vector<core::TypedExprPtr> CastVarcharAndJsonArgValuesGenerator::generate(
    const CallableSignature& signature,
    const VectorFuzzer::Options& options,
    FuzzerGenerator& rng,
    ExpressionFuzzerState& state) {
  VELOX_CHECK_EQ(signature.args.size(), 1);
  populateInputTypesAndNames(signature, state);
  std::vector<core::TypedExprPtr> inputExpressions;

  // Use default input generation for non-varchar inputs.
  if (signature.args[0]->kind() != TypeKind::VARCHAR) {
    state.customInputGenerators_.emplace_back(nullptr);
    inputExpressions.emplace_back(nullptr);
  }
  // Use custom input generation for varchar.
  // This will be used to test casting to primitive and complex types.
  else {
    const auto seed = rand<uint32_t>(rng);
    const auto nullRatio = options.nullRatio;

    // Populate state.customInputGenerators_ and inputExpressions for inputs
    // that require custom input generators. Casting to complex type should
    // require JSON as input.
    if (isJsonType(signature.args[0])) {
      state.customInputGenerators_.emplace_back(
          std::make_shared<fuzzer::JsonInputGenerator>(
              seed,
              signature.args[0],
              nullRatio,
              fuzzer::getRandomInputGenerator(
                  seed, signature.returnType, nullRatio),
              true));
    } else {
      VELOX_CHECK(signature.args[0]->isPrimitiveType());
      state.customInputGenerators_.emplace_back(
          std::make_shared<fuzzer::CastVarcharInputGenerator>(
              seed, signature.args[0], nullRatio, signature.returnType));
    }

    VELOX_CHECK_GE(signature.args.size(), 1);
    inputExpressions.emplace_back(
        std::make_shared<core::FieldAccessTypedExpr>(
            signature.args[0], state.inputRowNames_.back()));
  }

  return inputExpressions;
}

std::vector<core::TypedExprPtr> QDigestArgValuesGenerator::generate(
    const CallableSignature& signature,
    const VectorFuzzer::Options& options,
    FuzzerGenerator& rng,
    ExpressionFuzzerState& state) {
  populateInputTypesAndNames(signature, state);
  const auto seed = rand<uint32_t>(rng);
  const auto nullRatio = options.nullRatio;
  std::vector<core::TypedExprPtr> inputExpressions;
  VELOX_CHECK_GE(signature.args.size(), 2);
  const std::vector<std::string> functionNames = {
      "value_at_quantile",
      "values_at_quantiles",
      "quantile_at_value",
      "scale_qdigest",
      "fb_truncated_mean"};
  if (std::find(functionNames.begin(), functionNames.end(), functionName_) ==
      functionNames.end()) {
    return inputExpressions;
  }
  VELOX_CHECK_GE(state.inputRowNames_.size(), 2);

  const auto& qDigestType = [&signature]() -> TypePtr {
    const auto& typeStr = signature.args[0]->toString();
    if (typeStr.find("QDIGEST(BIGINT)") != std::string::npos) {
      return BIGINT();
    } else if (typeStr.find("QDIGEST(DOUBLE)") != std::string::npos) {
      return DOUBLE();
    } else if (typeStr.find("QDIGEST(REAL)") != std::string::npos) {
      return REAL();
    }
    VELOX_UNREACHABLE("Unsupported QDigest type: {}", typeStr);
  };

  state.customInputGenerators_.emplace_back(
      std::make_shared<fuzzer::QDigestInputGenerator>(
          seed, signature.args[0], nullRatio, qDigestType()));
  VELOX_CHECK_GE(state.inputRowNames_.size(), signature.args.size());
  inputExpressions.emplace_back(
      std::make_shared<core::FieldAccessTypedExpr>(
          signature.args[0],
          signature.args.size() == 1
              ? state.inputRowNames_.back()
              : state.inputRowNames_
                    [state.inputRowNames_.size() - signature.args.size()]));

  // Add null generators for remaining arguments
  for (size_t i = 1; i < signature.args.size(); i++) {
    state.customInputGenerators_.emplace_back(nullptr);
    VELOX_CHECK_GE(state.inputRowNames_.size(), signature.args.size() - i);
    inputExpressions.emplace_back(
        std::make_shared<core::FieldAccessTypedExpr>(
            signature.args[i],
            state.inputRowNames_
                [state.inputRowNames_.size() + i - signature.args.size()]));
  }
  return inputExpressions;
}

std::vector<core::TypedExprPtr> URLArgValuesGenerator::generate(
    const CallableSignature& signature,
    const VectorFuzzer::Options& options,
    FuzzerGenerator& rng,
    ExpressionFuzzerState& state) {
  VELOX_CHECK_GE(signature.args.size(), 1);
  populateInputTypesAndNames(signature, state);
  std::vector<core::TypedExprPtr> inputExpressions;

  const auto seed = rand<uint32_t>(rng);
  const auto nullRatio = options.nullRatio;

  // Only URL part of input should have URL input generation.
  state.customInputGenerators_.emplace_back(
      std::make_shared<fuzzer::URLInputGenerator>(
          seed,
          signature.args[0],
          nullRatio,
          signature.name,
          std::vector<std::string>(
              {"url_extract_query",
               "url_extract_path",
               "url_extract_host",
               "url_extract_protocol"}),
          std::vector<std::string>(
              {"url_extract_path",
               "url_extract_fragment",
               "url_extract_query",
               "url_extract_protocol",
               "url_extract_host"})));

  VELOX_CHECK_GE(signature.args.size(), 1);
  inputExpressions.emplace_back(
      std::make_shared<core::FieldAccessTypedExpr>(
          signature.args[0], state.inputRowNames_.back()));

  // Add null generators for remaining arguments
  for (size_t i = 1; i < signature.args.size(); i++) {
    state.customInputGenerators_.emplace_back(nullptr);
    VELOX_CHECK_GE(state.inputRowNames_.size(), signature.args.size() - i);
    inputExpressions.emplace_back(
        std::make_shared<core::FieldAccessTypedExpr>(
            signature.args[i],
            state.inputRowNames_
                [state.inputRowNames_.size() + i - signature.args.size()]));
  }

  return inputExpressions;
}

std::vector<core::TypedExprPtr> TDigestArgValuesGenerator::generate(
    const CallableSignature& signature,
    const VectorFuzzer::Options& options,
    FuzzerGenerator& rng,
    ExpressionFuzzerState& state) {
  VELOX_CHECK_GE(signature.args.size(), 1);
  populateInputTypesAndNames(signature, state);
  const auto seed = rand<uint32_t>(rng);
  const auto nullRatio = options.nullRatio;
  std::vector<core::TypedExprPtr> inputExpressions;
  const std::vector<std::string> functionNames = {
      "value_at_quantile",
      "values_at_quantiles",
      "scale_tdigest",
      "quantile_at_value",
      "quantiles_at_values",
      "destructure_tdigest",
      "trimmed_mean"};
  if (std::find(functionNames.begin(), functionNames.end(), functionName_) !=
      functionNames.end()) {
    // Handle first argument (TDigest)
    state.customInputGenerators_.emplace_back(
        std::make_shared<fuzzer::TDigestInputGenerator>(
            seed, signature.args[0], nullRatio));
    VELOX_CHECK_GE(state.inputRowNames_.size(), signature.args.size());
    inputExpressions.emplace_back(
        std::make_shared<core::FieldAccessTypedExpr>(
            signature.args[0],
            signature.args.size() == 1
                ? state.inputRowNames_.back()
                : state.inputRowNames_
                      [state.inputRowNames_.size() - signature.args.size()]));
    // Add null generators for remaining arguments
    for (size_t i = 1; i < signature.args.size(); i++) {
      state.customInputGenerators_.emplace_back(nullptr);
      VELOX_CHECK_GE(state.inputRowNames_.size(), signature.args.size() - i);
      inputExpressions.emplace_back(
          std::make_shared<core::FieldAccessTypedExpr>(
              signature.args[i],
              state.inputRowNames_
                  [state.inputRowNames_.size() + i - signature.args.size()]));
    }
  }
  return inputExpressions;
}

std::vector<core::TypedExprPtr> UnifiedDigestArgValuesGenerator::generate(
    const CallableSignature& signature,
    const VectorFuzzer::Options& options,
    FuzzerGenerator& rng,
    ExpressionFuzzerState& state) {
  VELOX_CHECK_GE(signature.args.size(), 1);
  const auto& argType = signature.args[0];

  if (argType->toString().find("TDIGEST") != std::string::npos) {
    return tdigestGenerator_.generate(signature, options, rng, state);

  } else if (argType->toString().find("QDIGEST") != std::string::npos) {
    return qdigestGenerator_.generate(signature, options, rng, state);
  }

  VELOX_UNREACHABLE("Unsupported digest type");
}

std::vector<core::TypedExprPtr>
FbDedupNormalizeTextArgValuesGenerator::generate(
    const CallableSignature& signature,
    const VectorFuzzer::Options& options,
    FuzzerGenerator& rng,
    ExpressionFuzzerState& state) {
  VELOX_CHECK_LE(signature.args.size(), 2);
  populateInputTypesAndNames(signature, state);

  const auto seed = static_cast<size_t>(rand<uint32_t>(rng));
  const auto nullRatio = options.nullRatio;

  // Add custom input generator for the first argument (text)
  state.customInputGenerators_.emplace_back(
      std::make_shared<RandomInputGenerator<StringView>>(
          seed,
          signature.args[0],
          nullRatio,
          20,
          std::vector<UTF8CharList>{
              UTF8CharList::ASCII,
              UTF8CharList::UNICODE_CASE_SENSITIVE,
              UTF8CharList::EXTENDED_UNICODE,
              UTF8CharList::MATHEMATICAL_SYMBOLS},
          RandomStrVariationOptions{
              /*controlCharacterProbability=*/0.5,
              /*escapeStringProbability=*/0.5,
              /*truncateProbability=*/0.2,
          }));

  // Populate inputExpressions_ for the argument that requires custom
  // generation. A nullptr should be added at inputExpressions[i] if the i-th
  // argument does not require custom input generation.
  std::vector<core::TypedExprPtr> inputExpressions{
      signature.args.size(), nullptr};

  // First argument (text) - use FieldAccessTypedExpr
  inputExpressions[0] = std::make_shared<core::FieldAccessTypedExpr>(
      signature.args[0], state.inputRowNames_.back());

  // Second argument (normalization form) - Add an INVALID form
  if (signature.args.size() > 1) {
    // Add null generator for second argument if it exists
    state.customInputGenerators_.emplace_back(nullptr);

    static const std::vector<std::string> testForms = {
        "NFC", "NFD", "NFKC", "NFKD", "INVALID"};

    auto index = rand<uint64_t>(rng, 0, testForms.size() - 1);
    inputExpressions[1] = std::make_shared<core::ConstantTypedExpr>(
        VARCHAR(), variant(testForms[index]));
  }

  return inputExpressions;
}

std::vector<core::TypedExprPtr> AtTimezoneArgValuesGenerator::generate(
    const CallableSignature& signature,
    const VectorFuzzer::Options& options,
    FuzzerGenerator& rng,
    ExpressionFuzzerState& state) {
  VELOX_CHECK_EQ(signature.args.size(), 2);
  populateInputTypesAndNames(signature, state);

  std::vector<core::TypedExprPtr> inputExpressions{
      signature.args.size(), nullptr};

  // First argument - use default generation (TIME WITH TIME ZONE or TIMESTAMP
  // WITH TIME ZONE)
  state.customInputGenerators_.emplace_back(nullptr);
  inputExpressions[0] = std::make_shared<core::FieldAccessTypedExpr>(
      signature.args[0], state.inputRowNames_[state.inputRowNames_.size() - 2]);

  // Second argument - constrain to valid timezone offset formats
  // Valid formats: "+HH:mm" or "-HH:mm" (Presto-compatible)
  // Offset range: -14:00 to +14:00
  // Uses shared timezone generation utility (25% frequently used, 75% random)
  state.customInputGenerators_.emplace_back(nullptr);

  // Generate random timezone offset and convert to string
  int16_t offsetMinutes = generateRandomTimezoneOffset(rng);
  std::string timezoneString = timezoneOffsetToString(offsetMinutes);
  inputExpressions[1] = std::make_shared<core::ConstantTypedExpr>(
      VARCHAR(), variant(timezoneString));

  return inputExpressions;
}

std::vector<core::TypedExprPtr> SetDigestArgValuesGenerator::generate(
    const CallableSignature& signature,
    const VectorFuzzer::Options& options,
    FuzzerGenerator& rng,
    ExpressionFuzzerState& state) {
  populateInputTypesAndNames(signature, state);
  const auto seed = rand<uint32_t>(rng);
  const auto nullRatio = options.nullRatio;
  std::vector<core::TypedExprPtr> inputExpressions;

  VELOX_CHECK_GE(signature.args.size(), 1);
  VELOX_CHECK_LE(signature.args.size(), 2);

  const std::vector<std::string> singleArgFunctions = {
      "hash_counts", "cardinality"};
  const std::vector<std::string> twoArgFunctions = {
      "jaccard_index", "intersection_cardinality"};

  if (std::find(
          singleArgFunctions.begin(),
          singleArgFunctions.end(),
          functionName_) != singleArgFunctions.end()) {
    // Functions with one SetDigest input
    VELOX_CHECK_EQ(signature.args.size(), 1);

    state.customInputGenerators_.emplace_back(
        std::make_shared<fuzzer::SetDigestInputGenerator>(
            seed, signature.args[0], nullRatio));

    VELOX_CHECK_GE(state.inputRowNames_.size(), 1);
    inputExpressions.emplace_back(
        std::make_shared<core::FieldAccessTypedExpr>(
            signature.args[0], state.inputRowNames_.back()));
  } else if (
      std::find(
          twoArgFunctions.begin(), twoArgFunctions.end(), functionName_) !=
      twoArgFunctions.end()) {
    // Functions with two SetDigest inputs
    VELOX_CHECK_EQ(signature.args.size(), 2);

    // First SetDigest input
    state.customInputGenerators_.emplace_back(
        std::make_shared<fuzzer::SetDigestInputGenerator>(
            seed, signature.args[0], nullRatio));

    VELOX_CHECK_GE(state.inputRowNames_.size(), 2);
    inputExpressions.emplace_back(
        std::make_shared<core::FieldAccessTypedExpr>(
            signature.args[0],
            state.inputRowNames_[state.inputRowNames_.size() - 2]));

    // Second SetDigest input
    state.customInputGenerators_.emplace_back(
        std::make_shared<fuzzer::SetDigestInputGenerator>(
            seed + 1, signature.args[1], nullRatio));

    inputExpressions.emplace_back(
        std::make_shared<core::FieldAccessTypedExpr>(
            signature.args[1],
            state.inputRowNames_[state.inputRowNames_.size() - 1]));
  }
  return inputExpressions;
}

#ifdef VELOX_ENABLE_GEO
namespace {

// Generates a random valid S2 cell ID.
// Picks random face (0-5), level (0-30), and Hilbert curve position.
// S2CellId::FromFacePosLevel clamps the position, so any uint64_t is safe.
int64_t randomS2CellId(FuzzerGenerator& rng) {
  auto face = static_cast<int>(rand<uint64_t>(rng, 0, 5));
  auto level = static_cast<int>(rand<uint64_t>(rng, 0, 30));
  auto position = rand<uint64_t>(rng);
  return functions::S2CellOp::cellIdFromFacePositionLevel(
      face, position, level);
}

} // namespace

std::vector<core::TypedExprPtr> S2CellIdArgValuesGenerator::generate(
    const CallableSignature& signature,
    const VectorFuzzer::Options& /*options*/,
    FuzzerGenerator& rng,
    ExpressionFuzzerState& state) {
  VELOX_CHECK_GE(signature.args.size(), 1);
  populateInputTypesAndNames(signature, state);

  std::vector<core::TypedExprPtr> inputExpressions{
      signature.args.size(), nullptr};
  VELOX_CHECK(!inputExpressions.empty());

  // Generate valid constants for each argument based on type:
  // BIGINT arguments are cell IDs, INTEGER arguments are levels (0-30).
  for (size_t i = 0; i < inputExpressions.size(); ++i) {
    state.customInputGenerators_.emplace_back(nullptr);
    if (signature.args.at(i)->kind() == TypeKind::BIGINT) {
      inputExpressions[i] = std::make_shared<core::ConstantTypedExpr>(
          BIGINT(), variant(randomS2CellId(rng)));
    } else if (signature.args.at(i)->kind() == TypeKind::INTEGER) {
      auto level = static_cast<int32_t>(rand<uint64_t>(rng, 0, 30));
      inputExpressions[i] =
          std::make_shared<core::ConstantTypedExpr>(INTEGER(), variant(level));
    }
  }

  return inputExpressions;
}

std::vector<core::TypedExprPtr> S2CellTokenArgValuesGenerator::generate(
    const CallableSignature& signature,
    const VectorFuzzer::Options& /*options*/,
    FuzzerGenerator& rng,
    ExpressionFuzzerState& state) {
  VELOX_CHECK_EQ(signature.args.size(), 1);
  populateInputTypesAndNames(signature, state);

  std::vector<core::TypedExprPtr> inputExpressions{1, nullptr};

  // Generate a random valid cell ID, then convert to token.
  auto token = functions::S2CellOp::toToken(randomS2CellId(rng));
  state.customInputGenerators_.emplace_back(nullptr);
  inputExpressions[0] =
      std::make_shared<core::ConstantTypedExpr>(VARCHAR(), variant(token));

  return inputExpressions;
}
#endif // VELOX_ENABLE_GEO

std::vector<core::TypedExprPtr> InverseFCdfArgValuesGenerator::generate(
    const CallableSignature& signature,
    const VectorFuzzer::Options& options,
    FuzzerGenerator& rng,
    ExpressionFuzzerState& state) {
  VELOX_CHECK_EQ(signature.args.size(), 3);
  populateInputTypesAndNames(signature, state);

  // Draw the upper bound of the degrees of freedom from the powers of ten up
  // to 1e6. A single uniform range up to 1e6 would almost never produce the
  // small values that real queries use.
  const auto maxDegreesOfFreedom = std::pow(10.0, rand<int32_t>(rng, 0, 6));
  const std::vector<double> maxValues{
      maxDegreesOfFreedom,
      maxDegreesOfFreedom,
      1.0,
  };

  const auto firstColumn = state.inputRowNames_.size() - signature.args.size();
  std::vector<core::TypedExprPtr> inputExpressions;
  for (size_t i = 0; i < signature.args.size(); ++i) {
    state.customInputGenerators_.emplace_back(
        std::make_shared<RangeConstrainedGenerator<double>>(
            rand<uint32_t>(rng),
            signature.args[i],
            options.nullRatio,
            0.0,
            maxValues[i]));
    inputExpressions.emplace_back(
        std::make_shared<core::FieldAccessTypedExpr>(
            signature.args[i], state.inputRowNames_[firstColumn + i]));
  }
  return inputExpressions;
}

std::vector<core::TypedExprPtr> LevenshteinArgValuesGenerator::generate(
    const CallableSignature& signature,
    const VectorFuzzer::Options& options,
    FuzzerGenerator& rng,
    ExpressionFuzzerState& state) {
  VELOX_CHECK_GE(signature.args.size(), 2);
  VELOX_CHECK_LE(signature.args.size(), 3);
  populateInputTypesAndNames(signature, state);

  // Keeps the product of the two lengths within 1,000,000, the combined size
  // that Presto's levenshtein_distance accepts.
  static constexpr size_t kMaxLength = 1'000;

  const auto firstColumn = state.inputRowNames_.size() - signature.args.size();
  const auto column = [&](size_t index) {
    return std::make_shared<core::FieldAccessTypedExpr>(
        signature.args[index], state.inputRowNames_[firstColumn + index]);
  };
  const auto boundedString = [&](size_t index) {
    return std::make_shared<RandomInputGenerator<StringView>>(
        rand<uint32_t>(rng),
        signature.args[index],
        options.nullRatio,
        kMaxLength);
  };

  state.customInputGenerators_.emplace_back(boundedString(0));
  state.customInputGenerators_.emplace_back(boundedString(1));
  std::vector<core::TypedExprPtr> inputExpressions{column(0), column(1)};

  // The optional threshold does not affect the cost, so it keeps the default
  // random values.
  if (signature.args.size() == 3) {
    state.customInputGenerators_.emplace_back(nullptr);
    inputExpressions.emplace_back(column(2));
  }
  return inputExpressions;
}

std::vector<core::TypedExprPtr> RoundArgValuesGenerator::generate(
    const CallableSignature& signature,
    const VectorFuzzer::Options& /*options*/,
    FuzzerGenerator& rng,
    ExpressionFuzzerState& /*state*/) {
  VELOX_CHECK_GE(signature.args.size(), 1);
  VELOX_CHECK_LE(signature.args.size(), 2);

  // The fuzzer generates the number.
  std::vector<core::TypedExprPtr> inputExpressions(
      signature.args.size(), nullptr);
  if (signature.args.size() == 2) {
    VELOX_CHECK_EQ(signature.args[1]->kind(), TypeKind::INTEGER);
    // Mostly scales that round typical numbers to one of their digits.
    const int32_t maxScale = coinToss(rng, 0.8) ? 20 : 400;
    inputExpressions[1] = std::make_shared<core::ConstantTypedExpr>(
        signature.args[1], variant(rand<int32_t>(rng, -maxScale, maxScale)));
  }
  return inputExpressions;
}

namespace {

// Adds an input column of 'type' with values of 'generator' and returns an
// expression that reads it.
core::TypedExprPtr addColumn(
    const TypePtr& type,
    AbstractInputGeneratorPtr generator,
    ExpressionFuzzerState& state) {
  state.inputRowTypes_.emplace_back(type);
  state.inputRowNames_.emplace_back(
      fmt::format("c{}", state.inputRowTypes_.size() - 1));
  state.customInputGenerators_.emplace_back(std::move(generator));
  return std::make_shared<core::FieldAccessTypedExpr>(
      type, state.inputRowNames_.back());
}

// Adds an input column of 'type' with values in [min, max] and returns an
// expression that reads it.
template <typename T>
core::TypedExprPtr addRangeConstrainedColumn(
    const TypePtr& type,
    T min,
    T max,
    const VectorFuzzer::Options& options,
    FuzzerGenerator& rng,
    ExpressionFuzzerState& state) {
  return addColumn(
      type,
      std::make_shared<RangeConstrainedGenerator<T>>(
          rand<uint32_t>(rng), type, options.nullRatio, min, max),
      state);
}

} // namespace

std::vector<core::TypedExprPtr> PadArgValuesGenerator::generate(
    const CallableSignature& signature,
    const VectorFuzzer::Options& options,
    FuzzerGenerator& rng,
    ExpressionFuzzerState& state) {
  VELOX_CHECK_GE(signature.args.size(), 2);
  VELOX_CHECK_LE(signature.args.size(), 3);
  const auto& sizeType = signature.args[1];
  VELOX_CHECK_EQ(sizeType->kind(), TypeKind::INTEGER);

  // Mostly sizes close to the lengths of fuzzed strings.
  const int32_t maxSize = coinToss(rng, 0.9) ? 100 : 10'000;

  // The fuzzer generates the string and the pad string.
  std::vector<core::TypedExprPtr> inputExpressions(
      signature.args.size(), nullptr);
  inputExpressions[1] =
      addRangeConstrainedColumn(sizeType, -10, maxSize, options, rng, state);
  return inputExpressions;
}

std::vector<core::TypedExprPtr> RepeatArgValuesGenerator::generate(
    const CallableSignature& signature,
    const VectorFuzzer::Options& options,
    FuzzerGenerator& rng,
    ExpressionFuzzerState& state) {
  VELOX_CHECK_EQ(signature.args.size(), 2);
  const auto& countType = signature.args[1];
  VELOX_CHECK_EQ(countType->kind(), TypeKind::INTEGER);

  // Mostly small counts, so that repeating long strings, e.g. results of lpad,
  // stays within the limit.
  const int32_t maxCount = coinToss(rng, 0.9) ? 10 : 100;

  // The fuzzer generates the string.
  return {
      nullptr,
      addRangeConstrainedColumn(countType, -5, maxCount, options, rng, state)};
}

std::vector<core::TypedExprPtr> ArrayInsertArgValuesGenerator::generate(
    const CallableSignature& signature,
    const VectorFuzzer::Options& options,
    FuzzerGenerator& rng,
    ExpressionFuzzerState& state) {
  VELOX_CHECK_EQ(signature.args.size(), 4);
  const auto& positionType = signature.args[1];
  VELOX_CHECK_EQ(positionType->kind(), TypeKind::INTEGER);
  VELOX_CHECK_EQ(signature.args[3]->kind(), TypeKind::BOOLEAN);

  // Mostly positions within or next to the fuzzed arrays.
  const int32_t maxDistance = coinToss(rng, 0.9)
      ? static_cast<int32_t>(options.containerLength) + 2
      : 100;
  int32_t minPosition = -maxDistance;
  int32_t maxPosition = maxDistance;
  // A test case throws if any row has position 0, so most test cases have
  // positions of one sign.
  if (coinToss(rng, 0.9)) {
    if (coinToss(rng, 0.5)) {
      minPosition = 1;
    } else {
      maxPosition = -1;
    }
  }

  // The legacyNegativeIndex flag comes from a config in Spark, so it is never
  // null. The fuzzer generates the array and the element.
  return {
      nullptr,
      addRangeConstrainedColumn(
          positionType, minPosition, maxPosition, options, rng, state),
      nullptr,
      std::make_shared<core::ConstantTypedExpr>(
          signature.args[3], variant(coinToss(rng, 0.5)))};
}

std::vector<core::TypedExprPtr> TimestampAddDiffArgValuesGenerator::generate(
    const CallableSignature& signature,
    const VectorFuzzer::Options& options,
    FuzzerGenerator& rng,
    ExpressionFuzzerState& state) {
  VELOX_CHECK_EQ(signature.args.size(), 3);
  VELOX_CHECK_EQ(signature.args[0]->kind(), TypeKind::VARCHAR);
  const bool isAdd = signature.name == "timestampadd";

  // Units and the quantities of them in about 200 years. dayofyear comes last
  // since only timestampadd supports it.
  static const std::vector<std::pair<std::string, int64_t>> kUnits{
      {"microsecond", 6'300'000'000'000'000},
      {"millisecond", 6'300'000'000'000},
      {"second", 6'300'000'000},
      {"minute", 105'000'000},
      {"hour", 1'750'000},
      {"day", 73'000},
      {"week", 10'000},
      {"month", 2'400},
      {"quarter", 800},
      {"year", 200},
      {"dayofyear", 73'000}};
  const auto& [unit, maxQuantity] =
      kUnits[rand<size_t>(rng, 0, kUnits.size() - (isAdd ? 1 : 2))];
  std::string unitString;
  for (const auto c : unit) {
    unitString.push_back(
        coinToss(rng, 0.5) ? static_cast<char>(std::toupper(c)) : c);
  }

  // The fuzzer generates the timestamps.
  std::vector<core::TypedExprPtr> inputExpressions{
      std::make_shared<core::ConstantTypedExpr>(
          signature.args[0], variant(unitString)),
      nullptr,
      nullptr};
  if (isAdd && coinToss(rng, 0.9)) {
    const auto& quantityType = signature.args[1];
    if (quantityType->kind() == TypeKind::INTEGER) {
      const auto max = static_cast<int32_t>(
          std::min<int64_t>(maxQuantity, std::numeric_limits<int32_t>::max()));
      inputExpressions[1] = addRangeConstrainedColumn<int32_t>(
          quantityType, -max, max, options, rng, state);
    } else {
      VELOX_CHECK_EQ(quantityType->kind(), TypeKind::BIGINT);
      inputExpressions[1] = addRangeConstrainedColumn<int64_t>(
          quantityType, -maxQuantity, maxQuantity, options, rng, state);
    }
  }
  return inputExpressions;
}

std::vector<core::TypedExprPtr> MaskArgValuesGenerator::generate(
    const CallableSignature& signature,
    const VectorFuzzer::Options& options,
    FuzzerGenerator& rng,
    ExpressionFuzzerState& /*state*/) {
  const auto& args = signature.args;
  VELOX_CHECK_GE(args.size(), 1);
  VELOX_CHECK_LE(args.size(), 5);

  // Characters of 1 to 4 bytes, including the quote and backslash that Spark
  // SQL escapes.
  static const std::vector<std::string> kReplacements{
      "X",
      "x",
      "n",
      "Q",
      "q",
      "0",
      "*",
      "#",
      " ",
      "'",
      "\\",
      "é",
      "Ω",
      "ж",
      "€",
      "い",
      "𝐀",
      "🚀"};

  // The fuzzer generates the string to mask.
  std::vector<core::TypedExprPtr> inputExpressions{nullptr};
  for (auto i = 1; i < args.size(); ++i) {
    VELOX_CHECK_EQ(args[i]->kind(), TypeKind::VARCHAR);
    inputExpressions.push_back(
        std::make_shared<core::ConstantTypedExpr>(
            args[i],
            coinToss(rng, options.nullRatio)
                ? variant::null(TypeKind::VARCHAR)
                : variant(
                      kReplacements[rand<size_t>(
                          rng, 0, kReplacements.size() - 1)])));
  }
  return inputExpressions;
}

namespace {

// Time zones with DST, offsets that are not whole hours, and a zone that
// skipped a day.
const std::vector<std::string>& timeZoneNames() {
  static const std::vector<std::string> kTimeZones{
      "UTC",
      "America/Los_Angeles",
      "America/New_York",
      "America/Sao_Paulo",
      "America/St_Johns",
      "Europe/London",
      "Europe/Berlin",
      "Europe/Moscow",
      "Asia/Shanghai",
      "Asia/Kolkata",
      "Asia/Kathmandu",
      "Asia/Tokyo",
      "Australia/Lord_Howe",
      "Pacific/Apia",
      "Pacific/Chatham",
      "Pacific/Kiritimati",
      "+00:00",
      "-03:30",
      "+05:45",
      "+08:00",
      "-12:00",
      "+14:00"};
  return kTimeZones;
}

} // namespace

std::vector<core::TypedExprPtr> ToJsonArgValuesGenerator::generate(
    const CallableSignature& signature,
    const VectorFuzzer::Options& /*options*/,
    FuzzerGenerator& rng,
    ExpressionFuzzerState& /*state*/) {
  const auto& args = signature.args;
  VELOX_CHECK_GE(args.size(), 1);
  VELOX_CHECK_LE(args.size(), 2);

  // The fuzzer generates the value to convert.
  std::vector<core::TypedExprPtr> inputExpressions{nullptr};
  if (args.size() == 2) {
    VELOX_CHECK_EQ(args[1]->kind(), TypeKind::VARCHAR);
    const auto& timeZones = timeZoneNames();
    inputExpressions.push_back(
        std::make_shared<core::ConstantTypedExpr>(
            args[1],
            variant(timeZones[rand<size_t>(rng, 0, timeZones.size() - 1)])));
  }
  return inputExpressions;
}

std::vector<core::TypedExprPtr> MakeTimestampArgValuesGenerator::generate(
    const CallableSignature& signature,
    const VectorFuzzer::Options& options,
    FuzzerGenerator& rng,
    ExpressionFuzzerState& state) {
  VELOX_CHECK_GE(signature.args.size(), 6);
  VELOX_CHECK_LE(signature.args.size(), 7);
  const auto& args = signature.args;
  for (auto i = 0; i < 5; ++i) {
    VELOX_CHECK_EQ(args[i]->kind(), TypeKind::INTEGER);
  }
  VELOX_CHECK(args[5]->isShortDecimal());
  const auto [precision, scale] = getDecimalPrecisionScale(*args[5]);
  VELOX_CHECK_EQ(scale, 6);

  // Year, month, day, hour and minute.
  std::vector<core::TypedExprPtr> inputExpressions{
      addRangeConstrainedColumn(args[0], 1800, 2200, options, rng, state),
      addRangeConstrainedColumn(args[1], 0, 13, options, rng, state),
      addRangeConstrainedColumn(args[2], 0, 32, options, rng, state),
      addRangeConstrainedColumn(args[3], -1, 24, options, rng, state),
      addRangeConstrainedColumn(args[4], -1, 60, options, rng, state)};

  // Seconds with microseconds in [-1, 61), as far as the precision allows.
  const auto maxSeconds = std::min<int64_t>(
      60'999'999,
      static_cast<int64_t>(DecimalUtil::kPowersOfTen[precision]) - 1);
  inputExpressions.push_back(
      addRangeConstrainedColumn<int64_t>(
          args[5], -1'000'000, maxSeconds, options, rng, state));

  if (args.size() == 7) {
    const auto& kTimeZones = timeZoneNames();
    if (coinToss(rng, 0.5)) {
      inputExpressions.push_back(
          std::make_shared<core::ConstantTypedExpr>(
              args[6],
              variant(
                  kTimeZones[rand<size_t>(rng, 0, kTimeZones.size() - 1)])));
    } else {
      std::vector<variant> timeZones{variant::null(TypeKind::VARCHAR)};
      for (const auto& timeZone : kTimeZones) {
        timeZones.emplace_back(timeZone);
      }
      inputExpressions.push_back(addColumn(
          args[6],
          std::make_shared<SetConstrainedGenerator>(
              rand<uint32_t>(rng), args[6], timeZones),
          state));
    }
  }
  return inputExpressions;
}

namespace {

// A regular expression and its number of capturing groups.
struct Regexp {
  std::string pattern;
  int32_t numGroups;
};

// Regular expressions that RE2 and java.util.regex interpret the same way.
// Leaves out '.', '$', '\s' and '\b', which differ for line terminators or
// non-ASCII characters, and constructs that the engines don't both support,
// e.g. back references, octal escapes, possessive quantifiers, intersections
// of character classes and named groups.
const std::vector<Regexp>& regexps() {
  static const std::vector<Regexp> kRegexps{
      // Literals and escapes.
      {"a", 0},
      {"ab", 0},
      {",", 0},
      {"-", 0},
      {R"(\|)", 0},
      {R"(\.)", 0},
      {R"(\\)", 0},
      {R"(\Q*.\E)", 0},
      {R"(\x41)", 0},
      {"é", 0},
      // Character classes.
      {"[a-c]", 0},
      {"[^a-z]", 0},
      {"[,;]", 0},
      {"[a-c]+", 0},
      {"[^,]+", 0},
      {"[^a]", 0},
      {R"(\d)", 0},
      {R"(\d+)", 0},
      {R"(\D+)", 0},
      {R"(\w+)", 0},
      {R"(\W)", 0},
      {R"(\W+)", 0},
      {R"(\p{L}+)", 0},
      {"[0-9]{2}", 0},
      {"[A-Z][a-z]*", 0},
      // Quantifiers, including lazy ones and ones that match empty strings.
      {"", 0},
      {"a*", 0},
      {"b*", 0},
      {"x*", 0},
      {"a?", 0},
      {"a+?", 0},
      {"[a-z]*?", 0},
      {"a{2}", 0},
      {"a{1,2}", 0},
      {"a{2,}", 0},
      {",*", 0},
      {"[a-c]*", 0},
      // Alternations and anchors.
      {"a|b", 0},
      {"a|", 0},
      {"|a", 0},
      {"ab|a", 0},
      {"^", 0},
      {"^a", 0},
      {"^[a-z]*", 0},
      // Case-insensitive matching of letters without non-ASCII case variants.
      // RE2 matches 'k' with the Kelvin sign, while Java doesn't by default.
      {"(?i)ab", 0},
      {"(?i)[a-c]+", 0},
      // Groups.
      {"(?:ab)+", 0},
      {"(a)", 1},
      {"([a-z]+)", 1},
      {"(x)?", 1},
      {"(a|b)+", 1},
      {"(a*)", 1},
      {"([^,]*),", 1},
      {R"(^(\w+))", 1},
      {"(a)(b)?", 2},
      {R"((\d+)-(\d+))", 2},
      {"((a)b)?", 2},
      {"(a)|(b)", 2},
      {R"((\w)(\w))", 2},
      {"(b*)(a)", 2},
  };
  return kRegexps;
}

// Regular expressions that both RE2 and java.util.regex reject.
const std::vector<std::string>& invalidRegexps() {
  static const std::vector<std::string> kRegexps{
      "(", ")", "[", "*", "+a", "a{2,1}", "\\", "[a-"};
  return kRegexps;
}

// Generates short strings of characters that the patterns of 'regexps()'
// match, including multi-byte characters and line feeds.
class RegexpSubjectGenerator : public AbstractInputGenerator {
 public:
  RegexpSubjectGenerator(size_t seed, const TypePtr& type, double nullRatio)
      : AbstractInputGenerator(seed, type, nullptr, nullRatio) {}

  variant generate() override {
    if (coinToss(rng_, nullRatio_)) {
      return variant::null(type_->kind());
    }
    static const std::vector<std::string> kCharacters{
        "a",
        "b",
        "c",
        "x",
        "A",
        "B",
        "1",
        "2",
        ",",
        ";",
        "-",
        "|",
        ".",
        "*",
        "\\",
        " ",
        "\n",
        "é",
        "日"};
    std::string subject;
    const auto length = rand<int32_t>(rng_, 0, 8);
    for (auto i = 0; i < length; ++i) {
      subject += kCharacters[rand<size_t>(rng_, 0, kCharacters.size() - 1)];
    }
    return variant(subject);
  }
};

template <typename T>
const T& randElement(const std::vector<T>& values, FuzzerGenerator& rng) {
  return values[rand<size_t>(rng, 0, values.size() - 1)];
}

core::TypedExprPtr makeConstant(const TypePtr& type, variant value) {
  return std::make_shared<core::ConstantTypedExpr>(type, std::move(value));
}

// Returns a constant or an input column with values in [min, max] of 'type',
// which is INTEGER or BIGINT.
core::TypedExprPtr makeIntegerArg(
    const TypePtr& type,
    int32_t min,
    int32_t max,
    const VectorFuzzer::Options& options,
    FuzzerGenerator& rng,
    ExpressionFuzzerState& state) {
  const bool constant = coinToss(rng, 0.5);
  if (type->kind() == TypeKind::INTEGER) {
    return constant ? makeConstant(type, variant(rand<int32_t>(rng, min, max)))
                    : addRangeConstrainedColumn<int32_t>(
                          type, min, max, options, rng, state);
  }
  VELOX_CHECK_EQ(type->kind(), TypeKind::BIGINT);
  return constant
      ? makeConstant(type, variant(rand<int64_t>(rng, min, max)))
      : addRangeConstrainedColumn<int64_t>(type, min, max, options, rng, state);
}

} // namespace

std::vector<core::TypedExprPtr> RegexpArgValuesGenerator::generate(
    const CallableSignature& signature,
    const VectorFuzzer::Options& options,
    FuzzerGenerator& rng,
    ExpressionFuzzerState& state) {
  const auto& name = signature.name;
  const auto& args = signature.args;
  VELOX_CHECK_GE(args.size(), 2);
  VELOX_CHECK_LE(args.size(), 4);

  // The fuzzer generates the arguments that are left null, e.g. the string to
  // match half of the time.
  std::vector<core::TypedExprPtr> inputExpressions(args.size(), nullptr);
  if (args[0]->kind() == TypeKind::VARCHAR && coinToss(rng, 0.5)) {
    inputExpressions[0] = addColumn(
        args[0],
        std::make_shared<RegexpSubjectGenerator>(
            rand<uint32_t>(rng), args[0], options.nullRatio),
        state);
  }

  const auto& patternType = args[1];
  if (patternType->kind() != TypeKind::VARCHAR) {
    // A pattern of unknown type, which must be a constant for rlike.
    inputExpressions[1] =
        makeConstant(patternType, variant::null(patternType->kind()));
    return inputExpressions;
  }

  const bool isExtract =
      name == "regexp_extract" || name == "regexp_extract_all";
  const bool isReplace = name == "regexp_replace";
  // The minimum number of groups of the patterns. regexp_extract and
  // regexp_extract_all extract group 1 by default.
  int32_t minGroups = 0;
  if (isExtract) {
    minGroups = args.size() == 2 ? (coinToss(rng, 0.9) ? 1 : 0)
                                 : rand<int32_t>(rng, 0, 2);
  } else if (isReplace) {
    minGroups = rand<int32_t>(rng, 0, 2);
  }
  std::vector<std::string> patterns;
  std::vector<int32_t> patternGroups;
  for (const auto& regexp : regexps()) {
    if (regexp.numGroups >= minGroups) {
      patterns.push_back(regexp.pattern);
      patternGroups.push_back(regexp.numGroups);
    }
  }

  // The number of groups that all patterns have. Velox requires constant
  // patterns for rlike and regexp_extract.
  int32_t numGroups = minGroups;
  if (name == "rlike" || name == "regexp_extract" || coinToss(rng, 0.7)) {
    if (coinToss(rng, options.nullRatio)) {
      inputExpressions[1] =
          makeConstant(patternType, variant::null(TypeKind::VARCHAR));
    } else if (coinToss(rng, 0.02)) {
      // Velox and Spark throw for invalid patterns.
      numGroups = 0;
      inputExpressions[1] = makeConstant(
          patternType, variant(randElement(invalidRegexps(), rng)));
    } else {
      const auto index = rand<size_t>(rng, 0, patterns.size() - 1);
      numGroups = patternGroups[index];
      inputExpressions[1] = makeConstant(patternType, variant(patterns[index]));
    }
  } else {
    std::vector<variant> values;
    if (coinToss(rng, 0.5)) {
      values.push_back(variant::null(TypeKind::VARCHAR));
    }
    const auto numPatterns = rand<int32_t>(rng, 1, 8);
    for (auto i = 0; i < numPatterns; ++i) {
      values.emplace_back(randElement(patterns, rng));
    }
    inputExpressions[1] = addColumn(
        patternType,
        std::make_shared<SetConstrainedGenerator>(
            rand<uint32_t>(rng), patternType, values),
        state);
  }

  if (isExtract && args.size() == 3) {
    // Velox and Spark throw for indices of groups that don't exist if the
    // pattern matches.
    const bool validGroup = coinToss(rng, 0.85);
    inputExpressions[2] = makeIntegerArg(
        args[2],
        validGroup ? 0 : -1,
        validGroup ? numGroups : numGroups + 1,
        options,
        rng,
        state);
  } else if (isReplace) {
    std::vector<std::string> replacements{
        "", "-", "x", "$0", "<$0>", R"(\$)", R"(\\)"};
    if (numGroups >= 1) {
      replacements.insert(replacements.end(), {"$1", "[$1]", "$10"});
    }
    if (numGroups >= 2) {
      replacements.emplace_back("$2$1");
    }
    if (coinToss(rng, 0.02)) {
      // Velox and Spark throw for invalid replacements if the pattern matches.
      replacements = {"$", "\\", "$a", "${g}"};
    }
    const auto& replacementType = args[2];
    if (coinToss(rng, 0.8)) {
      inputExpressions[2] = makeConstant(
          replacementType, variant(randElement(replacements, rng)));
    } else {
      std::vector<variant> values{variant::null(TypeKind::VARCHAR)};
      for (const auto& replacement : replacements) {
        values.emplace_back(replacement);
      }
      inputExpressions[2] = addColumn(
          replacementType,
          std::make_shared<SetConstrainedGenerator>(
              rand<uint32_t>(rng), replacementType, values),
          state);
    }

    if (args.size() == 4) {
      // Spark requires the position to be a constant that is null or >= 1.
      const auto& positionType = args[3];
      VELOX_CHECK_EQ(positionType->kind(), TypeKind::INTEGER);
      inputExpressions[3] = makeConstant(
          positionType,
          coinToss(rng, options.nullRatio)
              ? variant::null(TypeKind::INTEGER)
              : variant(rand<int32_t>(rng, 1, 10)));
    }
  } else if (name == "split" && args.size() == 3) {
    // Limits <= 0 split the string at all matches.
    inputExpressions[2] = makeIntegerArg(args[2], -2, 5, options, rng, state);
  }
  return inputExpressions;
}

namespace {

// Literal characters of like patterns and the strings to match, including
// characters that regular expressions treat specially, line feeds and
// multi-byte characters.
const std::vector<std::string>& likeCharacters() {
  static const std::vector<std::string> kCharacters{
      "a", "b", "A", "1", ".", "*", "[", "(", "$", "^", "|", "\n", "é", "日"};
  return kCharacters;
}

// Generates short strings of the characters of like patterns, including
// wildcards and escape characters.
class LikeSubjectGenerator : public AbstractInputGenerator {
 public:
  LikeSubjectGenerator(size_t seed, const TypePtr& type, double nullRatio)
      : AbstractInputGenerator(seed, type, nullptr, nullRatio) {}

  variant generate() override {
    if (coinToss(rng_, nullRatio_)) {
      return variant::null(type_->kind());
    }
    static const std::vector<std::string> kSpecialCharacters{
        "%", "_", "\\", "#", "/", "!", "'"};
    std::string subject;
    const auto length = rand<int32_t>(rng_, 0, 6);
    for (auto i = 0; i < length; ++i) {
      subject += coinToss(rng_, 0.2) ? randElement(kSpecialCharacters, rng_)
                                     : randElement(likeCharacters(), rng_);
    }
    return variant(subject);
  }
};

// Returns a like pattern of literal characters, wildcards and sequences of
// 'escape' and the character it escapes. Rarely, 'escape' precedes another
// character or ends the pattern, for which Velox and Spark throw.
std::string makeLikePattern(char escape, FuzzerGenerator& rng) {
  const std::string escapeString(1, escape);
  const std::vector<std::string> escaped{"%", "_", escapeString};
  std::string pattern;
  const auto length = rand<int32_t>(rng, 0, 6);
  for (auto i = 0; i < length; ++i) {
    const auto kind = rand<int32_t>(rng, 0, 9);
    std::string part;
    if (kind < 4) {
      part = randElement(likeCharacters(), rng);
    } else if (kind < 8) {
      part = coinToss(rng, 0.6) ? "%" : "_";
    } else {
      part = escapeString + randElement(escaped, rng);
    }
    // A single escape character would escape the next part.
    if (part != escapeString) {
      pattern += part;
    }
  }
  if (coinToss(rng, 0.03)) {
    pattern += escape;
    if (coinToss(rng, 0.5)) {
      pattern += escape == 'a' ? 'b' : 'a';
    }
  }
  return pattern;
}

} // namespace

std::vector<core::TypedExprPtr> LikeArgValuesGenerator::generate(
    const CallableSignature& signature,
    const VectorFuzzer::Options& options,
    FuzzerGenerator& rng,
    ExpressionFuzzerState& state) {
  const auto& args = signature.args;
  VELOX_CHECK_GE(args.size(), 2);
  VELOX_CHECK_LE(args.size(), 3);

  // The fuzzer generates the string to match half of the time.
  std::vector<core::TypedExprPtr> inputExpressions(args.size(), nullptr);
  if (coinToss(rng, 0.5)) {
    inputExpressions[0] = addColumn(
        args[0],
        std::make_shared<LikeSubjectGenerator>(
            rand<uint32_t>(rng), args[0], options.nullRatio),
        state);
  }

  char escape = '\\';
  if (args.size() == 3) {
    static const std::vector<char> kEscapes{
        '\\', '#', '/', '!', '\'', 'a', '%', '_'};
    escape = randElement(kEscapes, rng);
    inputExpressions[2] =
        makeConstant(args[2], variant(std::string(1, escape)));
  }

  inputExpressions[1] = makeConstant(
      args[1],
      coinToss(rng, options.nullRatio) ? variant::null(TypeKind::VARCHAR)
                                       : variant(makeLikePattern(escape, rng)));
  return inputExpressions;
}

namespace {

template <typename T>
std::vector<variant> integralEdgeValues() {
  return {
      variant(T{0}),
      variant(T{1}),
      variant(T{-1}),
      variant(std::numeric_limits<T>::max()),
      variant(std::numeric_limits<T>::min())};
}

template <typename T>
std::vector<variant> floatingPointEdgeValues() {
  return {
      variant(T{0}),
      variant(-T{0}),
      variant(T{1}),
      variant(T{-1}),
      variant(std::numeric_limits<T>::quiet_NaN()),
      variant(std::numeric_limits<T>::infinity()),
      variant(-std::numeric_limits<T>::infinity()),
      variant(std::numeric_limits<T>::max()),
      variant(std::numeric_limits<T>::lowest())};
}

// Returns the unscaled values 0, the smallest and largest magnitudes, and 1 if
// the scale allows it, of either sign.
template <typename T>
std::vector<variant> decimalEdgeValues(const Type& type) {
  const auto [precision, scale] = getDecimalPrecisionScale(type);
  const auto max = static_cast<T>(DecimalUtil::kPowersOfTen[precision] - 1);
  std::vector<variant> values{
      variant(T{0}),
      variant(T{1}),
      variant(T{-1}),
      variant(max),
      variant(-max)};
  if (scale < precision) {
    const auto one = static_cast<T>(DecimalUtil::kPowersOfTen[scale]);
    values.emplace_back(one);
    values.emplace_back(static_cast<T>(-one));
  }
  return values;
}

std::vector<variant> numericEdgeValues(const Type& type) {
  if (type.isShortDecimal()) {
    return decimalEdgeValues<int64_t>(type);
  }
  if (type.isLongDecimal()) {
    return decimalEdgeValues<int128_t>(type);
  }
  switch (type.kind()) {
    case TypeKind::TINYINT:
      return integralEdgeValues<int8_t>();
    case TypeKind::SMALLINT:
      return integralEdgeValues<int16_t>();
    case TypeKind::INTEGER:
      return integralEdgeValues<int32_t>();
    case TypeKind::BIGINT:
      return integralEdgeValues<int64_t>();
    case TypeKind::REAL:
      return floatingPointEdgeValues<float>();
    case TypeKind::DOUBLE:
      return floatingPointEdgeValues<double>();
    default:
      VELOX_UNSUPPORTED("Unsupported type: {}", type.toString());
  }
}

// Generates edge values of a numeric type in a share of 'edgeRatio' of the
// non-null rows, and random values in the others. Random decimals stay within
// their precision.
class NumericEdgeValueGenerator : public AbstractInputGenerator {
 public:
  NumericEdgeValueGenerator(
      size_t seed,
      const TypePtr& type,
      double nullRatio,
      double edgeRatio)
      : AbstractInputGenerator(
            seed,
            type,
            type->isDecimal() ? nullptr
                              : getRandomInputGenerator(seed, type, 0.0),
            nullRatio),
        edgeRatio_{edgeRatio},
        edgeValues_{numericEdgeValues(*type)} {}

  variant generate() override {
    if (coinToss(rng_, nullRatio_)) {
      return variant::null(type_->kind());
    }
    if (coinToss(rng_, edgeRatio_)) {
      return randElement(edgeValues_, rng_);
    }
    if (type_->isShortDecimal()) {
      const auto max = static_cast<int64_t>(
          DecimalUtil::kPowersOfTen[type_->asShortDecimal().precision()] - 1);
      return variant(rand<int64_t>(rng_, -max, max));
    }
    if (type_->isLongDecimal()) {
      return variant(
          rand<int128_t>(rng_) %
          DecimalUtil::kPowersOfTen[type_->asLongDecimal().precision()]);
    }
    return next_->generate();
  }

 private:
  const double edgeRatio_;
  const std::vector<variant> edgeValues_;
};

} // namespace

std::vector<core::TypedExprPtr> ArithmeticArgValuesGenerator::generate(
    const CallableSignature& signature,
    const VectorFuzzer::Options& options,
    FuzzerGenerator& rng,
    ExpressionFuzzerState& state) {
  VELOX_CHECK_GE(signature.args.size(), 1);
  VELOX_CHECK_LE(signature.args.size(), 2);

  // Mostly a few edge values per batch, so that some batches have none.
  const double edgeRatio = coinToss(rng, 0.8) ? 0.05 : 0.5;

  // The fuzzer generates the other half of the arguments, e.g. nested
  // expressions.
  std::vector<core::TypedExprPtr> inputExpressions;
  for (const auto& type : signature.args) {
    inputExpressions.push_back(
        coinToss(rng, 0.5)
            ? addColumn(
                  type,
                  std::make_shared<NumericEdgeValueGenerator>(
                      rand<uint32_t>(rng), type, options.nullRatio, edgeRatio),
                  state)
            : nullptr);
  }
  return inputExpressions;
}

namespace {

// The target types of casts from strings that the cast generator supports.
enum class CastTarget {
  kIntegral,
  kFloatingPoint,
  kDecimal,
  kBoolean,
  kDate,
  kTimestamp,
};

// Returns true for scalar types other than the custom types of the same kind,
// e.g. DATE, TIME and the decimal types.
bool isPlainScalar(const TypePtr& type) {
  return type->isPrimitiveType() &&
      type->equivalent(*createScalarType(type->kind()));
}

std::optional<CastTarget> toCastTarget(const TypePtr& type) {
  if (type->isDecimal()) {
    return CastTarget::kDecimal;
  }
  if (type->isDate()) {
    return CastTarget::kDate;
  }
  // Spark doesn't support the TIME type.
  if (!isPlainScalar(type)) {
    return std::nullopt;
  }
  switch (type->kind()) {
    case TypeKind::TINYINT:
    case TypeKind::SMALLINT:
    case TypeKind::INTEGER:
    case TypeKind::BIGINT:
      return CastTarget::kIntegral;
    case TypeKind::REAL:
    case TypeKind::DOUBLE:
      return CastTarget::kFloatingPoint;
    case TypeKind::BOOLEAN:
      return CastTarget::kBoolean;
    case TypeKind::TIMESTAMP:
      return CastTarget::kTimestamp;
    default:
      return std::nullopt;
  }
}

std::string randomDigits(int32_t length, FuzzerGenerator& rng) {
  std::string digits;
  for (auto i = 0; i < length; ++i) {
    digits += static_cast<char>('0' + rand<int32_t>(rng, 0, 9));
  }
  return digits;
}

std::string randomSign(FuzzerGenerator& rng) {
  const auto sign = rand<int32_t>(rng, 0, 9);
  return sign < 7 ? "" : (sign < 9 ? "-" : "+");
}

// Returns an integer whose digits fit 'kind'.
std::string makeIntegralString(TypeKind kind, FuzzerGenerator& rng) {
  int32_t maxDigits = 18;
  if (kind == TypeKind::TINYINT) {
    maxDigits = 2;
  } else if (kind == TypeKind::SMALLINT) {
    maxDigits = 4;
  } else if (kind == TypeKind::INTEGER) {
    maxDigits = 9;
  }
  return randomSign(rng) + randomDigits(rand<int32_t>(rng, 1, maxDigits), rng);
}

std::string makeFloatingPointString(FuzzerGenerator& rng) {
  auto value = randomSign(rng);
  const bool hasInteger = coinToss(rng, 0.9);
  if (hasInteger) {
    value += randomDigits(rand<int32_t>(rng, 1, 6), rng);
  }
  if (!hasInteger || coinToss(rng, 0.5)) {
    value += "." + randomDigits(rand<int32_t>(rng, hasInteger ? 0 : 1, 6), rng);
  }
  if (coinToss(rng, 0.2)) {
    value += std::string(coinToss(rng, 0.5) ? "e" : "E") + randomSign(rng) +
        randomDigits(rand<int32_t>(rng, 1, 2), rng);
  }
  return value;
}

// Returns a number with at most the integer and fraction digits of 'type'.
std::string makeDecimalString(const Type& type, FuzzerGenerator& rng) {
  const auto [precision, scale] = getDecimalPrecisionScale(type);
  auto value = randomSign(rng);
  const auto integerDigits =
      rand<int32_t>(rng, scale == 0 ? 1 : 0, precision - scale);
  if (integerDigits > 0) {
    value += randomDigits(integerDigits, rng);
  } else if (coinToss(rng, 0.5)) {
    value += "0";
  }
  if (scale > 0 && (integerDigits == 0 || coinToss(rng, 0.5))) {
    value += "." +
        randomDigits(
                 rand<int32_t>(rng, integerDigits == 0 ? 1 : 0, scale), rng);
  }
  return value;
}

std::string makeBooleanString(FuzzerGenerator& rng) {
  static const std::vector<std::string> kValues{
      "t", "true", "y", "yes", "1", "f", "false", "n", "no", "0"};
  auto value = randElement(kValues, rng);
  for (auto& c : value) {
    if (coinToss(rng, 0.2)) {
      c = static_cast<char>(std::toupper(c));
    }
  }
  return value;
}

int32_t daysInMonth(int32_t year, int32_t month) {
  static const std::vector<int32_t> kDays{
      31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
  const bool leap = (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
  return month == 2 && leap ? 29 : kDays[month - 1];
}

// Returns a valid date between the years 1 and 9999. Months and days have one
// digit sometimes.
std::string makeDateString(FuzzerGenerator& rng) {
  const auto year = rand<int32_t>(rng, 1, 9999);
  const auto month = rand<int32_t>(rng, 1, 12);
  const auto day = rand<int32_t>(rng, 1, daysInMonth(year, month));
  auto formatField = [&](int32_t field) {
    return coinToss(rng, 0.1) ? std::to_string(field)
                              : fmt::format("{:02}", field);
  };
  auto value = fmt::format("{:04}-", year);
  value += formatField(month);
  value += "-";
  value += formatField(day);
  return value;
}

// Returns a date, a year or a month. Spark ignores what follows a space or 'T'
// after a date.
std::string makeDateOnlyString(FuzzerGenerator& rng) {
  static const std::vector<std::string> kSuffixes{
      "T", " ", "T10:00:00", " 23:59:59.999999", "T01:02:03Z"};
  const auto value = makeDateString(rng);
  const auto form = rand<int32_t>(rng, 0, 9);
  if (form == 0) {
    return value.substr(0, 4);
  }
  if (form == 1) {
    return value.substr(0, value.rfind('-'));
  }
  if (form == 2) {
    return value + randElement(kSuffixes, rng);
  }
  return value;
}

// Returns a valid timestamp. Spark uses the current date for a time without a
// date, so all timestamps have one.
std::string makeTimestampString(FuzzerGenerator& rng) {
  static const std::vector<std::string> kTimeZones{
      "Z",
      "UTC",
      " UTC",
      "+08:00",
      "-05:30",
      "+00:00",
      " America/New_York",
      "America/Los_Angeles",
      " Asia/Kolkata",
      "Europe/Berlin"};
  auto value = makeDateString(rng);
  if (coinToss(rng, 0.1)) {
    return value;
  }
  value += coinToss(rng, 0.5) ? "T" : " ";
  value += fmt::format(
      "{:02}:{:02}", rand<int32_t>(rng, 0, 23), rand<int32_t>(rng, 0, 59));
  // Spark rejects a time zone after a time without seconds.
  if (coinToss(rng, 0.1)) {
    return value;
  }
  value += fmt::format(":{:02}", rand<int32_t>(rng, 0, 59));
  if (coinToss(rng, 0.4)) {
    value += "." + randomDigits(rand<int32_t>(rng, 1, 6), rng);
  }
  if (coinToss(rng, 0.3)) {
    value += randElement(kTimeZones, rng);
  }
  return value;
}

// Returns strings that are invalid or out of range for 'target', or have
// unusual forms.
const std::vector<std::string>& castEdgeStrings(CastTarget target) {
  static const std::vector<std::string> kIntegral{
      "127",
      "128",
      "-128",
      "-129",
      "32767",
      "32768",
      "-32768",
      "-32769",
      "2147483647",
      "2147483648",
      "-2147483648",
      "-2147483649",
      "9223372036854775807",
      "9223372036854775808",
      "-9223372036854775808",
      "-9223372036854775809",
      "123456789012345678901234567890",
      "007",
      "-0",
      "1.5",
      "-1.5",
      "127.9",
      "128.1",
      ".5",
      "5.",
      "-.9",
      "1e2",
      "1E-2",
      "",
      " ",
      "-",
      "+",
      ".",
      "--1",
      "+-1",
      "1-",
      "1 2",
      "1_000",
      "1,000",
      "0x1A",
      "1L",
      "NaN",
      "Infinity",
      "\uff11\uff12",
      "1.2.3",
      "abc"};
  static const std::vector<std::string> kFloatingPoint{
      "NaN",
      "nan",
      "NAN",
      "Infinity",
      "-Infinity",
      "+Infinity",
      "infinity",
      "INFINITY",
      "inf",
      "-inf",
      "+inf",
      "INF",
      "-0",
      "-0.0",
      "1e39",
      "-1e39",
      "1e-46",
      "3.4028235e38",
      "3.4028236e38",
      "1.4e-45",
      "1e309",
      "-1e309",
      "1e-400",
      "4.9e-324",
      "1.7976931348623157e308",
      "2.2250738585072014E-308",
      "123456789012345678901234567890",
      "0.1234567890123456789",
      "1d",
      "1f",
      "1D",
      "1F",
      "-1.5d",
      "0x1p3",
      "0x1.8p1",
      "0x1.8",
      "1e",
      "e5",
      ".e1",
      "1e+",
      "",
      " ",
      "-",
      ".",
      "1.2.3",
      "1,5",
      "NaNx",
      "Infinityx",
      "abc",
      "\uff11.\uff15"};
  static const std::vector<std::string> kDecimal{
      "NaN",
      "Infinity",
      "-Infinity",
      "1e2",
      "1E-2",
      "-1.5e1",
      "1.5e+3",
      "1e40",
      "-1e40",
      "1e-40",
      "-0",
      "-0.0",
      "00001",
      "+.5",
      "0.5",
      "-0.5",
      "99999999999999999999999999999999999999.5",
      "0.000000000000000000000000000000000000005",
      "",
      " ",
      "-",
      ".",
      "1e",
      "e1",
      "1.2.3",
      "1,5",
      "0x1A",
      "1d",
      "abc",
      "\uff11.\uff15"};
  static const std::vector<std::string> kBoolean{
      "tru",
      "truee",
      "yess",
      "2",
      "-1",
      "01",
      "1.0",
      "0.0",
      "on",
      "off",
      "",
      " ",
      "nan",
      "null",
      "t r u e",
      "\uff54\uff52\uff55\uff45"};
  static const std::vector<std::string> kDate{
      "2019-02-29",
      "2020-02-29",
      "2020-02-30",
      "2020-04-31",
      "2020-13-01",
      "2020-00-01",
      "2020-01-00",
      "2020-01-32",
      "0000-01-01",
      "-0001-01-01",
      "+2020-01-01",
      "10000-01-01",
      "5881580-07-11",
      "5881580-07-12",
      "-5877641-06-23",
      "-5877641-06-22",
      "999-01-01",
      "99999999-01-01",
      "1582-10-10",
      "2020/01/01",
      "2020.01.01",
      "20200101",
      "2020-01-01garbage",
      "2020-01-01 garbage",
      "2020-01-01T",
      "2020-",
      "2020-01-",
      "2020-1",
      "T2020-01-01",
      "",
      " ",
      "-",
      "2020-01-01 10:00:00",
      "2020-01-01T10:00:00Z",
      "\uff12\uff10\uff12\uff10-01-01"};
  static const std::vector<std::string> kTimestamp{
      "2020-01-01 24:00:00",
      "2020-01-01 23:60:00",
      "2020-01-01 23:59:60",
      "2020-01-01 10:00:00.1234567",
      "2020-01-01 10:00:00.123456789",
      "2020-01-01 10:00:00.",
      "2020-01-01 10",
      "2020-01-01 10:00",
      "2020-01-01 1:2:3",
      "2020-1-1 10:00:00",
      "2020-01-01T10:00Z",
      "2020-01-01 10:00:00 garbage",
      "2020-01-01 10:00:00+18:00",
      "2020-01-01 10:00:00-18:00",
      "2020-01-01 10:00:00+19:00",
      "2020-01-01 10:00:00+0800",
      "2020-01-01 10:00:00+8",
      "2020-01-01 10:00:00-08",
      "2020-01-01 10:00:00GMT+8",
      "2020-01-01 10:00:00UTC+08:00",
      "2020-01-01 10:00:00 GMT",
      "2020-01-01 10:00:00 PST",
      "2020-01-01 10:00:00 CET",
      "2020-01-01 10:00:00 Invalid/Zone",
      "2020-01-01 10:00:00America/New_York",
      "2020-02-30 10:00:00",
      "+2020-01-01 10:00:00",
      "-0001-01-01 00:00:00",
      "0000-01-01 00:00:00",
      "10000-01-01 00:00:00",
      "294247-01-10 04:00:54.775807",
      "294247-01-09 00:00:00",
      "-290308-12-21 19:59:05.224192",
      "-290308-12-22 00:00:00",
      "1883-11-18 12:00:00",
      "1883-11-18 11:59:59",
      "1800-01-01 00:00:00",
      "1582-10-10 00:00:00",
      "2020-03-08 02:30:00",
      "2020-11-01 01:30:00",
      "2020",
      "2020-01",
      "2020-01-01T",
      "",
      " ",
      "2020/01/01 10:00:00"};
  switch (target) {
    case CastTarget::kIntegral:
      return kIntegral;
    case CastTarget::kFloatingPoint:
      return kFloatingPoint;
    case CastTarget::kDecimal:
      return kDecimal;
    case CastTarget::kBoolean:
      return kBoolean;
    case CastTarget::kDate:
      return kDate;
    case CastTarget::kTimestamp:
      return kTimestamp;
  }
  VELOX_UNREACHABLE();
}

// Generates strings for a cast to 'toType'. A share of 'edgeRatio' of the
// non-null strings comes from castEdgeStrings().
class CastStringGenerator : public AbstractInputGenerator {
 public:
  CastStringGenerator(
      size_t seed,
      const TypePtr& type,
      const TypePtr& toType,
      CastTarget target,
      double nullRatio,
      double edgeRatio)
      : AbstractInputGenerator(seed, type, nullptr, nullRatio),
        toType_{toType},
        target_{target},
        edgeRatio_{edgeRatio} {}

  variant generate() override {
    if (coinToss(rng_, nullRatio_)) {
      return variant::null(TypeKind::VARCHAR);
    }
    if (coinToss(rng_, edgeRatio_)) {
      return variant(makeEdgeString());
    }
    return variant(addWhitespace(makeString()));
  }

 private:
  std::string makeString() {
    switch (target_) {
      case CastTarget::kIntegral:
        return makeIntegralString(toType_->kind(), rng_);
      case CastTarget::kFloatingPoint:
        return makeFloatingPointString(rng_);
      case CastTarget::kDecimal:
        return makeDecimalString(*toType_, rng_);
      case CastTarget::kBoolean:
        return makeBooleanString(rng_);
      case CastTarget::kDate:
        return makeDateOnlyString(rng_);
      case CastTarget::kTimestamp:
        return makeTimestampString(rng_);
    }
    VELOX_UNREACHABLE();
  }

  std::string makeEdgeString() {
    // Decimals with too many integer digits overflow, and with too many
    // fraction digits round.
    if (target_ == CastTarget::kDecimal && coinToss(rng_, 0.3)) {
      const auto [precision, scale] = getDecimalPrecisionScale(*toType_);
      return coinToss(rng_, 0.5)
          ? "9" + randomDigits(precision - scale, rng_)
          : "0." + randomDigits(scale + rand<int32_t>(rng_, 1, 3), rng_);
    }
    return randElement(castEdgeStrings(target_), rng_);
  }

  // Spark trims ASCII control characters and spaces around the strings, and DEL
  // for some target types. It keeps Unicode spaces.
  std::string addWhitespace(std::string value) {
    static const std::vector<std::string> kWhitespace{
        " ",
        "  ",
        "\t",
        "\n",
        std::string(1, '\0'),
        "\x01",
        "\x1f",
        "\x7f",
        "\u00a0",
        "\u3000"};
    if (coinToss(rng_, 0.1)) {
      value = randElement(kWhitespace, rng_) + value;
    }
    if (coinToss(rng_, 0.1)) {
      value += randElement(kWhitespace, rng_);
    }
    return value;
  }

  const TypePtr toType_;
  const CastTarget target_;
  const double edgeRatio_;
};

// Returns a generator of the argument of a cast from 'fromType' to 'toType',
// or nullptr to let the fuzzer generate it.
std::unique_ptr<AbstractInputGenerator> makeCastInputGenerator(
    size_t seed,
    const TypePtr& fromType,
    const TypePtr& toType,
    double nullRatio,
    double edgeRatio) {
  if (fromType->kind() != toType->kind() &&
      (fromType->isArray() || fromType->isMap() || fromType->isRow())) {
    return nullptr;
  }
  switch (fromType->kind()) {
    case TypeKind::ARRAY: {
      auto elements = makeCastInputGenerator(
          seed + 1,
          fromType->childAt(0),
          toType->childAt(0),
          nullRatio,
          edgeRatio);
      if (elements == nullptr) {
        return nullptr;
      }
      return std::make_unique<RandomInputGenerator<ArrayType>>(
          seed, fromType, nullRatio, 10, std::move(elements));
    }
    case TypeKind::MAP: {
      auto values = makeCastInputGenerator(
          seed + 1,
          fromType->childAt(1),
          toType->childAt(1),
          nullRatio,
          edgeRatio);
      if (values == nullptr) {
        return nullptr;
      }
      return std::make_unique<RandomInputGenerator<MapType>>(
          seed, fromType, nullRatio, 10, nullptr, std::move(values));
    }
    case TypeKind::ROW: {
      if (fromType->size() != toType->size()) {
        return nullptr;
      }
      std::vector<std::unique_ptr<AbstractInputGenerator>> fields;
      bool hasGenerator = false;
      for (uint32_t i = 0; i < fromType->size(); ++i) {
        fields.push_back(makeCastInputGenerator(
            seed + i + 1,
            fromType->childAt(i),
            toType->childAt(i),
            nullRatio,
            edgeRatio));
        hasGenerator |= fields.back() != nullptr;
      }
      if (!hasGenerator) {
        return nullptr;
      }
      return std::make_unique<RandomInputGenerator<RowType>>(
          seed, fromType, std::move(fields), nullRatio);
    }
    default:
      break;
  }
  if (fromType->isDecimal()) {
    return std::make_unique<NumericEdgeValueGenerator>(
        seed, fromType, nullRatio, edgeRatio);
  }
  if (!isPlainScalar(fromType)) {
    return nullptr;
  }
  switch (fromType->kind()) {
    case TypeKind::VARCHAR: {
      const auto target = toCastTarget(toType);
      if (!target.has_value()) {
        return nullptr;
      }
      return std::make_unique<CastStringGenerator>(
          seed, fromType, toType, target.value(), nullRatio, edgeRatio);
    }
    case TypeKind::TINYINT:
    case TypeKind::SMALLINT:
    case TypeKind::INTEGER:
    case TypeKind::BIGINT:
    case TypeKind::REAL:
    case TypeKind::DOUBLE:
      return std::make_unique<NumericEdgeValueGenerator>(
          seed, fromType, nullRatio, edgeRatio);
    default:
      return nullptr;
  }
}

} // namespace

std::vector<core::TypedExprPtr> CastArgValuesGenerator::generate(
    const CallableSignature& signature,
    const VectorFuzzer::Options& options,
    FuzzerGenerator& rng,
    ExpressionFuzzerState& state) {
  VELOX_CHECK_EQ(signature.args.size(), 1);

  // The fuzzer generates the other half of the arguments, e.g. nested
  // expressions.
  if (coinToss(rng, 0.5)) {
    return {nullptr};
  }

  // A third of the batches has no edge values, so that more of them succeed in
  // ANSI mode.
  const auto edgeKind = rand<int32_t>(rng, 0, 2);
  const double edgeRatio = edgeKind == 0 ? 0.0 : (edgeKind == 1 ? 0.02 : 0.3);
  auto generator = makeCastInputGenerator(
      rand<uint32_t>(rng),
      signature.args[0],
      signature.returnType,
      options.nullRatio,
      edgeRatio);
  if (generator == nullptr) {
    return {nullptr};
  }
  return {addColumn(signature.args[0], std::move(generator), state)};
}

} // namespace facebook::velox::fuzzer
