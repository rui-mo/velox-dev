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

#include <cmath>

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
    // Zones with DST, offsets that are not whole hours, and a zone that
    // skipped a day.
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

} // namespace facebook::velox::fuzzer
