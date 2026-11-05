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

#include <gtest/gtest.h>

#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstdlib>
#include <limits>
#include <regex>
#include <unordered_map>
#include <unordered_set>

#include "velox/core/Expressions.h"
#include "velox/core/QueryConfig.h"
#include "velox/expression/fuzzer/ArgValuesGenerators.h"
#include "velox/type/DecimalUtil.h"
#include "velox/type/tz/TimeZoneMap.h"

namespace facebook::velox::fuzzer::test {
namespace {

TEST(ArgValuesGeneratorsTest, inverseFCdf) {
  const CallableSignature signature{
      .name = "inverse_f_cdf",
      .args = {DOUBLE(), DOUBLE(), DOUBLE()},
      .returnType = DOUBLE(),
      .constantArgs = {false, false, false},
  };
  VectorFuzzer::Options options;
  options.nullRatio = 0;

  FuzzerGenerator rng{0};
  ExpressionFuzzerState state{rng, 5};
  InverseFCdfArgValuesGenerator generator;

  for (auto i = 0; i < 100; ++i) {
    state.reset();
    const auto args = generator.generate(signature, options, rng, state);

    // Every argument must read an input column with a custom generator.
    // Arguments left null would be filled with arbitrary expressions, which
    // can produce degrees of freedom large enough to hang the function.
    ASSERT_EQ(args.size(), 3);
    ASSERT_EQ(state.customInputGenerators_.size(), 3);
    for (auto j = 0; j < 3; ++j) {
      const auto field =
          std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(args[j]);
      ASSERT_NE(field, nullptr);
      EXPECT_EQ(field->name(), state.inputRowNames_[j]);
      ASSERT_NE(state.customInputGenerators_[j], nullptr);
    }

    for (auto j = 0; j < 100; ++j) {
      const auto numerator =
          state.customInputGenerators_[0]->generate().value<double>();
      const auto denominator =
          state.customInputGenerators_[1]->generate().value<double>();
      const auto probability =
          state.customInputGenerators_[2]->generate().value<double>();
      EXPECT_GE(numerator, 0);
      EXPECT_LE(numerator, 1'000'000);
      EXPECT_GE(denominator, 0);
      EXPECT_LE(denominator, 1'000'000);
      EXPECT_GE(probability, 0);
      EXPECT_LE(probability, 1);
    }
  }
}

TEST(ArgValuesGeneratorsTest, levenshtein) {
  VectorFuzzer::Options options;
  options.nullRatio = 0;

  FuzzerGenerator rng{0};
  ExpressionFuzzerState state{rng, 5};
  LevenshteinArgValuesGenerator generator;

  for (const auto& args : std::vector<std::vector<TypePtr>>{
           {VARCHAR(), VARCHAR()},
           {VARCHAR(), VARCHAR(), INTEGER()},
       }) {
    const CallableSignature signature{
        .name = "levenshtein",
        .args = args,
        .returnType = INTEGER(),
        .constantArgs = std::vector<bool>(args.size(), false),
    };

    for (auto i = 0; i < 100; ++i) {
      state.reset();
      const auto inputs = generator.generate(signature, options, rng, state);

      // Every argument must read an input column. Arguments left null would be
      // filled with arbitrary expressions, which can produce strings long
      // enough to make the distance computation run for minutes.
      ASSERT_EQ(inputs.size(), args.size());
      ASSERT_EQ(state.customInputGenerators_.size(), args.size());
      for (auto j = 0; j < args.size(); ++j) {
        const auto field =
            std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(
                inputs[j]);
        ASSERT_NE(field, nullptr);
        EXPECT_EQ(field->name(), state.inputRowNames_[j]);
      }

      for (auto j = 0; j < 2; ++j) {
        ASSERT_NE(state.customInputGenerators_[j], nullptr);
        for (auto k = 0; k < 20; ++k) {
          const auto value =
              state.customInputGenerators_[j]->generate().value<std::string>();
          const auto numCharacters =
              std::count_if(value.begin(), value.end(), [](char c) {
                return (c & 0xC0) != 0x80;
              });
          EXPECT_LE(numCharacters, 1'000);
        }
      }
    }
  }
}

TEST(ArgValuesGeneratorsTest, round) {
  VectorFuzzer::Options options;
  FuzzerGenerator rng{0};
  ExpressionFuzzerState state{rng, 5};
  RoundArgValuesGenerator generator;

  const CallableSignature unary{
      .name = "round",
      .args = {DOUBLE()},
      .returnType = DOUBLE(),
      .constantArgs = {false},
  };
  const auto inputs = generator.generate(unary, options, rng, state);
  ASSERT_EQ(inputs.size(), 1);
  EXPECT_EQ(inputs[0], nullptr);

  const CallableSignature binary{
      .name = "round",
      .args = {DOUBLE(), INTEGER()},
      .returnType = DOUBLE(),
      .constantArgs = {false, false},
  };
  bool hasLargeScale = false;
  for (auto i = 0; i < 1'000; ++i) {
    state.reset();
    const auto inputs = generator.generate(binary, options, rng, state);

    // The fuzzer generates the number. Spark requires the scale to be a
    // constant, and fails for scales of large magnitude.
    ASSERT_EQ(inputs.size(), 2);
    EXPECT_EQ(inputs[0], nullptr);
    const auto scale =
        std::dynamic_pointer_cast<const core::ConstantTypedExpr>(inputs[1]);
    ASSERT_NE(scale, nullptr);
    ASSERT_EQ(*scale->type(), *INTEGER());
    const auto value = std::abs(scale->value().value<int32_t>());
    EXPECT_LE(value, 400);
    hasLargeScale |= value > 308;
  }
  // Covers scales whose power of 10 does not fit in a double.
  EXPECT_TRUE(hasLargeScale);
}

TEST(ArgValuesGeneratorsTest, pad) {
  VectorFuzzer::Options options;
  FuzzerGenerator rng{0};
  ExpressionFuzzerState state{rng, 5};
  PadArgValuesGenerator generator;

  for (const auto& args : std::vector<std::vector<TypePtr>>{
           {VARCHAR(), INTEGER()},
           {VARCHAR(), INTEGER(), VARCHAR()},
       }) {
    const CallableSignature signature{
        .name = "lpad",
        .args = args,
        .returnType = VARCHAR(),
        .constantArgs = std::vector<bool>(args.size(), false),
    };

    bool hasNegativeSize = false;
    bool hasLargeSize = false;
    for (auto i = 0; i < 100; ++i) {
      state.reset();
      const auto inputs = generator.generate(signature, options, rng, state);

      // The fuzzer generates the strings, and the size reads an input column.
      ASSERT_EQ(inputs.size(), args.size());
      EXPECT_EQ(inputs[0], nullptr);
      EXPECT_EQ(inputs.back() == nullptr, args.size() == 3);
      const auto size =
          std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(
              inputs[1]);
      ASSERT_NE(size, nullptr);
      ASSERT_EQ(state.inputRowNames_.size(), 1);
      EXPECT_EQ(size->name(), state.inputRowNames_[0]);
      ASSERT_EQ(*state.inputRowTypes_[0], *INTEGER());

      // Velox throws for sizes greater than 1048576.
      ASSERT_EQ(state.customInputGenerators_.size(), 1);
      ASSERT_NE(state.customInputGenerators_[0], nullptr);
      for (auto j = 0; j < 20; ++j) {
        const auto value =
            state.customInputGenerators_[0]->generate().value<int32_t>();
        EXPECT_GE(value, -10);
        EXPECT_LE(value, 10'000);
        hasNegativeSize |= value < 0;
        hasLargeSize |= value > 100;
      }
    }
    EXPECT_TRUE(hasNegativeSize);
    EXPECT_TRUE(hasLargeSize);
  }
}

TEST(ArgValuesGeneratorsTest, repeat) {
  VectorFuzzer::Options options;
  FuzzerGenerator rng{0};
  ExpressionFuzzerState state{rng, 5};
  RepeatArgValuesGenerator generator;
  const CallableSignature signature{
      .name = "repeat",
      .args = {VARCHAR(), INTEGER()},
      .returnType = VARCHAR(),
      .constantArgs = {false, false},
  };

  bool hasNonPositiveCount = false;
  bool hasLargeCount = false;
  for (auto i = 0; i < 100; ++i) {
    state.reset();
    const auto inputs = generator.generate(signature, options, rng, state);

    // The fuzzer generates the string, and the count reads an input column.
    ASSERT_EQ(inputs.size(), 2);
    EXPECT_EQ(inputs[0], nullptr);
    const auto count =
        std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(inputs[1]);
    ASSERT_NE(count, nullptr);
    ASSERT_EQ(state.inputRowNames_.size(), 1);
    EXPECT_EQ(count->name(), state.inputRowNames_[0]);
    ASSERT_EQ(*state.inputRowTypes_[0], *INTEGER());

    // Velox throws for results larger than 1048576 bytes.
    ASSERT_EQ(state.customInputGenerators_.size(), 1);
    ASSERT_NE(state.customInputGenerators_[0], nullptr);
    for (auto j = 0; j < 20; ++j) {
      const auto value =
          state.customInputGenerators_[0]->generate().value<int32_t>();
      EXPECT_GE(value, -5);
      EXPECT_LE(value, 100);
      hasNonPositiveCount |= value <= 0;
      hasLargeCount |= value > 10;
    }
  }
  EXPECT_TRUE(hasNonPositiveCount);
  EXPECT_TRUE(hasLargeCount);
}

TEST(ArgValuesGeneratorsTest, arrayInsert) {
  VectorFuzzer::Options options;
  FuzzerGenerator rng{0};
  ExpressionFuzzerState state{rng, 5};
  ArrayInsertArgValuesGenerator generator;
  const CallableSignature signature{
      .name = "array_insert",
      .args = {ARRAY(BIGINT()), INTEGER(), BIGINT(), BOOLEAN()},
      .returnType = ARRAY(BIGINT()),
      .constantArgs = {false, false, false, true},
  };

  const auto maxNearPosition =
      static_cast<int32_t>(options.containerLength) + 2;
  int32_t numMixedSigns = 0;
  bool hasFarPosition = false;
  std::unordered_set<bool> legacyNegativeIndices;
  for (auto i = 0; i < 100; ++i) {
    state.reset();
    const auto inputs = generator.generate(signature, options, rng, state);

    // The fuzzer generates the array and the element, and the position reads
    // an input column.
    ASSERT_EQ(inputs.size(), 4);
    EXPECT_EQ(inputs[0], nullptr);
    EXPECT_EQ(inputs[2], nullptr);
    const auto legacyNegativeIndex =
        std::dynamic_pointer_cast<const core::ConstantTypedExpr>(inputs[3]);
    ASSERT_NE(legacyNegativeIndex, nullptr);
    ASSERT_FALSE(legacyNegativeIndex->isNull());
    legacyNegativeIndices.insert(
        legacyNegativeIndex->value().value<TypeKind::BOOLEAN>());
    const auto position =
        std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(inputs[1]);
    ASSERT_NE(position, nullptr);
    ASSERT_EQ(state.inputRowNames_.size(), 1);
    EXPECT_EQ(position->name(), state.inputRowNames_[0]);
    ASSERT_EQ(*state.inputRowTypes_[0], *INTEGER());

    // Velox throws for results of more than 10'000 elements.
    ASSERT_EQ(state.customInputGenerators_.size(), 1);
    ASSERT_NE(state.customInputGenerators_[0], nullptr);
    bool hasPositive = false;
    bool hasNonPositive = false;
    for (auto j = 0; j < 20; ++j) {
      const auto value =
          state.customInputGenerators_[0]->generate().value<int32_t>();
      EXPECT_GE(value, -100);
      EXPECT_LE(value, 100);
      hasPositive |= value > 0;
      hasNonPositive |= value <= 0;
      hasFarPosition |= std::abs(value) > maxNearPosition;
    }
    numMixedSigns += hasPositive && hasNonPositive;
  }
  // Most test cases have positions of one sign, since a test case throws if
  // any row has position 0.
  EXPECT_GT(numMixedSigns, 0);
  EXPECT_LT(numMixedSigns, 30);
  EXPECT_TRUE(hasFarPosition);
  EXPECT_EQ(legacyNegativeIndices.size(), 2);
}

TEST(ArgValuesGeneratorsTest, timestampAddDiff) {
  VectorFuzzer::Options options;
  FuzzerGenerator rng{0};
  ExpressionFuzzerState state{rng, 5};
  TimestampAddDiffArgValuesGenerator generator;
  // Average length of each unit in days.
  const std::unordered_map<std::string, double> kDaysPerUnit{
      {"microsecond", 1.0 / 86'400'000'000},
      {"millisecond", 1.0 / 86'400'000},
      {"second", 1.0 / 86'400},
      {"minute", 1.0 / 1'440},
      {"hour", 1.0 / 24},
      {"day", 1},
      {"dayofyear", 1},
      {"week", 7},
      {"month", 365.25 / 12},
      {"quarter", 365.25 / 4},
      {"year", 365.25}};

  for (const auto& signature : std::vector<CallableSignature>{
           {.name = "timestampadd",
            .args = {VARCHAR(), INTEGER(), TIMESTAMP()},
            .returnType = TIMESTAMP(),
            .constantArgs = {true, false, false}},
           {.name = "timestampadd",
            .args = {VARCHAR(), BIGINT(), TIMESTAMP()},
            .returnType = TIMESTAMP(),
            .constantArgs = {true, false, false}},
           {.name = "timestampdiff",
            .args = {VARCHAR(), TIMESTAMP(), TIMESTAMP()},
            .returnType = BIGINT(),
            .constantArgs = {true, false, false}}}) {
    SCOPED_TRACE(signature.toString());
    const bool isAdd = signature.name == "timestampadd";
    std::unordered_set<std::string> units;
    bool hasUpperCase = false;
    int32_t numRandomQuantities = 0;
    for (auto i = 0; i < 200; ++i) {
      state.reset();
      const auto inputs = generator.generate(signature, options, rng, state);

      // The unit is a constant, and the fuzzer generates the timestamps.
      ASSERT_EQ(inputs.size(), 3);
      EXPECT_EQ(inputs[2], nullptr);
      const auto unitInput =
          std::dynamic_pointer_cast<const core::ConstantTypedExpr>(inputs[0]);
      ASSERT_NE(unitInput, nullptr);
      ASSERT_FALSE(unitInput->isNull());
      const auto& unitString = unitInput->value().value<TypeKind::VARCHAR>();
      std::string unit;
      for (const auto c : unitString) {
        hasUpperCase |= std::isupper(c) != 0;
        unit.push_back(static_cast<char>(std::tolower(c)));
      }
      ASSERT_TRUE(kDaysPerUnit.contains(unit)) << unitString;
      units.insert(unit);
      if (!isAdd) {
        EXPECT_EQ(inputs[1], nullptr);
        continue;
      }
      if (inputs[1] == nullptr) {
        ++numRandomQuantities;
        continue;
      }

      // Quantities move the fuzzed timestamps by up to about 200 years, which
      // keeps results in range.
      const auto quantity =
          std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(
              inputs[1]);
      ASSERT_NE(quantity, nullptr);
      ASSERT_EQ(state.inputRowNames_.size(), 1);
      EXPECT_EQ(quantity->name(), state.inputRowNames_[0]);
      ASSERT_EQ(*state.inputRowTypes_[0], *signature.args[1]);
      ASSERT_EQ(state.customInputGenerators_.size(), 1);
      ASSERT_NE(state.customInputGenerators_[0], nullptr);
      for (auto j = 0; j < 20; ++j) {
        const auto value = state.customInputGenerators_[0]->generate();
        const double days = kDaysPerUnit.at(unit) *
            (signature.args[1]->kind() == TypeKind::INTEGER
                 ? value.value<int32_t>()
                 : value.value<int64_t>());
        EXPECT_LE(std::abs(days), 200 * 365.25) << unitString;
      }
    }
    EXPECT_EQ(units.contains("dayofyear"), isAdd);
    EXPECT_EQ(units.size(), isAdd ? 11 : 10);
    EXPECT_TRUE(hasUpperCase);
    if (isAdd) {
      // Random quantities test overflows.
      EXPECT_GT(numRandomQuantities, 0);
      EXPECT_LT(numRandomQuantities, 50);
    }
  }
}

TEST(ArgValuesGeneratorsTest, mask) {
  VectorFuzzer::Options options;
  options.nullRatio = 0.2;
  FuzzerGenerator rng{0};
  ExpressionFuzzerState state{rng, 5};
  MaskArgValuesGenerator generator;
  for (auto numArgs = 1; numArgs <= 5; ++numArgs) {
    const CallableSignature signature{
        .name = "mask",
        .args = std::vector<TypePtr>(numArgs, VARCHAR()),
        .returnType = VARCHAR(),
        .constantArgs = std::vector<bool>(numArgs, false),
    };
    SCOPED_TRACE(signature.toString());
    int32_t numNulls = 0;
    std::unordered_set<std::string> replacements;
    for (auto i = 0; i < 100; ++i) {
      state.reset();
      const auto inputs = generator.generate(signature, options, rng, state);

      // The fuzzer generates the string to mask. Spark requires each
      // replacement to be a constant NULL or single character.
      ASSERT_EQ(inputs.size(), numArgs);
      EXPECT_EQ(inputs[0], nullptr);
      for (auto j = 1; j < numArgs; ++j) {
        const auto replacement =
            std::dynamic_pointer_cast<const core::ConstantTypedExpr>(inputs[j]);
        ASSERT_NE(replacement, nullptr);
        if (replacement->isNull()) {
          ++numNulls;
          continue;
        }
        const auto& value = replacement->value().value<TypeKind::VARCHAR>();
        ASSERT_GE(value.size(), 1);
        ASSERT_LE(value.size(), 4);
        EXPECT_EQ(
            std::count_if(
                value.begin(),
                value.end(),
                [](char c) { return (c & 0xC0) != 0x80; }),
            1)
            << value;
        replacements.insert(value);
      }
    }
    if (numArgs > 1) {
      EXPECT_GT(numNulls, 0);
      EXPECT_GT(replacements.size(), 10);
      // Includes supplementary characters, which have 4 bytes.
      EXPECT_TRUE(
          std::any_of(
              replacements.begin(),
              replacements.end(),
              [](const auto& replacement) { return replacement.size() == 4; }));
    }
  }
}

TEST(ArgValuesGeneratorsTest, toJson) {
  VectorFuzzer::Options options;
  FuzzerGenerator rng{0};
  ExpressionFuzzerState state{rng, 5};
  ToJsonArgValuesGenerator generator;
  const CallableSignature signature{
      .name = "to_json",
      .args = {ARRAY(TIMESTAMP()), VARCHAR()},
      .returnType = VARCHAR(),
      .constantArgs = {false, false},
  };
  std::unordered_set<std::string> timeZones;
  for (auto i = 0; i < 100; ++i) {
    state.reset();
    const auto inputs = generator.generate(signature, options, rng, state);

    // The fuzzer generates the value, and Spark requires a constant time zone.
    ASSERT_EQ(inputs.size(), 2);
    EXPECT_EQ(inputs[0], nullptr);
    const auto timeZone =
        std::dynamic_pointer_cast<const core::ConstantTypedExpr>(inputs[1]);
    ASSERT_NE(timeZone, nullptr);
    ASSERT_FALSE(timeZone->isNull());
    const auto& name = timeZone->value().value<TypeKind::VARCHAR>();
    EXPECT_NO_THROW(tz::locateZone(name)) << name;
    timeZones.insert(name);
  }
  EXPECT_GT(timeZones.size(), 10);
}

TEST(ArgValuesGeneratorsTest, regexp) {
  VectorFuzzer::Options options;
  FuzzerGenerator rng{0};
  ExpressionFuzzerState state{rng, 5};
  RegexpArgValuesGenerator generator;
  const auto maxCompiledRegexes =
      core::QueryConfig({}).exprMaxCompiledRegexes();

  for (const auto& [name, args] :
       std::vector<std::pair<std::string, std::vector<TypePtr>>>{
           {"rlike", {VARCHAR(), VARCHAR()}},
           {"regexp_extract", {VARCHAR(), VARCHAR(), INTEGER()}},
           {"regexp_extract_all", {VARCHAR(), VARCHAR(), BIGINT()}},
           {"regexp_instr", {VARCHAR(), VARCHAR()}},
           {"regexp_replace", {VARCHAR(), VARCHAR(), VARCHAR(), INTEGER()}},
           {"split", {VARCHAR(), VARCHAR(), INTEGER()}},
       }) {
    const CallableSignature signature{
        .name = name,
        .args = args,
        .returnType = VARCHAR(),
        .constantArgs = std::vector<bool>(args.size(), false),
    };
    // Velox requires constant patterns for rlike and regexp_extract.
    const bool constantPattern = name == "rlike" || name == "regexp_extract";
    bool hasColumnPattern = false;
    for (auto i = 0; i < 100; ++i) {
      state.reset();
      const auto inputs = generator.generate(signature, options, rng, state);

      // The fuzzer may generate the string to match, but no other argument.
      ASSERT_EQ(inputs.size(), args.size());
      for (auto j = 1; j < args.size(); ++j) {
        ASSERT_NE(inputs[j], nullptr);
      }

      const auto pattern =
          std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(
              inputs[1]);
      if (pattern == nullptr) {
        EXPECT_NE(
            std::dynamic_pointer_cast<const core::ConstantTypedExpr>(inputs[1]),
            nullptr);
      } else {
        EXPECT_FALSE(constantPattern) << name;
        hasColumnPattern = true;

        // Velox throws if a function compiles more patterns than the limit.
        const auto column = std::find(
            state.inputRowNames_.begin(),
            state.inputRowNames_.end(),
            pattern->name());
        ASSERT_NE(column, state.inputRowNames_.end());
        const auto& patternGenerator =
            state.customInputGenerators_[column - state.inputRowNames_.begin()];
        ASSERT_NE(patternGenerator, nullptr);
        std::unordered_set<std::string> patterns;
        for (auto j = 0; j < 1'000; ++j) {
          const auto value = patternGenerator->generate();
          if (!value.isNull()) {
            patterns.insert(value.value<std::string>());
          }
        }
        EXPECT_LT(patterns.size(), maxCompiledRegexes);
      }

      if (name == "regexp_replace") {
        // Spark requires the position to be a constant that is null or >= 1.
        const auto position =
            std::dynamic_pointer_cast<const core::ConstantTypedExpr>(inputs[3]);
        ASSERT_NE(position, nullptr);
        if (!position->value().isNull()) {
          EXPECT_GE(position->value().value<int32_t>(), 1);
        }
      }
    }
    EXPECT_EQ(hasColumnPattern, !constantPattern) << name;
  }
}

TEST(ArgValuesGeneratorsTest, like) {
  VectorFuzzer::Options options;
  options.nullRatio = 0.1;
  FuzzerGenerator rng{0};
  ExpressionFuzzerState state{rng, 5};
  LikeArgValuesGenerator generator;
  for (auto numArgs = 2; numArgs <= 3; ++numArgs) {
    std::vector<bool> constantArgs(numArgs, true);
    constantArgs[0] = false;
    const CallableSignature signature{
        .name = "like",
        .args = std::vector<TypePtr>(numArgs, VARCHAR()),
        .returnType = BOOLEAN(),
        .constantArgs = constantArgs,
    };
    SCOPED_TRACE(signature.toString());
    std::unordered_set<char> escapes;
    int32_t numWildcards = 0;
    int32_t numEscaped = 0;
    int32_t numSubjectColumns = 0;
    for (auto i = 0; i < 100; ++i) {
      state.reset();
      const auto inputs = generator.generate(signature, options, rng, state);
      ASSERT_EQ(inputs.size(), numArgs);
      if (inputs[0] != nullptr) {
        ++numSubjectColumns;
      }

      // Spark requires an escape argument to be a single-character literal,
      // and Velox only supports ASCII ones. Spark escapes with '\' by default.
      char escape = '\\';
      if (numArgs == 3) {
        const auto escapeArg =
            std::dynamic_pointer_cast<const core::ConstantTypedExpr>(inputs[2]);
        ASSERT_NE(escapeArg, nullptr);
        ASSERT_FALSE(escapeArg->isNull());
        const auto& value = escapeArg->value().value<TypeKind::VARCHAR>();
        ASSERT_EQ(value.size(), 1);
        ASSERT_LT(static_cast<unsigned char>(value[0]), 0x80);
        escape = value[0];
        escapes.insert(escape);
      }

      // Velox requires a constant pattern.
      const auto pattern =
          std::dynamic_pointer_cast<const core::ConstantTypedExpr>(inputs[1]);
      ASSERT_NE(pattern, nullptr);
      if (pattern->isNull()) {
        continue;
      }
      const auto& value = pattern->value().value<TypeKind::VARCHAR>();
      for (auto j = 0; j < value.size(); ++j) {
        if (value[j] == escape && j + 1 < value.size()) {
          ++numEscaped;
          ++j;
        } else if (value[j] == '%' || value[j] == '_') {
          ++numWildcards;
        }
      }
    }
    EXPECT_GT(numSubjectColumns, 20);
    EXPECT_GT(numWildcards, 50);
    EXPECT_GT(numEscaped, 10);
    if (numArgs == 3) {
      EXPECT_GT(escapes.size(), 4);
    }
  }
}

TEST(ArgValuesGeneratorsTest, arithmetic) {
  VectorFuzzer::Options options;
  options.nullRatio = 0.1;
  FuzzerGenerator rng{0};
  ExpressionFuzzerState state{rng, 5};
  ArithmeticArgValuesGenerator generator;

  // The values that divide by zero or overflow.
  const std::vector<std::pair<TypePtr, std::vector<variant>>> kTypes{
      {TINYINT(),
       {variant(int8_t{0}), variant(int8_t{-1}), variant(int8_t{-128})}},
      {BIGINT(),
       {variant(int64_t{0}),
        variant(int64_t{-1}),
        variant(std::numeric_limits<int64_t>::min())}},
      {DOUBLE(), {variant(0.0)}},
      {DECIMAL(10, 2),
       {variant(int64_t{0}),
        variant(int64_t{-100}),
        variant(int64_t{-9'999'999'999})}},
      {DECIMAL(38, 0),
       {variant(int128_t{0}),
        variant(int128_t{-1}),
        variant(-DecimalUtil::kPowersOfTen[38] + 1)}},
  };
  for (const auto& [type, edgeValues] : kTypes) {
    SCOPED_TRACE(type->toString());
    const CallableSignature signature{
        .name = "div",
        .args = {type, type},
        .returnType = BIGINT(),
        .constantArgs = {false, false},
    };
    int32_t numColumns = 0;
    std::vector<variant> values;
    bool hasNaN = false;
    for (auto i = 0; i < 100; ++i) {
      state.reset();
      const auto inputs = generator.generate(signature, options, rng, state);

      // The fuzzer generates the arguments that don't read an input column.
      ASSERT_EQ(inputs.size(), 2);
      ASSERT_EQ(
          state.customInputGenerators_.size(), state.inputRowNames_.size());
      for (auto j = 0; j < inputs.size(); ++j) {
        if (inputs[j] == nullptr) {
          continue;
        }
        const auto field =
            std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(
                inputs[j]);
        ASSERT_NE(field, nullptr);
        ++numColumns;
        const auto it = std::find(
            state.inputRowNames_.begin(),
            state.inputRowNames_.end(),
            field->name());
        ASSERT_NE(it, state.inputRowNames_.end());
        const auto index = it - state.inputRowNames_.begin();
        ASSERT_EQ(*state.inputRowTypes_[index], *type);
        ASSERT_NE(state.customInputGenerators_[index], nullptr);
        for (auto k = 0; k < 100; ++k) {
          const auto value = state.customInputGenerators_[index]->generate();
          if (value.isNull()) {
            continue;
          }
          if (type->isShortDecimal()) {
            EXPECT_LT(std::abs(value.value<int64_t>()), 10'000'000'000);
          } else if (type->isLongDecimal()) {
            EXPECT_LT(value.value<int128_t>(), DecimalUtil::kPowersOfTen[38]);
            EXPECT_GT(value.value<int128_t>(), -DecimalUtil::kPowersOfTen[38]);
          } else if (type->kind() == TypeKind::DOUBLE) {
            hasNaN |= std::isnan(value.value<double>());
          }
          values.push_back(value);
        }
      }
    }
    EXPECT_GT(numColumns, 70);
    EXPECT_LT(numColumns, 130);
    for (const auto& edgeValue : edgeValues) {
      EXPECT_NE(
          std::find(values.begin(), values.end(), edgeValue), values.end())
          << edgeValue.toJson(type);
    }
    EXPECT_EQ(hasNaN, type->kind() == TypeKind::DOUBLE);
  }
}

// Adds the strings in 'value' to 'strings'. Skips the keys of maps.
void collectStrings(const variant& value, std::vector<std::string>& strings) {
  if (value.isNull()) {
    return;
  }
  switch (value.kind()) {
    case TypeKind::VARCHAR:
      strings.push_back(value.value<TypeKind::VARCHAR>());
      break;
    case TypeKind::ARRAY:
      for (const auto& element : value.array()) {
        collectStrings(element, strings);
      }
      break;
    case TypeKind::MAP:
      for (const auto& [_, mapValue] : value.map()) {
        collectStrings(mapValue, strings);
      }
      break;
    case TypeKind::ROW:
      for (const auto& field : value.row()) {
        collectStrings(field, strings);
      }
      break;
    default:
      VELOX_FAIL("Unexpected value: {}", value.toJsonUnsafe());
  }
}

TEST(ArgValuesGeneratorsTest, cast) {
  VectorFuzzer::Options options;
  options.nullRatio = 0.1;
  FuzzerGenerator rng{0};
  ExpressionFuzzerState state{rng, 5};
  CastArgValuesGenerator generator;

  // Generates the argument of a cast 100 times. Returns the number of input
  // columns and 100 values of each.
  auto generate = [&](const TypePtr& fromType, const TypePtr& toType) {
    const CallableSignature signature{
        .name = "cast",
        .args = {fromType},
        .returnType = toType,
        .constantArgs = {false},
    };
    int32_t numColumns = 0;
    std::vector<variant> values;
    for (auto i = 0; i < 100; ++i) {
      state.reset();
      const auto inputs = generator.generate(signature, options, rng, state);
      EXPECT_EQ(inputs.size(), 1);
      if (inputs.empty() || inputs[0] == nullptr) {
        EXPECT_TRUE(state.inputRowTypes_.empty());
        continue;
      }
      const auto field =
          std::dynamic_pointer_cast<const core::FieldAccessTypedExpr>(
              inputs[0]);
      EXPECT_NE(field, nullptr);
      EXPECT_EQ(state.inputRowNames_.size(), 1);
      EXPECT_EQ(state.customInputGenerators_.size(), 1);
      if (field == nullptr || state.inputRowNames_.size() != 1) {
        continue;
      }
      ++numColumns;
      EXPECT_EQ(state.inputRowNames_[0], field->name());
      EXPECT_EQ(*state.inputRowTypes_[0], *fromType);
      for (auto j = 0; j < 100; ++j) {
        values.push_back(state.customInputGenerators_[0]->generate());
      }
    }
    return std::make_pair(numColumns, values);
  };

  // Strings look like values of the target type, after trimming whitespace,
  // in most rows. Patterns match the plain strings of the generator.
  const std::string kTimestamp =
      "[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}([T ][0-9]{2}:[0-9]{2}(:[0-9]{2}"
      "(\\.[0-9]{1,6})?(Z|UTC| UTC|[+-][0-9]{2}:[0-9]{2}| ?[a-z]+/[a-z_]+)?)?)?";
  const std::vector<std::pair<TypePtr, std::string>> kStringTargets{
      {TINYINT(), "[+-]?[0-9]{1,2}"},
      {INTEGER(), "[+-]?[0-9]{1,9}"},
      {BIGINT(), "[+-]?[0-9]{1,18}"},
      {REAL(), "[+-]?([0-9]+(\\.[0-9]*)?|\\.[0-9]+)(e[+-]?[0-9]{1,2})?"},
      {DOUBLE(), "[+-]?([0-9]+(\\.[0-9]*)?|\\.[0-9]+)(e[+-]?[0-9]{1,2})?"},
      {DECIMAL(6, 0), "[+-]?[0-9]{1,6}"},
      {DECIMAL(18, 6), "[+-]?([0-9]{1,12}(\\.[0-9]{0,6})?|0?\\.[0-9]{1,6})"},
      {DECIMAL(38, 38), "[+-]?0?\\.[0-9]{1,38}"},
      {BOOLEAN(), "t|true|y|yes|1|f|false|n|no|0"},
      {DATE(), "[0-9]{4}(-[0-9]{1,2}(-[0-9]{1,2}([T ].*)?)?)?"},
      {TIMESTAMP(), kTimestamp},
  };
  for (const auto& [toType, pattern] : kStringTargets) {
    SCOPED_TRACE(toType->toString());
    const auto [numColumns, values] = generate(VARCHAR(), toType);
    EXPECT_GT(numColumns, 30);
    EXPECT_LT(numColumns, 70);
    std::vector<std::string> strings;
    for (const auto& value : values) {
      collectStrings(value, strings);
    }
    const std::regex regex{pattern, std::regex::icase};
    int32_t numMatches = 0;
    bool hasWhitespace = false;
    for (const auto& string : strings) {
      const auto begin = string.find_first_not_of(" \t\n");
      const auto trimmed = begin == std::string::npos
          ? std::string()
          : string.substr(begin, string.find_last_not_of(" \t\n") - begin + 1);
      hasWhitespace |= trimmed.size() < string.size();
      numMatches += std::regex_match(trimmed, regex) ? 1 : 0;
    }
    EXPECT_GT(numMatches, strings.size() / 2);
    // Some batches have edge values.
    EXPECT_GT(strings.size() - numMatches, 10);
    EXPECT_TRUE(hasWhitespace);
  }

  // Containers contain such strings.
  const std::vector<std::tuple<TypePtr, TypePtr, std::string>> kContainers{
      {ARRAY(VARCHAR()),
       ARRAY(DATE()),
       "[0-9]{4}(-[0-9]{1,2}(-[0-9]{1,2}([T ].*)?)?)?"},
      {MAP(VARCHAR(), VARCHAR()),
       MAP(VARCHAR(), BOOLEAN()),
       "t|true|y|yes|1|f|false|n|no|0"},
      {ROW({VARCHAR()}), ROW({INTEGER()}), "[+-]?[0-9]{1,9}"},
  };
  for (const auto& [fromType, toType, pattern] : kContainers) {
    SCOPED_TRACE(fromType->toString());
    const auto [numColumns, values] = generate(fromType, toType);
    EXPECT_GT(numColumns, 30);
    std::vector<std::string> strings;
    for (const auto& value : values) {
      collectStrings(value, strings);
    }
    const std::regex regex{pattern, std::regex::icase};
    const auto numMatches =
        std::count_if(strings.begin(), strings.end(), [&](const auto& string) {
          return std::regex_match(string, regex);
        });
    EXPECT_GT(strings.size(), 1'000);
    EXPECT_GT(numMatches, strings.size() / 2);
  }

  // Numbers include the edge values of their type.
  const std::vector<std::tuple<TypePtr, TypePtr, variant>> kNumbers{
      {DOUBLE(), INTEGER(), variant(std::numeric_limits<double>::infinity())},
      {BIGINT(), TINYINT(), variant(std::numeric_limits<int64_t>::max())},
      {DECIMAL(38, 18), BIGINT(), variant(-DecimalUtil::kPowersOfTen[38] + 1)},
  };
  for (const auto& [fromType, toType, edgeValue] : kNumbers) {
    SCOPED_TRACE(fromType->toString());
    const auto [numColumns, values] = generate(fromType, toType);
    EXPECT_GT(numColumns, 30);
    EXPECT_NE(std::find(values.begin(), values.end(), edgeValue), values.end());
    if (fromType->kind() == TypeKind::DOUBLE) {
      EXPECT_TRUE(std::any_of(values.begin(), values.end(), [](const auto& v) {
        return !v.isNull() && std::isnan(v.template value<double>());
      }));
    }
  }

  // The fuzzer generates the other arguments, e.g. of casts to types that Spark
  // doesn't support, or from types without edge values.
  const std::vector<std::pair<TypePtr, TypePtr>> kOthers{
      {VARCHAR(), TIME_MICRO_UTC()},
      {VARCHAR(), VARBINARY()},
      {DATE(), VARCHAR()},
      {BOOLEAN(), TIMESTAMP()},
      {ARRAY(VARCHAR()), VARCHAR()},
      {ARRAY(BOOLEAN()), ARRAY(TIMESTAMP())},
      {ROW({DATE(), BOOLEAN()}), ROW({VARCHAR(), INTEGER()})},
  };
  for (const auto& [fromType, toType] : kOthers) {
    SCOPED_TRACE(
        fmt::format("{} -> {}", fromType->toString(), toType->toString()));
    EXPECT_EQ(generate(fromType, toType).first, 0);
  }
}

} // namespace
} // namespace facebook::velox::fuzzer::test
