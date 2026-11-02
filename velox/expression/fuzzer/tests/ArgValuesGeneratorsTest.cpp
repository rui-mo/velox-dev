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
#include <cstdlib>
#include <unordered_set>

#include "velox/core/Expressions.h"
#include "velox/core/QueryConfig.h"
#include "velox/expression/fuzzer/ArgValuesGenerators.h"

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

} // namespace
} // namespace facebook::velox::fuzzer::test
