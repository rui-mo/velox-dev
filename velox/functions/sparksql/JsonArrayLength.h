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

#include <cctype>
#include <cstdint>
#include <optional>
#include <string_view>

#include "velox/functions/Macros.h"

namespace facebook::velox::functions::sparksql {

namespace detail {

/// Reads JSON the way Spark's Jackson parser does. Strings and field names can
/// be enclosed in single or double quotes and contain unescaped control
/// characters. Arrays and objects can nest at most 1000 levels deep. Only
/// space, tab, line feed and carriage return are whitespace.
class SparkJsonReader {
 public:
  explicit SparkJsonReader(std::string_view json) : json_{json} {}

  /// Returns the number of elements in the JSON array at the start of the
  /// input, or std::nullopt if the input does not start with a valid JSON
  /// array. Like Spark, ignores any content after the array.
  std::optional<int32_t> arrayLength() {
    skipWhitespace();
    if (!consume('[')) {
      return std::nullopt;
    }
    int32_t length = 0;
    if (!skipArrayElements(/*depth=*/1, length)) {
      return std::nullopt;
    }
    return length;
  }

 private:
  static constexpr int32_t kMaxDepth = 1000;

  // Skips the elements and the closing bracket of an array whose opening
  // bracket is consumed. 'depth' is the nesting depth of the array. Sets
  // 'length' to the number of elements.
  bool skipArrayElements(int32_t depth, int32_t& length) {
    skipWhitespace();
    if (consume(']')) {
      return true;
    }
    while (true) {
      if (!skipValue(depth)) {
        return false;
      }
      ++length;
      skipWhitespace();
      if (consume(']')) {
        return true;
      }
      if (!consume(',')) {
        return false;
      }
      skipWhitespace();
    }
  }

  // Skips the fields and the closing brace of an object whose opening brace is
  // consumed. 'depth' is the nesting depth of the object.
  bool skipObjectFields(int32_t depth) {
    skipWhitespace();
    if (consume('}')) {
      return true;
    }
    while (true) {
      if (!atQuote() || !skipString()) {
        return false;
      }
      skipWhitespace();
      if (!consume(':')) {
        return false;
      }
      skipWhitespace();
      if (!skipValue(depth)) {
        return false;
      }
      skipWhitespace();
      if (consume('}')) {
        return true;
      }
      if (!consume(',')) {
        return false;
      }
      skipWhitespace();
    }
  }

  // Skips the value at the current position. 'depth' is the nesting depth of
  // the array or object that contains the value.
  bool skipValue(int32_t depth) {
    if (pos_ == json_.size()) {
      return false;
    }
    switch (json_[pos_]) {
      case '[': {
        if (depth == kMaxDepth) {
          return false;
        }
        ++pos_;
        int32_t length = 0;
        return skipArrayElements(depth + 1, length);
      }
      case '{':
        if (depth == kMaxDepth) {
          return false;
        }
        ++pos_;
        return skipObjectFields(depth + 1);
      case '"':
      case '\'':
        return skipString();
      case 't':
        return consume("true");
      case 'f':
        return consume("false");
      case 'n':
        return consume("null");
      default:
        return skipNumber();
    }
  }

  // Skips the string that starts with the quote at the current position.
  bool skipString() {
    const char quote = json_[pos_++];
    while (pos_ < json_.size()) {
      const char c = json_[pos_++];
      if (c == quote) {
        return true;
      }
      if (c == '\\' && !skipEscape()) {
        return false;
      }
    }
    return false;
  }

  // Skips the escape sequence after a backslash.
  bool skipEscape() {
    if (pos_ == json_.size()) {
      return false;
    }
    switch (json_[pos_++]) {
      case '"':
      case '\'':
      case '\\':
      case '/':
      case 'b':
      case 'f':
      case 'n':
      case 'r':
      case 't':
        return true;
      case 'u':
        for (int i = 0; i < 4; ++i) {
          if (pos_ == json_.size() ||
              !std::isxdigit(static_cast<unsigned char>(json_[pos_]))) {
            return false;
          }
          ++pos_;
        }
        return true;
      default:
        return false;
    }
  }

  // Skips a number. Leading zeros, a leading plus sign, a decimal point
  // without digits on either side, NaN and Infinity are invalid.
  bool skipNumber() {
    consume('-');
    if (consume('0')) {
      if (atDigit()) {
        return false;
      }
    } else if (!skipDigits()) {
      return false;
    }
    if (consume('.') && !skipDigits()) {
      return false;
    }
    if (consume('e') || consume('E')) {
      if (!consume('+')) {
        consume('-');
      }
      if (!skipDigits()) {
        return false;
      }
    }
    return true;
  }

  // Skips one or more digits. Returns false if there is no digit.
  bool skipDigits() {
    if (!atDigit()) {
      return false;
    }
    do {
      ++pos_;
    } while (atDigit());
    return true;
  }

  void skipWhitespace() {
    while (pos_ < json_.size() &&
           (json_[pos_] == ' ' || json_[pos_] == '\t' || json_[pos_] == '\n' ||
            json_[pos_] == '\r')) {
      ++pos_;
    }
  }

  bool atDigit() const {
    return pos_ < json_.size() && json_[pos_] >= '0' && json_[pos_] <= '9';
  }

  bool atQuote() const {
    return pos_ < json_.size() && (json_[pos_] == '"' || json_[pos_] == '\'');
  }

  bool consume(char c) {
    if (pos_ < json_.size() && json_[pos_] == c) {
      ++pos_;
      return true;
    }
    return false;
  }

  bool consume(std::string_view token) {
    if (json_.substr(pos_, token.size()) == token) {
      pos_ += token.size();
      return true;
    }
    return false;
  }

  const std::string_view json_;
  size_t pos_{0};
};

} // namespace detail

/// json_array_length(jsonString) -> integer
///
/// Returns the number of elements in the outermost JSON array, or NULL if
/// 'jsonString' does not start with a valid JSON array.
template <typename T>
struct JsonArrayLengthFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(
      int32_t& result,
      const arg_type<Varchar>& json) {
    const auto length =
        detail::SparkJsonReader(std::string_view(json.data(), json.size()))
            .arrayLength();
    if (!length.has_value()) {
      return false;
    }
    result = length.value();
    return true;
  }
};

} // namespace facebook::velox::functions::sparksql
