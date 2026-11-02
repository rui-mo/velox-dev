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

#include <string_view>

#include "velox/functions/Udf.h"
#include "velox/functions/lib/Utf8Utils.h"
#include "velox/functions/sparksql/SparkQueryConfig.h"

namespace facebook::velox::functions::sparksql {

/// split(string, delimiter[, limit]) -> array(varchar)
///
/// Splits string on delimiter and returns an array of size at most limit.
/// delimiter is a string representing regular expression.
/// limit is an integer which controls the number of times the regex is applied.
/// By default, limit is -1, which means 'no limit', the delimiter will be
/// applied as many times as possible.
template <typename T>
struct Split {
  Split() : cache_(0) {}

  VELOX_DEFINE_FUNCTION_TYPES(T);

  // Results refer to strings in the first argument.
  static constexpr int32_t reuse_strings_from_arg = 0;

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& inputTypes,
      const core::QueryConfig& config,
      const arg_type<Varchar>* input,
      const arg_type<Varchar>* delimiter) {
    initialize(inputTypes, config, input, delimiter, nullptr);
  }

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Varchar>* /*input*/,
      const arg_type<Varchar>* delimiter,
      const arg_type<int32_t>* /*limit*/) {
    cache_.setMaxCompiledRegexes(config.exprMaxCompiledRegexes());
    legacyTruncateForEmptyRegexSplit_ =
        SparkQueryConfig{config}.legacyTruncateForEmptyRegexSplit();
    if (delimiter) {
      initializeFastPath(delimiter->data(), delimiter->size());
      isConstantDelimiter_ = true;
    }
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Array<Varchar>>& result,
      const arg_type<Varchar>& input,
      const arg_type<Varchar>& delimiter) {
    doCall(result, input, delimiter, INT32_MAX);
  }

  FOLLY_ALWAYS_INLINE void call(
      out_type<Array<Varchar>>& result,
      const arg_type<Varchar>& input,
      const arg_type<Varchar>& delimiter,
      const arg_type<int32_t>& limit) {
    doCall(result, input, delimiter, limit > 0 ? limit : INT32_MAX);
  }

 private:
  enum class FastPathKind {
    kNone,
    kSingleChar,
    kLiteralString,
  };

  void doCall(
      out_type<Array<Varchar>>& result,
      const arg_type<Varchar>& input,
      const arg_type<Varchar>& delimiter,
      int32_t limit) {
    if (delimiter.empty()) {
      splitEmptyDelimiter(result, input, limit);
    } else {
      split(result, input, delimiter, limit);
    }
  }

  // Checks if a string represents an octal-escaped ascii character (e.g.,
  // \123). If valid, converts it to its decimal value.
  bool isOctalString(const char* str, char& decimal, size_t size) const {
    if (size > 4 || str[0] != '\\') {
      return false;
    }
    for (int i = 1; i < size; ++i) {
      char ch = str[i];
      if (ch < '0' || ch > '7') {
        return false;
      }
    }
    // The ascii octal number should not exceed 177.
    if (std::stoi(str + 1) > 177) {
      return false;
    }
    decimal = std::stoi(str + 1, nullptr, 8);
    return true;
  }

  // When pattern is empty, split each character out. Since Spark 3.4, when
  // delimiter is empty, the result does not include an empty tail string, e.g.
  // split('abc', '') outputs ["a", "b", "c"] instead of ["a", "b", "c", ""].
  // When limit is smaller than the number of characters, the last element
  // contains the rest of the input, e.g. split('abc', '', 2) outputs ["a",
  // "bc"]. If 'legacyTruncateForEmptyRegexSplit_' is true, the rest of the
  // input is dropped instead, e.g. split('abc', '', 2) outputs ["a", "b"].
  void splitEmptyDelimiter(
      out_type<Array<Varchar>>& result,
      const arg_type<Varchar>& input,
      int32_t limit) const {
    if (input.size() == 0) {
      result.add_item().setNoCopy(StringView());
      return;
    }

    const size_t end = input.size();
    const char* start = input.data();
    size_t pos = 0;
    int32_t count = 0;
    while (pos < end && count < limit) {
      if (count + 1 == limit && !legacyTruncateForEmptyRegexSplit_) {
        result.add_item().setNoCopy(StringView(start + pos, end - pos));
        return;
      }
      int32_t codePoint;
      auto charLength = tryGetUtf8CharLength(start + pos, end - pos, codePoint);
      if (charLength <= 0) {
        // Invalid UTF-8 character, the length of the invalid
        // character is the absolute value of result of `tryGetUtf8CharLength`.
        charLength = -charLength;
      }
      result.add_item().setNoCopy(StringView(start + pos, charLength));
      pos += charLength;
      count += 1;
    }
  }

  // Fast path if the delimiter is a
  // (1)one-char string and this character is not one of the RegEx's meta
  // characters ".$|()[{^?*+\\", or
  // (2)two-char string and the first char is the backslash and the second is
  // not the ascii digit or ascii letter, or
  // (3)octal string that falls within the range of ASCII characters, or
  // (4)non-empty plain string without any RegEx meta character.
  void initializeFastPath(const char* delimiterStr, size_t delimiterSize) {
    // Referencing from
    // https://github.com/openjdk/jdk8/blob/master/jdk/src/share/classes/java/lang/String.java#L2325
    // to align with Spark.
    static constexpr std::string_view kReservedChars = ".$|()[{^?*+\\";

    if (delimiterSize == 1) {
      singleCharDelimiter_ = delimiterStr[0];
      if (kReservedChars.find(singleCharDelimiter_) == std::string_view::npos) {
        fastPathKind_ = FastPathKind::kSingleChar;
        return;
      }
    } else if (delimiterSize == 2 && delimiterStr[0] == '\\') {
      singleCharDelimiter_ = delimiterStr[1];
      const auto delimiterChar =
          static_cast<unsigned char>(singleCharDelimiter_);
      if (!std::isdigit(delimiterChar) && !std::isalpha(delimiterChar)) {
        fastPathKind_ = FastPathKind::kSingleChar;
        return;
      }
    } else if (isOctalString(
                   delimiterStr, singleCharDelimiter_, delimiterSize)) {
      fastPathKind_ = FastPathKind::kSingleChar;
      return;
    }

    if (delimiterSize > 1 &&
        std::string_view{delimiterStr, delimiterSize}.find_first_of(
            kReservedChars) == std::string_view::npos) {
      fastPathKind_ = FastPathKind::kLiteralString;
      literalDelimiter_.assign(delimiterStr, delimiterSize);
      return;
    }

    fastPathKind_ = FastPathKind::kNone;
  }

  // Split with a non-empty delimiter. If limit > 0, The resulting array's
  // length will not be more than limit and the resulting array's last entry
  // will contain all input beyond the last matched regex. If limit <= 0,
  // delimiter will be applied as many times as possible, and the resulting
  // array can be of any size.
  void split(
      out_type<Array<Varchar>>& result,
      const arg_type<Varchar>& input,
      const arg_type<Varchar>& delimiter,
      int32_t limit) {
    VELOX_DCHECK(!delimiter.empty(), "Non-empty delimiter is expected");

    if (!isConstantDelimiter_) {
      initializeFastPath(delimiter.data(), delimiter.size());
    }

    if (fastPathKind_ == FastPathKind::kNone) {
      splitByRegex(result, input, delimiter, limit);
      return;
    }

    // Trivial case of converting string to array with 1 element.
    if (limit == 1) {
      result.add_item().setNoCopy(input);
      return;
    }

    // Splits input string using the delimiter and adds the cutting-off pieces
    // to elements vector until the string's end or the limit is reached.
    int32_t addedElements{0};
    const size_t end = input.size();
    const char* start = input.data();
    size_t pos = 0;

    if (fastPathKind_ == FastPathKind::kSingleChar) {
      for (size_t i = 0; i < end; ++i) {
        if (start[i] == singleCharDelimiter_) {
          result.add_item().setNoCopy(StringView(start + pos, i - pos));
          pos = i + 1;
          ++addedElements;
          if (addedElements + 1 == limit) {
            break;
          }
        }
      }
    } else if (fastPathKind_ == FastPathKind::kLiteralString) {
      std::string_view inputView{start, end};
      std::string_view delimiterView{literalDelimiter_};
      while (true) {
        const size_t found = inputView.find(delimiterView, pos);
        if (found == std::string_view::npos) {
          break;
        }

        result.add_item().setNoCopy(StringView(start + pos, found - pos));
        pos = found + literalDelimiter_.size();

        ++addedElements;
        // If the next element should be the last, leave the loop.
        if (addedElements + 1 == limit) {
          break;
        }
      }
    }

    // Add the rest of the string and we are done.
    // Note that the rest of the string can be empty - we still add it.
    result.add_item().setNoCopy(StringView(start + pos, end - pos));
  }

  // Splits like java.util.regex.Pattern.split. A zero-width match at the
  // beginning of the input is skipped. After an empty match, the next search
  // starts one character later, so an empty match right after a non-empty match
  // is kept. Once limit - 1 elements are added, the next match adds the rest of
  // the input as the last element.
  void splitByRegex(
      out_type<Array<Varchar>>& result,
      const arg_type<Varchar>& input,
      const arg_type<Varchar>& delimiter,
      int32_t limit) {
    // Compiles the pattern even if the result is known so that an invalid
    // pattern throws like in Spark.
    auto* re = cache_.findOrCompile(delimiter);
    if (input.empty() || limit == 1) {
      result.add_item().setNoCopy(input);
      return;
    }

    const char* start = input.data();
    const size_t end = input.size();
    const re2::StringPiece text(start, end);
    re2::StringPiece match;
    int32_t addedElements = 0;
    // The start of the next element.
    size_t pos = 0;
    size_t searchPos = 0;
    while (searchPos <= end &&
           re->Match(text, searchPos, end, RE2::UNANCHORED, &match, 1)) {
      const size_t matchStart = match.data() - start;
      const size_t matchEnd = matchStart + match.size();
      if (matchStart != matchEnd) {
        searchPos = matchEnd;
      } else if (matchEnd < end) {
        int32_t codePoint;
        const auto charLength =
            tryGetUtf8CharLength(start + matchEnd, end - matchEnd, codePoint);
        // An invalid UTF-8 character has a length of -charLength.
        searchPos = matchEnd + std::abs(charLength);
      } else {
        searchPos = end + 1;
      }

      if (addedElements + 1 == limit) {
        result.add_item().setNoCopy(StringView(start + pos, end - pos));
        return;
      }
      if (pos == 0 && matchEnd == 0) {
        continue;
      }
      result.add_item().setNoCopy(StringView(start + pos, matchStart - pos));
      pos = matchEnd;
      ++addedElements;
    }
    result.add_item().setNoCopy(StringView(start + pos, end - pos));
  }

  mutable facebook::velox::functions::detail::ReCache cache_;
  FastPathKind fastPathKind_{FastPathKind::kNone};
  // Single character delimiter for fast path.
  char singleCharDelimiter_;
  // Non-empty delimiter for the literal-string fast path.
  std::string literalDelimiter_;
  bool isConstantDelimiter_{false};
  bool legacyTruncateForEmptyRegexSplit_{false};
};
} // namespace facebook::velox::functions::sparksql
