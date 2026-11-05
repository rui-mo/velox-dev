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
#include "velox/functions/sparksql/TimestampUtils.h"

namespace facebook::velox::functions::sparksql {
namespace {

bool isPatternLetter(char c) {
  return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
}

// Checks a run of 'count' copies of 'letter' in a datetime pattern.
Status validatePatternLetters(char letter, size_t count, bool isParsing) {
  size_t maxCount;
  switch (letter) {
    case 'Y':
    case 'w':
    case 'e':
      return Status::UserError(
          "All week-based patterns are unsupported since Spark 3.0, detected "
          "week-based character: {}. Please use the SQL function EXTRACT "
          "instead.",
          letter);
    case 'C':
      return Status::UserError(
          "Illegal pattern character found in datetime pattern: {}.", letter);
    case 'E':
      if (isParsing) {
        return Status::UserError(
            "Illegal pattern character found in datetime pattern: {}.", letter);
      }
      maxCount = 4;
      break;
    case 'G':
    case 'M':
    case 'z':
      maxCount = 4;
      break;
    case 'y':
      maxCount = 6;
      break;
    case 'D':
      maxCount = 3;
      break;
    case 'd':
    case 'h':
    case 'H':
    case 'k':
    case 'K':
    case 'm':
    case 's':
      maxCount = 2;
      break;
    case 'a':
      maxCount = 1;
      break;
    case 'S':
      maxCount = 9;
      break;
    case 'Z':
    case 'x':
      maxCount = 5;
      break;
    default:
      return Status::OK();
  }
  if (count > maxCount) {
    return Status::UserError(
        "Too many letters in datetime pattern: {}.",
        std::string(count, letter));
  }
  return Status::OK();
}

} // namespace

void toGMTWithGapCorrection(Timestamp& time, const tz::TimeZone& zone) {
  time = Timestamp(
      zone.correct_nonexistent_time(std::chrono::seconds(time.getSeconds()))
          .count(),
      time.getNanos());
  time.toGMT(zone);
}

void toGMTWithPreferredOffset(
    Timestamp& time,
    const tz::TimeZone& zone,
    int64_t preferredOffset) {
  const auto local =
      zone.correct_nonexistent_time(std::chrono::seconds(time.getSeconds()));
  // An offset is valid for 'local' if its instant maps back to 'local'.
  const auto preferred = local - std::chrono::seconds(preferredOffset);
  if (zone.toLocalChecked(preferred) == local) {
    time = Timestamp(preferred.count(), time.getNanos());
    return;
  }
  time = Timestamp(local.count(), time.getNanos());
  time.toGMT(zone);
}

Status validateDateTimePattern(std::string_view pattern, bool isParsing) {
  // Spark parses a fraction of a second with its own builder. That builder
  // splits the pattern on quotes and accepts an unclosed one.
  const bool allowUnclosedQuote =
      isParsing && pattern.find('S') != std::string_view::npos;
  size_t pos = 0;
  while (pos < pattern.size()) {
    const char c = pattern[pos];
    if (c == '\'') {
      // Skip the quoted text, where two quotes stand for one.
      ++pos;
      while (pos < pattern.size() &&
             (pattern[pos] != '\'' ||
              (pos + 1 < pattern.size() && pattern[pos + 1] == '\''))) {
        pos += pattern[pos] == '\'' ? 2 : 1;
      }
      if (pos == pattern.size()) {
        if (allowUnclosedQuote) {
          break;
        }
        return Status::UserError("No closing single quote for literal");
      }
      ++pos;
    } else if (isPatternLetter(c)) {
      const auto start = pos;
      while (pos < pattern.size() && pattern[pos] == c) {
        ++pos;
      }
      VELOX_RETURN_NOT_OK(validatePatternLetters(c, pos - start, isParsing));
    } else {
      if (c == '{' || c == '}' || c == '#') {
        return Status::UserError(
            "Pattern includes reserved character: '{}'", c);
      }
      ++pos;
    }
  }
  return Status::OK();
}

} // namespace facebook::velox::functions::sparksql
