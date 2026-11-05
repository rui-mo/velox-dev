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

// Timestamp conversion code inspired by DuckDB's date/time/timestamp conversion
// libraries. License below:

/*
 * Copyright 2018 DuckDB Contributors
 * (see https://github.com/cwida/duckdb/graphs/contributors)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#include "velox/type/TimestampConversion.h"
#include <folly/Expected.h>
#include <algorithm>
#include "velox/common/base/CheckedArithmetic.h"
#include "velox/common/base/Exceptions.h"
#include "velox/type/FastDate.h"
#include "velox/type/HugeInt.h"
#include "velox/type/WideRangeDateConversion.h"
#include "velox/type/tz/TimeZoneMap.h"

namespace facebook::velox::util {

constexpr int32_t kLeapDays[] =
    {0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
constexpr int32_t kNormalDays[] =
    {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

namespace {

inline bool characterIsSpace(char c) {
  return c == ' ' || c == '\t' || c == '\n' || c == '\v' || c == '\f' ||
      c == '\r';
}

inline bool characterIsDigit(char c) {
  return c >= '0' && c <= '9';
}

bool parseDoubleDigit(
    const char* buf,
    size_t len,
    size_t& pos,
    int32_t& result) {
  if (pos < len && characterIsDigit(buf[pos])) {
    result = buf[pos++] - '0';
    if (pos < len && characterIsDigit(buf[pos])) {
      result = (buf[pos++] - '0') + result * 10;
    }
    return true;
  }
  return false;
}

bool isValidWeekDate(int32_t weekYear, int32_t weekOfYear, int32_t dayOfWeek) {
  if (dayOfWeek < 1 || dayOfWeek > 7) {
    return false;
  }
  if (weekOfYear < 1 || weekOfYear > 52) {
    return false;
  }
  if (weekYear < kMinYear || weekYear > kMaxYear) {
    return false;
  }
  return true;
}

bool isValidWeekOfMonthDate(
    int32_t year,
    int32_t month,
    int32_t weekOfMonth,
    int32_t dayOfWeek) {
  if (year < 1 || year > kMaxYear) {
    return false;
  }
  if (month < 1 || month > 12) {
    return false;
  }

  Expected<int64_t> daysSinceEpochOfFirstDayOfMonth =
      daysSinceEpochFromDate(year, month, 1);
  if (daysSinceEpochOfFirstDayOfMonth.hasError()) {
    return false;
  }
  daysSinceEpochOfFirstDayOfMonth = daysSinceEpochOfFirstDayOfMonth.value();

  // Calculates the actual number of week of month and validates if it is in the
  // valid range.
  const int32_t firstDayOfWeek =
      extractISODayOfTheWeek(daysSinceEpochOfFirstDayOfMonth.value());
  const int32_t firstWeekLength = 7 - firstDayOfWeek + 1;
  const int32_t monthLength =
      isLeapYear(year) ? kLeapDays[month] : kNormalDays[month];
  const int32_t actualWeeks = 1 + ceil((monthLength - firstWeekLength) / 7.0);
  if (weekOfMonth < 1 || weekOfMonth > actualWeeks) {
    return false;
  }

  // Validate day of week.
  // If dayOfWeek is before the first day of week, it is considered invalid.
  if (weekOfMonth == 1 && dayOfWeek < firstDayOfWeek) {
    return false;
  }
  const int32_t lastWeekLength = (monthLength - firstWeekLength) % 7;
  // If dayOfWeek is after the last day of the last week of the month, it is
  // considered invalid.
  if (weekOfMonth == actualWeeks && lastWeekLength != 0 &&
      dayOfWeek > lastWeekLength) {
    return false;
  }

  return true;
}

inline bool validDate(int64_t daysSinceEpoch) {
  return daysSinceEpoch >= std::numeric_limits<int32_t>::min() &&
      daysSinceEpoch <= std::numeric_limits<int32_t>::max();
}

// Skip leading spaces.
inline void skipSpaces(const char* buf, size_t len, size_t& pos) {
  while (pos < len && characterIsSpace(buf[pos])) {
    pos++;
  }
}

bool tryParseDateString(
    const char* buf,
    size_t len,
    size_t& pos,
    int64_t& daysSinceEpoch,
    ParseMode mode) {
  pos = 0;
  if (len == 0) {
    return false;
  }

  int32_t day = 0;
  int32_t month = -1;
  int32_t year = 0;
  bool yearneg = false;
  // Whether a sign is included in the date string.
  bool sign = false;
  int sep;
  if (mode != ParseMode::kIso8601) {
    skipSpaces(buf, len, pos);
  }

  if (pos >= len) {
    return false;
  }
  if (buf[pos] == '-') {
    sign = true;
    yearneg = true;
    pos++;
    if (pos >= len) {
      return false;
    }
  } else if (buf[pos] == '+') {
    sign = true;
    pos++;
    if (pos >= len) {
      return false;
    }
  }

  if (!characterIsDigit(buf[pos])) {
    return false;
  }
  // First parse the year.
  for (; pos < len && characterIsDigit(buf[pos]); pos++) {
    year = checkedPlus((buf[pos] - '0'), checkedMultiply(year, 10));
    if (year > kMaxYear) {
      break;
    }
  }
  /// Spark digits of year must >= 4. The following formats are allowed:
  /// `[+-]yyyy*`
  /// `[+-]yyyy*-[m]m`
  /// `[+-]yyyy*-[m]m-[d]d`
  /// `[+-]yyyy*-[m]m-[d]d `
  /// `[+-]yyyy*-[m]m-[d]d *`
  /// `[+-]yyyy*-[m]m-[d]dT*`
  if (mode == ParseMode::kSparkCast && pos - sign < 4) {
    return false;
  }
  if (yearneg) {
    year = checkedNegate(year);
    if (year < kMinYear) {
      return false;
    }
  }

  // No month or day.
  if ((mode == ParseMode::kSparkCast || mode == ParseMode::kIso8601) &&
      (pos == len || buf[pos] == 'T')) {
    Expected<int64_t> expected = daysSinceEpochFromDate(year, 1, 1);
    if (expected.hasError()) {
      return false;
    }
    daysSinceEpoch = expected.value();
    return validDate(daysSinceEpoch);
  }

  if (pos >= len) {
    return false;
  }

  // Fetch the separator.
  sep = buf[pos++];
  if (mode == ParseMode::kPrestoCast || mode == ParseMode::kSparkCast ||
      mode == ParseMode::kIso8601) {
    // Only '-' is valid for cast.
    if (sep != '-') {
      return false;
    }
  } else {
    if (sep != ' ' && sep != '-' && sep != '/' && sep != '\\') {
      // Invalid separator.
      return false;
    }
  }

  // Parse the month.
  if (!parseDoubleDigit(buf, len, pos, month)) {
    return false;
  }

  // No day.
  if ((mode == ParseMode::kSparkCast || mode == ParseMode::kIso8601) &&
      (pos == len || buf[pos] == 'T')) {
    Expected<int64_t> expected = daysSinceEpochFromDate(year, month, 1);
    if (expected.hasError()) {
      return false;
    }
    daysSinceEpoch = expected.value();
    return validDate(daysSinceEpoch);
  }

  if (pos >= len) {
    return false;
  }

  if (buf[pos++] != sep) {
    return false;
  }

  if (pos >= len) {
    return false;
  }

  // Now parse the day.
  if (!parseDoubleDigit(buf, len, pos, day)) {
    return false;
  }

  if (mode == ParseMode::kPrestoCast || mode == ParseMode::kIso8601) {
    Expected<int64_t> expected = daysSinceEpochFromDate(year, month, day);
    if (expected.hasError()) {
      return false;
    }
    daysSinceEpoch = expected.value();

    if (mode == ParseMode::kPrestoCast) {
      skipSpaces(buf, len, pos);
    }

    if (pos == len) {
      return validDate(daysSinceEpoch);
    }
    return false;
  }

  // In non-standard cast mode, an optional trailing 'T' or space followed
  // by any optional characters are valid patterns.
  if (mode == ParseMode::kSparkCast) {
    Expected<int64_t> expected = daysSinceEpochFromDate(year, month, day);
    if (expected.hasError()) {
      return false;
    }
    daysSinceEpoch = expected.value();

    if (!validDate(daysSinceEpoch)) {
      return false;
    }

    if (pos == len) {
      return true;
    }

    if (buf[pos] == 'T' || buf[pos] == ' ') {
      return true;
    }
    return false;
  }

  // Check for an optional trailing " (BC)".
  if (len - pos >= 5 && characterIsSpace(buf[pos]) && buf[pos + 1] == '(' &&
      buf[pos + 2] == 'B' && buf[pos + 3] == 'C' && buf[pos + 4] == ')') {
    if (yearneg || year == 0) {
      return false;
    }
    year = -year + 1;
    pos += 5;

    if (year < kMinYear) {
      return false;
    }
  }

  // In strict mode, check remaining string for non-space characters.
  if (mode == ParseMode::kStrict || mode == ParseMode::kIso8601) {
    skipSpaces(buf, len, pos);

    // Check position. if end was not reached, non-space chars remaining.
    if (pos < len) {
      return false;
    }
  } else {
    // In non-strict mode, check for any direct trailing digits.
    if (pos < len && characterIsDigit(buf[pos])) {
      return false;
    }
  }
  Expected<int64_t> expected = daysSinceEpochFromDate(year, month, day);
  if (expected.hasError()) {
    return false;
  }
  daysSinceEpoch = expected.value();
  return true;
}

void parseTimeSeparator(
    const char* buf,
    size_t& pos,
    TimestampParseMode parseMode) {
  switch (parseMode) {
    case TimestampParseMode::kIso8601:
      if (buf[pos] == 'T') {
        pos++;
      }
      break;
    case TimestampParseMode::kPrestoCast:
      if (buf[pos] == ' ') {
        pos++;
      }
      break;
    case TimestampParseMode::kLegacyCast:
      if (buf[pos] == ' ' || buf[pos] == 'T') {
        pos++;
      }
      break;
    case TimestampParseMode::kSparkCast:
      VELOX_UNREACHABLE();
  }
}

// String format is hh:mm:ss.microseconds (seconds and microseconds are
// optional).
// ISO 8601
bool tryParseTimeString(
    const char* buf,
    size_t len,
    size_t& pos,
    int64_t& result,
    TimestampParseMode parseMode) {
  static constexpr int sep = ':';
  int32_t hour = 0, min = 0, sec = 0, micros = 0;
  pos = 0;

  if (len == 0) {
    return false;
  }

  if (parseMode != TimestampParseMode::kIso8601) {
    skipSpaces(buf, len, pos);
  }

  if (pos >= len) {
    return false;
  }

  if (!characterIsDigit(buf[pos])) {
    return false;
  }

  // Read the hours.
  if (!parseDoubleDigit(buf, len, pos, hour)) {
    return false;
  }
  if (hour < 0 || hour >= 24) {
    return false;
  }

  if (pos >= len || buf[pos] != sep) {
    if (parseMode == TimestampParseMode::kIso8601) {
      result = fromTime(hour, 0, 0, 0);
      return true;
    }
    return false;
  }

  // Fetch the separator.
  if (buf[pos++] != sep) {
    // Invalid separator.
    return false;
  }

  // Read the minutes.
  if (!parseDoubleDigit(buf, len, pos, min)) {
    return false;
  }
  if (min < 0 || min >= 60) {
    return false;
  }

  // Try to read seconds.
  if (pos < len && buf[pos] == sep) {
    ++pos;
    if (!parseDoubleDigit(buf, len, pos, sec)) {
      return false;
    }
    if (sec < 0 || sec > 60) {
      return false;
    }

    // Try to read microseconds.
    if (pos < len) {
      if (buf[pos] == '.') {
        ++pos;
      } else if (parseMode == TimestampParseMode::kIso8601 && buf[pos] == ',') {
        ++pos;
      }

      if (pos >= len) {
        return false;
      }

      // We expect microseconds.
      int32_t mult = 100000;
      for (; pos < len && characterIsDigit(buf[pos]); pos++, mult /= 10) {
        if (mult > 0) {
          micros += (buf[pos] - '0') * mult;
        }
      }
    }
  }

  result = fromTime(hour, min, sec, micros);
  return true;
}

// String format is [+/-]hh:mm:ss.MMM
// * minutes, seconds, and milliseconds are optional.
// * all separators are optional.
// * . may be replaced with ,
bool tryParsePrestoTimeOffsetString(
    const char* buf,
    size_t len,
    size_t& pos,
    int64_t& result) {
  static constexpr int sep = ':';
  int32_t hour = 0, min = 0, sec = 0, millis = 0;
  pos = 0;
  result = 0;

  if (len == 0) {
    return false;
  }

  if (buf[pos] != '+' && buf[pos] != '-') {
    return false;
  }

  bool positive = buf[pos++] == '+';

  if (pos >= len) {
    return false;
  }

  // Read the hours.
  if (!parseDoubleDigit(buf, len, pos, hour)) {
    return false;
  }
  if (hour < 0 || hour >= 24) {
    return false;
  }

  result += hour * kMillisPerHour;

  if (pos >= len || (buf[pos] != sep && !characterIsDigit(buf[pos]))) {
    result *= positive ? 1 : -1;
    return pos == len;
  }

  // Skip the separator.
  if (buf[pos] == sep) {
    pos++;
  }

  // Read the minutes.
  if (!parseDoubleDigit(buf, len, pos, min)) {
    return false;
  }
  if (min < 0 || min >= 60) {
    return false;
  }

  result += min * kMillisPerMinute;

  if (pos >= len || (buf[pos] != sep && !characterIsDigit(buf[pos]))) {
    result *= positive ? 1 : -1;
    return pos == len;
  }

  // Skip the separator.
  if (buf[pos] == sep) {
    pos++;
  }

  // Try to read seconds.
  if (!parseDoubleDigit(buf, len, pos, sec)) {
    return false;
  }
  if (sec < 0 || sec >= 60) {
    return false;
  }

  result += sec * kMillisPerSecond;

  if (pos >= len ||
      (buf[pos] != '.' && buf[pos] != ',' && !characterIsDigit(buf[pos]))) {
    result *= positive ? 1 : -1;
    return pos == len;
  }

  // Skip the decimal.
  if (buf[pos] == '.' || buf[pos] == ',') {
    pos++;
  }

  // Try to read microseconds.
  if (pos >= len) {
    return false;
  }

  // We expect milliseconds.
  int32_t mult = 100;
  for (; pos < len && mult > 0 && characterIsDigit(buf[pos]);
       pos++, mult /= 10) {
    millis += (buf[pos] - '0') * mult;
  }

  result += millis;
  result *= positive ? 1 : -1;
  return pos == len;
}

// Parses a variety of timestamp strings, depending on the value of
// `parseMode`. Consumes as much of the string as it can and sets `result` to
// the timestamp from whatever it successfully parses. `pos` is set to the
// position of first character that was not consumed. Returns true if it
// successfully parsed at least a date, `result` is only set if true is
// returned.
bool tryParseTimestampString(
    const char* buf,
    size_t len,
    size_t& pos,
    Timestamp& result,
    TimestampParseMode parseMode) {
  int64_t daysSinceEpoch = 0;
  int64_t microsSinceMidnight = 0;

  if (parseMode == TimestampParseMode::kIso8601) {
    // Leading spaces are not allowed.
    size_t startPos = pos;
    skipSpaces(buf, len, pos);
    if (pos > startPos) {
      return false;
    }
  }

  if (parseMode == TimestampParseMode::kIso8601 && pos < len &&
      buf[pos] == 'T') {
    if (pos == len - 1) {
      // If the string is just 'T'.
      return false;
    }
    // No date. Assume 1970-01-01.
  } else if (!tryParseDateString(
                 buf,
                 len,
                 pos,
                 daysSinceEpoch,
                 parseMode == TimestampParseMode::kIso8601
                     ? ParseMode::kSparkCast
                     : ParseMode::kNonStrict)) {
    return false;
  }

  if (pos == len) {
    // No time: only a date.
    result = fromDatetime(daysSinceEpoch, 0);
    return true;
  }

  // Try to parse a time field.
  parseTimeSeparator(buf, pos, parseMode);

  size_t timePos = 0;
  if (!tryParseTimeString(
          buf + pos, len - pos, timePos, microsSinceMidnight, parseMode)) {
    // The rest of the string is not a valid time, but it could be relevant to
    // the caller (e.g. it could be a time zone), return the date we parsed
    // and let them decide what to do with the rest.
    result = fromDatetime(daysSinceEpoch, 0);
    return true;
  }

  pos += timePos;
  result = fromDatetime(daysSinceEpoch, microsSinceMidnight);
  return true;
}

// Returns true for the bytes that Spark trims from timestamp strings, i.e.
// whitespace and ISO control characters.
inline bool isSparkWhitespaceOrControl(char c) {
  const auto byte = static_cast<unsigned char>(c);
  return byte <= 0x20 || byte == 0x7F;
}

// Parses a timestamp string in TimestampParseMode::kSparkCast, like
// SparkDateTimeUtils.parseTimestampString in Spark. Sets 'timeZone' to the
// time zone after the time, without surrounding whitespace, or to an empty
// string if there is none.
bool tryParseSparkTimestampString(
    const char* buf,
    size_t len,
    Timestamp& result,
    std::string_view& timeZone) {
  size_t pos = 0;
  size_t end = len;
  while (pos < end && isSparkWhitespaceOrControl(buf[pos])) {
    ++pos;
  }
  while (end > pos && isSparkWhitespaceOrControl(buf[end - 1])) {
    --end;
  }
  if (pos == end) {
    return false;
  }

  int32_t yearSign = 1;
  if (buf[pos] == '-' || buf[pos] == '+') {
    yearSign = buf[pos] == '-' ? -1 : 1;
    ++pos;
  }

  // The year, month, day, hour, minute, second and microseconds. Each
  // separator ends the segment with the same index.
  static constexpr char kSeparators[] = {'-', '-', ' ', ':', ':', '.'};
  int32_t segments[] = {1, 1, 1, 0, 0, 0, 0};
  size_t segment = 0;
  int32_t value = 0;
  size_t digits = 0;
  const auto isValidDigits = [&]() {
    switch (segment) {
      case 0:
        return digits >= 4 && digits <= 6;
      case 6:
        return true;
      default:
        return digits >= 1 && digits <= 2;
    }
  };

  for (; pos < end; ++pos) {
    const char c = buf[pos];
    if (characterIsDigit(c)) {
      // Spark ignores the digits of the fraction after the 6th. Other
      // segments with that many digits are invalid.
      if (digits < 6) {
        value = value * 10 + (c - '0');
      }
      ++digits;
      continue;
    }
    if (segment == 6 || (segment == 5 && c != '.')) {
      // The rest is the time zone.
      break;
    }
    // Spark also accepts a time without a date, e.g. "10:00:00" or
    // "T10:00:00", and uses the current date for it. This is not supported.
    if (c != kSeparators[segment] && !(segment == 2 && c == 'T')) {
      return false;
    }
    if (!isValidDigits()) {
      return false;
    }
    segments[segment++] = value;
    value = 0;
    digits = 0;
  }
  if (!isValidDigits()) {
    return false;
  }
  segments[segment] = value;
  if (segment == 6) {
    for (; digits < 6; ++digits) {
      segments[6] *= 10;
    }
  }

  if (segments[3] > 23 || segments[4] > 59 || segments[5] > 59) {
    return false;
  }
  const auto daysSinceEpoch =
      daysSinceEpochFromDate(yearSign * segments[0], segments[1], segments[2]);
  if (daysSinceEpoch.hasError()) {
    return false;
  }
  result = fromDatetime(
      daysSinceEpoch.value(),
      fromTime(segments[3], segments[4], segments[5], segments[6]));

  // Like Java's String.trim(), which Spark applies to the time zone.
  while (pos < end && static_cast<unsigned char>(buf[pos]) <= 0x20) {
    ++pos;
  }
  timeZone = std::string_view(buf + pos, end - pos);
  return true;
}

// Returns the offset in seconds of a Java ZoneOffset ID, i.e. 'Z' or a sign
// followed by h, hh, hhmm, hh:mm, hhmmss or hh:mm:ss within 18 hours, like
// Java's ZoneOffset.of(). Returns std::nullopt for other IDs.
std::optional<int32_t> tryParseJavaZoneOffset(std::string_view id) {
  if (id == "Z") {
    return 0;
  }
  if (id.empty() || (id[0] != '+' && id[0] != '-')) {
    return std::nullopt;
  }
  std::string padded;
  if (id.size() == 2) {
    padded = {id[0], '0', id[1]};
    id = padded;
  }
  // Parses the 2 digits at 'pos', which follow a colon if 'afterColon'.
  const auto parseField = [&](size_t pos, bool afterColon) {
    if ((afterColon && id[pos - 1] != ':') || !characterIsDigit(id[pos]) ||
        !characterIsDigit(id[pos + 1])) {
      return -1;
    }
    return (id[pos] - '0') * 10 + (id[pos + 1] - '0');
  };
  int32_t minutes = 0;
  int32_t seconds = 0;
  switch (id.size()) {
    case 3:
      break;
    case 5:
      minutes = parseField(3, false);
      break;
    case 6:
      minutes = parseField(4, true);
      break;
    case 7:
      minutes = parseField(3, false);
      seconds = parseField(5, false);
      break;
    case 9:
      minutes = parseField(4, true);
      seconds = parseField(7, true);
      break;
    default:
      return std::nullopt;
  }
  const int32_t hours = parseField(1, false);
  if (hours < 0 || minutes < 0 || seconds < 0 || hours > 18 || minutes > 59 ||
      seconds > 59 || (hours == 18 && minutes + seconds > 0)) {
    return std::nullopt;
  }
  const int32_t offset = (hours * 60 + minutes) * 60 + seconds;
  return id[0] == '-' ? -offset : offset;
}

// Returns the offset in seconds of the Java region IDs with fixed offsets that
// tz::locateZone() doesn't resolve by their exact names, i.e. the aliases of
// UTC and Etc/GMT+h and Etc/GMT-h. Returns std::nullopt for other IDs.
std::optional<int32_t> tryParseJavaFixedOffsetRegion(std::string_view id) {
  static constexpr std::string_view kUtcAliases[] = {
      "UCT",
      "Universal",
      "Zulu",
      "Greenwich",
      "GMT0",
      "Etc/UTC",
      "Etc/UCT",
      "Etc/Universal",
      "Etc/Zulu",
      "Etc/GMT",
      "Etc/GMT0",
      "Etc/Greenwich"};
  if (std::find(std::begin(kUtcAliases), std::end(kUtcAliases), id) !=
      std::end(kUtcAliases)) {
    return 0;
  }
  // Etc/GMT+h is h hours behind UTC, from Etc/GMT+12 to Etc/GMT-14, without
  // leading zeros.
  static constexpr std::string_view kEtcGmt = "Etc/GMT";
  if (id.size() < kEtcGmt.size() + 2 || id.size() > kEtcGmt.size() + 3 ||
      id.substr(0, kEtcGmt.size()) != kEtcGmt) {
    return std::nullopt;
  }
  const char sign = id[kEtcGmt.size()];
  const auto digits = id.substr(kEtcGmt.size() + 1);
  if ((sign != '+' && sign != '-') ||
      !std::all_of(digits.begin(), digits.end(), characterIsDigit) ||
      (digits.size() == 2 && digits[0] == '0')) {
    return std::nullopt;
  }
  int32_t hours = 0;
  for (const char digit : digits) {
    hours = hours * 10 + (digit - '0');
  }
  if (hours > (sign == '+' ? 12 : 14)) {
    return std::nullopt;
  }
  return (sign == '+' ? -hours : hours) * 3'600;
}

// Resolves a Java region ID, matching its case exactly.
bool tryResolveJavaRegion(
    std::string_view id,
    ParsedTimestampWithTimeZone& result) {
  if (const auto offset = tryParseJavaFixedOffsetRegion(id)) {
    result.offsetMillis = *offset * kMillisPerSecond;
    return true;
  }
  const auto* timeZone = tz::locateZone(id, false);
  if (timeZone == nullptr || timeZone->name() != id) {
    return false;
  }
  result.timeZone = timeZone;
  return true;
}

// Resolves the time zone of a timestamp string like Spark's
// SparkDateTimeUtils.getZoneId(). Sets the offset of 'result' for fixed
// offsets and its time zone for regions. Returns false for invalid zones.
bool tryResolveSparkZoneId(
    std::string_view zone,
    ParsedTimestampWithTimeZone& result) {
  // Spark pads single-digit hours and minutes of offsets with a zero, i.e. the
  // first "+h:" anywhere and "+hh:m" at the end, also with '-' signs.
  std::string id(zone);
  for (size_t i = 0; i + 2 < id.size(); ++i) {
    if ((id[i] == '+' || id[i] == '-') && characterIsDigit(id[i + 1]) &&
        id[i + 2] == ':') {
      id.insert(i + 1, 1, '0');
      break;
    }
  }
  const auto size = id.size();
  if (size >= 5 && (id[size - 5] == '+' || id[size - 5] == '-') &&
      characterIsDigit(id[size - 4]) && characterIsDigit(id[size - 3]) &&
      id[size - 2] == ':' && characterIsDigit(id[size - 1])) {
    id.insert(size - 1, 1, '0');
  }

  // Java's ZoneId.SHORT_IDS, which Spark resolves before ZoneId.of().
  static constexpr std::pair<std::string_view, std::string_view> kShortIds[] = {
      {"ACT", "Australia/Darwin"},
      {"AET", "Australia/Sydney"},
      {"AGT", "America/Argentina/Buenos_Aires"},
      {"ART", "Africa/Cairo"},
      {"AST", "America/Anchorage"},
      {"BET", "America/Sao_Paulo"},
      {"BST", "Asia/Dhaka"},
      {"CAT", "Africa/Harare"},
      {"CNT", "America/St_Johns"},
      {"CST", "America/Chicago"},
      {"CTT", "Asia/Shanghai"},
      {"EAT", "Africa/Addis_Ababa"},
      {"ECT", "Europe/Paris"},
      {"EST", "-05:00"},
      {"HST", "-10:00"},
      {"IET", "America/Indiana/Indianapolis"},
      {"IST", "Asia/Kolkata"},
      {"JST", "Asia/Tokyo"},
      {"MIT", "Pacific/Apia"},
      {"MST", "-07:00"},
      {"NET", "Asia/Yerevan"},
      {"NST", "Pacific/Auckland"},
      {"PLT", "Asia/Karachi"},
      {"PNT", "America/Phoenix"},
      {"PRT", "America/Puerto_Rico"},
      {"PST", "America/Los_Angeles"},
      {"SST", "Pacific/Guadalcanal"},
      {"VST", "Asia/Ho_Chi_Minh"}};
  for (const auto& [shortId, target] : kShortIds) {
    if (id == shortId) {
      id = target;
      break;
    }
  }

  // Like Java's ZoneId.of(). The prefixes UTC, GMT and UT alone are UTC, and
  // followed by a sign they start an offset.
  std::optional<int32_t> offset;
  if (id.size() <= 1 || id[0] == '+' || id[0] == '-') {
    offset = tryParseJavaZoneOffset(id);
  } else {
    size_t prefixSize = 0;
    if (id.starts_with("UTC") || id.starts_with("GMT")) {
      prefixSize = 3;
    } else if (id.starts_with("UT")) {
      prefixSize = 2;
    }
    if (prefixSize == 0 ||
        (id.size() > prefixSize && id[prefixSize] != '+' &&
         id[prefixSize] != '-')) {
      return tryResolveJavaRegion(id, result);
    }
    if (id.size() == prefixSize) {
      offset = 0;
    } else {
      offset = tryParseJavaZoneOffset(std::string_view(id).substr(prefixSize));
    }
  }
  if (!offset.has_value()) {
    return false;
  }
  result.offsetMillis = *offset * kMillisPerSecond;
  return true;
}

} // namespace

bool isLeapYear(int32_t year) {
  return year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
}

bool isValidDate(int32_t year, int32_t month, int32_t day) {
  if (month < 1 || month > 12) {
    return false;
  }
  if (year < kMinYear || year > kMaxYear) {
    return false;
  }
  if (day < 1) {
    return false;
  }
  return isLeapYear(year) ? day <= kLeapDays[month] : day <= kNormalDays[month];
}

bool isValidDayOfYear(int32_t year, int32_t dayOfYear) {
  if (year < kMinYear || year > kMaxYear) {
    return false;
  }
  if (dayOfYear < 1 || dayOfYear > 365 + isLeapYear(year)) {
    return false;
  }
  return true;
}

Expected<int64_t> lastDayOfMonthSinceEpochFromDate(const std::tm& dateTime) {
  auto year = dateTime.tm_year + 1900;
  auto month = dateTime.tm_mon + 1;
  auto day = util::getMaxDayOfMonth(year, month);
  return util::daysSinceEpochFromDate(year, month, day);
}

int32_t getMaxDayOfMonth(int32_t year, int32_t month) {
  return isLeapYear(year) ? kLeapDays[month] : kNormalDays[month];
}

Expected<int64_t>
daysSinceEpochFromDate(int32_t year, int32_t month, int32_t day) {
  // Fast path: years strictly inside the fast-date inverse algorithm's
  // exact domain. The algorithm's date range is bounded by Mar 1 kYearMin
  // and Feb 28 kYearMax, not the full calendar year on either end, so the
  // boundary years are excluded entirely to avoid the month-dependent
  // wrinkle. Loses 2 years of fast-path coverage out of ~3 million —
  // negligible. Years outside the strict range delegate to
  // WideRangeDateConversion, which validates and handles the full
  // [kMinYear, kMaxYear] domain via the loop-based fallback.
  if (FOLLY_LIKELY(year > fast_date::kYearMin && year < fast_date::kYearMax)) {
    if (!isValidDate(year, month, day)) {
      if (threadSkipErrorDetails()) {
        return folly::makeUnexpected(Status::UserError());
      }
      return folly::makeUnexpected(
          Status::UserError("Date out of range: {}-{}-{}", year, month, day));
    }
    return static_cast<int64_t>(ymdToDays(
        year, static_cast<uint32_t>(month), static_cast<uint32_t>(day)));
  }
  return WideRangeDateConversion::daysSinceEpochFromDate(year, month, day);
}

Expected<int64_t> daysSinceEpochFromWeekDate(
    int32_t weekYear,
    int32_t weekOfYear,
    int32_t dayOfWeek) {
  if (!isValidWeekDate(weekYear, weekOfYear, dayOfWeek)) {
    if (threadSkipErrorDetails()) {
      return folly::makeUnexpected(Status::UserError());
    }
    return folly::makeUnexpected(
        Status::UserError(
            "Date out of range: {}-{}-{}", weekYear, weekOfYear, dayOfWeek));
  }

  return daysSinceEpochFromDate(weekYear, 1, 4)
      .then([&weekOfYear, &dayOfWeek](int64_t daysSinceEpochOfJanFourth) {
        int32_t firstDayOfWeekYear =
            extractISODayOfTheWeek(daysSinceEpochOfJanFourth);

        return daysSinceEpochOfJanFourth - (firstDayOfWeekYear - 1) +
            7 * (weekOfYear - 1) + dayOfWeek - 1;
      });
}

Expected<int64_t> daysSinceEpochFromWeekOfMonthDate(
    int32_t year,
    int32_t month,
    int32_t weekOfMonth,
    int32_t dayOfWeek,
    bool lenient) {
  if (!lenient &&
      !isValidWeekOfMonthDate(year, month, weekOfMonth, dayOfWeek)) {
    if (threadSkipErrorDetails()) {
      return folly::makeUnexpected(Status::UserError());
    }
    return folly::makeUnexpected(
        Status::UserError(
            "Date out of range: {}-{}-{}-{}",
            year,
            month,
            weekOfMonth,
            dayOfWeek));
  }

  // Adjusts the year and month to ensure month is within the range 1-12,
  // accounting for overflow or underflow.
  int32_t additionYears = 0;
  if (month < 1) {
    additionYears = month / 12 - 1;
    month = 12 - abs(month) % 12;
  } else if (month > 12) {
    additionYears = (month - 1) / 12;
    month = (month - 1) % 12 + 1;
  }
  year += additionYears;

  return daysSinceEpochFromDate(year, month, 1)
      .then(
          [&dayOfWeek, &weekOfMonth](int64_t daysSinceEpochOfFirstDayOfMonth) {
            const int32_t firstDayOfWeek =
                extractISODayOfTheWeek(daysSinceEpochOfFirstDayOfMonth);
            int32_t days;
            if (dayOfWeek < 1) {
              days = 7 - abs(dayOfWeek - 1) % 7;
            } else if (dayOfWeek > 7) {
              days = (dayOfWeek - 1) % 7;
            } else {
              days = dayOfWeek % 7;
            }
            return daysSinceEpochOfFirstDayOfMonth - (firstDayOfWeek - 1) +
                7 * (weekOfMonth - 1) + days - 1;
          });
}

Expected<int64_t> daysSinceEpochFromDayOfYear(int32_t year, int32_t dayOfYear) {
  if (!isValidDayOfYear(year, dayOfYear)) {
    if (threadSkipErrorDetails()) {
      return folly::makeUnexpected(Status::UserError());
    }
    return folly::makeUnexpected(
        Status::UserError("Day of year out of range: {}", dayOfYear));
  }
  return daysSinceEpochFromDate(year, 1, 1)
      .then([&dayOfYear](int64_t startOfYear) {
        return startOfYear + (dayOfYear - 1);
      });
}

Expected<int32_t> fromDateString(const char* str, size_t len, ParseMode mode) {
  int64_t daysSinceEpoch;
  size_t pos = 0;

  if (!tryParseDateString(str, len, pos, daysSinceEpoch, mode)) {
    if (threadSkipErrorDetails()) {
      return folly::makeUnexpected(Status::UserError());
    }

    switch (mode) {
      case ParseMode::kPrestoCast:
        return folly::makeUnexpected(
            Status::UserError(
                "Unable to parse date value: \"{}\". "
                "Valid date string pattern is (YYYY-MM-DD), "
                "and can be prefixed with [+-]",
                std::string(str, len)));
      case ParseMode::kSparkCast:
        return folly::makeUnexpected(
            Status::UserError(
                "Unable to parse date value: \"{}\". "
                "Valid date string patterns include "
                "([y]y*, [y]y*-[m]m*, [y]y*-[m]m*-[d]d*, "
                "[y]y*-[m]m*-[d]d* *, [y]y*-[m]m*-[d]d*T*), "
                "and any pattern prefixed with [+-]",
                std::string(str, len)));
      case ParseMode::kIso8601:
        return folly::makeUnexpected(
            Status::UserError(
                "Unable to parse date value: \"{}\". "
                "Valid date string patterns include "
                "([y]y*, [y]y*-[m]m*, [y]y*-[m]m*-[d]d*, "
                "[y]y*-[m]m*-[d]d* *), "
                "and any pattern prefixed with [+-]",
                std::string(str, len)));
      default:
        VELOX_UNREACHABLE();
    }
  }
  return daysSinceEpoch;
}

int32_t extractISODayOfTheWeek(int64_t daysSinceEpoch) {
  // date of 0 is 1970-01-01, which was a Thursday (4)
  // -7 = 4
  // -6 = 5
  // -5 = 6
  // -4 = 7
  // -3 = 1
  // -2 = 2
  // -1 = 3
  // 0  = 4
  // 1  = 5
  // 2  = 6
  // 3  = 7
  // 4  = 1
  // 5  = 2
  // 6  = 3
  // 7  = 4
  if (daysSinceEpoch < 0) {
    // negative date: start off at 4 and cycle downwards
    return (7 - ((-int128_t(daysSinceEpoch) + 3) % 7));
  } else {
    // positive date: start off at 4 and cycle upwards
    return ((int128_t(daysSinceEpoch) + 3) % 7) + 1;
  }
}

int64_t
fromTime(int32_t hour, int32_t minute, int32_t second, int32_t microseconds) {
  int64_t result;
  result = hour; // hours
  result = result * kMinsPerHour + minute; // hours -> minutes
  result = result * kSecsPerMinute + second; // minutes -> seconds
  result = result * kMicrosPerSec + microseconds; // seconds -> microseconds
  return result;
}

Timestamp fromDatetime(int64_t daysSinceEpoch, int64_t microsSinceMidnight) {
  int64_t secondsSinceEpoch =
      static_cast<int64_t>(daysSinceEpoch) * kSecsPerDay;
  secondsSinceEpoch += microsSinceMidnight / kMicrosPerSec;
  return Timestamp(
      secondsSinceEpoch,
      (microsSinceMidnight % kMicrosPerSec) * kNanosPerMicro);
}

namespace {

Status parserError(const char* str, size_t len) {
  if (threadSkipErrorDetails()) {
    return Status::UserError();
  }
  return Status::UserError(
      "Unable to parse timestamp value: \"{}\", "
      "expected format is (YYYY-MM-DD HH:MM:SS[.MS])",
      std::string(str, len));
}

} // namespace

Expected<Timestamp>
fromTimestampString(const char* str, size_t len, TimestampParseMode parseMode) {
  if (parseMode == TimestampParseMode::kSparkCast) {
    Timestamp timestamp;
    std::string_view timeZone;
    if (!tryParseSparkTimestampString(str, len, timestamp, timeZone) ||
        !timeZone.empty()) {
      return folly::makeUnexpected(parserError(str, len));
    }
    return timestamp;
  }

  size_t pos = 0;
  Timestamp resultTimestamp;

  if (!tryParseTimestampString(str, len, pos, resultTimestamp, parseMode)) {
    return folly::makeUnexpected(parserError(str, len));
  }
  skipSpaces(str, len, pos);

  // If not all input was consumed.
  if (pos < len) {
    return folly::makeUnexpected(parserError(str, len));
  }
  VELOX_CHECK_EQ(pos, len);
  return resultTimestamp;
}

Expected<ParsedTimestampWithTimeZone> fromTimestampWithTimezoneString(
    const char* str,
    size_t len,
    TimestampParseMode parseMode) {
  if (parseMode == TimestampParseMode::kSparkCast) {
    Timestamp timestamp;
    std::string_view timeZoneName;
    if (!tryParseSparkTimestampString(str, len, timestamp, timeZoneName)) {
      return folly::makeUnexpected(parserError(str, len));
    }
    ParsedTimestampWithTimeZone result{timestamp, nullptr, std::nullopt};
    if (!timeZoneName.empty() && !tryResolveSparkZoneId(timeZoneName, result)) {
      return folly::makeUnexpected(
          Status::UserError("Unknown timezone value: \"{}\"", timeZoneName));
    }
    return result;
  }

  size_t pos = 0;
  Timestamp resultTimestamp;

  if (!tryParseTimestampString(str, len, pos, resultTimestamp, parseMode)) {
    return folly::makeUnexpected(parserError(str, len));
  }

  const tz::TimeZone* timeZone = nullptr;
  std::optional<int64_t> offset = std::nullopt;

  if (pos < len && parseMode != TimestampParseMode::kIso8601 &&
      characterIsSpace(str[pos])) {
    pos++;
  }

  // If there is anything left to parse, it must be a timezone definition.
  if (pos < len) {
    if (parseMode == TimestampParseMode::kIso8601) {
      // Only +HH:MM and -HH:MM are supported. Minutes, seconds, etc. in the
      // offset are optional.
      if (str[pos] != 'Z' && str[pos] != '+' && str[pos] != '-') {
        return folly::makeUnexpected(parserError(str, len));
      }
    }

    size_t timezonePos = pos;
    while (timezonePos < len && !characterIsSpace(str[timezonePos])) {
      timezonePos++;
    }

    std::string_view timeZoneName(str + pos, timezonePos - pos);

    if ((timeZone = tz::locateZone(timeZoneName, false)) == nullptr) {
      int64_t offsetMillis = 0;
      size_t offsetPos = 0;
      if (parseMode == TimestampParseMode::kPrestoCast &&
          tryParsePrestoTimeOffsetString(
              str + pos, timezonePos - pos, offsetPos, offsetMillis)) {
        offset = offsetMillis;
      } else {
        return folly::makeUnexpected(
            Status::UserError("Unknown timezone value: \"{}\"", timeZoneName));
      }
    }

    // Skip any spaces at the end.
    pos = timezonePos;
    if (parseMode != TimestampParseMode::kIso8601) {
      skipSpaces(str, len, pos);
    }

    if (pos < len) {
      return folly::makeUnexpected(parserError(str, len));
    }
  }
  return {{resultTimestamp, timeZone, offset}};
}

Timestamp fromParsedTimestampWithTimeZone(
    ParsedTimestampWithTimeZone parsed,
    const tz::TimeZone* sessionTimeZone) {
  if (parsed.timeZone) {
    parsed.timestamp.toGMT(*parsed.timeZone);
  } else if (parsed.offsetMillis.has_value()) {
    auto seconds = parsed.timestamp.getSeconds();
    // use int128_t to avoid overflow.
    __int128_t nanos = parsed.timestamp.getNanos();
    seconds -= parsed.offsetMillis.value() / util::kMillisPerSecond;
    nanos -= (parsed.offsetMillis.value() % util::kMillisPerSecond) *
        util::kNanosPerMicro * util::kMicrosPerMsec;
    if (nanos < 0) {
      seconds -= 1;
      nanos += Timestamp::kNanosInSecond;
    } else if (nanos > Timestamp::kMaxNanos) {
      seconds += 1;
      nanos -= Timestamp::kNanosInSecond;
    }
    parsed.timestamp = Timestamp(seconds, nanos);
  } else {
    if (sessionTimeZone) {
      parsed.timestamp.toGMT(*sessionTimeZone);
    }
  }
  return parsed.timestamp;
}

int32_t toDate(const Timestamp& timestamp, const tz::TimeZone* timeZone_) {
  auto convertToDate = [](const Timestamp& t) -> int32_t {
    static const int32_t kSecsPerDay{86'400};
    auto seconds = t.getSeconds();
    if (seconds >= 0 || seconds % kSecsPerDay == 0) {
      return seconds / kSecsPerDay;
    }
    // For division with negatives, minus 1 to compensate the discarded
    // fractional part. e.g. -1/86'400 yields 0, yet it should be considered
    // as -1 day.
    return seconds / kSecsPerDay - 1;
  };

  if (timeZone_ != nullptr) {
    Timestamp copy = timestamp;
    copy.toTimezone(*timeZone_);
    return convertToDate(copy);
  }

  return convertToDate(timestamp);
}

} // namespace facebook::velox::util
