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

#include "velox/functions/sparksql/specialforms/SparkCastHooks.h"

#include <algorithm>
#include <bit>
#include <cmath>
#include <cstdint>
#include <exception>
#include <limits>
#include <optional>
#include <string_view>

#include <folly/Range.h>

#include "velox/common/base/VeloxException.h"
#include "velox/functions/lib/string/StringImpl.h"
#include "velox/functions/sparksql/SparkQueryConfig.h"
#include "velox/functions/sparksql/TimestampUtils.h"
#include "velox/type/TimestampConversion.h"
#include "velox/type/Type.h"
#include "velox/type/tz/TimeZoneMap.h"

namespace facebook::velox::functions::sparksql {

namespace {

// Max DATE magnitude before TIMESTAMP_NTZ's microseconds-since-epoch range
// overflows. Spark throws here unconditionally, ANSI or not.
constexpr int64_t kMaxTimestampUtcDays = 106'751'991;

bool equalsIgnoreAsciiCase(std::string_view text, std::string_view expected) {
  return text.size() == expected.size() &&
      std::equal(
             text.begin(),
             text.end(),
             expected.begin(),
             folly::AsciiCaseInsensitive{});
}

bool isAsciiDigit(char c) {
  return c >= '0' && c <= '9';
}

// Returns the value of hexadecimal digit 'c', or -1 if 'c' is not one.
int hexDigitValue(char c) {
  if (isAsciiDigit(c)) {
    return c - '0';
  }
  if (c >= 'a' && c <= 'f') {
    return c - 'a' + 10;
  }
  if (c >= 'A' && c <= 'F') {
    return c - 'A' + 10;
  }
  return -1;
}

// Returns true if 'text' is a decimal number as Java writes it, without sign
// and type suffix: digits with an optional decimal point, then an optional
// exponent. For example, '5.', '.5' and '1.5e-3'.
bool isJavaDecimal(std::string_view text) {
  size_t pos = 0;
  size_t numDigits = 0;
  bool seenDot = false;
  for (; pos < text.size(); ++pos) {
    if (isAsciiDigit(text[pos])) {
      ++numDigits;
    } else if (text[pos] == '.' && !seenDot) {
      seenDot = true;
    } else {
      break;
    }
  }
  if (numDigits == 0) {
    return false;
  }
  if (pos < text.size() && (text[pos] == 'e' || text[pos] == 'E')) {
    ++pos;
    if (pos < text.size() && (text[pos] == '+' || text[pos] == '-')) {
      ++pos;
    }
    const size_t exponentStart = pos;
    while (pos < text.size() && isAsciiDigit(text[pos])) {
      ++pos;
    }
    if (pos == exponentStart) {
      return false;
    }
  }
  return pos == text.size();
}

// Parses a hexadecimal number as Java writes it, after the sign and '0x' and
// without type suffix. For example, '1.8p1' for '-0x1.8p1f'. The binary
// exponent after 'p' is required. Rounds to the nearest value of T, and to the
// even one for ties. Returns std::nullopt if 'text' is malformed.
template <typename T>
std::optional<T> parseHexFloatingPoint(std::string_view text, bool negative) {
  // The value is significand * 2^exponent. The significand keeps the leading
  // 61 to 64 bits. Rounding only needs to know if the other digits are zero.
  uint64_t significand = 0;
  bool sticky = false;
  int64_t exponent = 0;
  size_t numDigits = 0;
  bool seenDot = false;
  size_t pos = 0;
  for (; pos < text.size(); ++pos) {
    if (text[pos] == '.' && !seenDot) {
      seenDot = true;
      continue;
    }
    const int digit = hexDigitValue(text[pos]);
    if (digit < 0) {
      break;
    }
    ++numDigits;
    if (significand < (uint64_t{1} << 60)) {
      significand = significand * 16 + digit;
      if (seenDot) {
        exponent -= 4;
      }
    } else {
      sticky |= digit != 0;
      if (!seenDot) {
        exponent += 4;
      }
    }
  }
  if (numDigits == 0 || pos == text.size() ||
      (text[pos] != 'p' && text[pos] != 'P')) {
    return std::nullopt;
  }
  ++pos;
  bool negativeExponent = false;
  if (pos < text.size() && (text[pos] == '+' || text[pos] == '-')) {
    negativeExponent = text[pos] == '-';
    ++pos;
  }
  const size_t exponentStart = pos;
  int64_t binaryExponent = 0;
  for (; pos < text.size() && isAsciiDigit(text[pos]); ++pos) {
    if (binaryExponent <= std::numeric_limits<int32_t>::max()) {
      binaryExponent = binaryExponent * 10 + (text[pos] - '0');
    }
  }
  if (pos == exponentStart || pos != text.size()) {
    return std::nullopt;
  }

  using Limits = std::numeric_limits<T>;
  const T zero = negative ? -T(0) : T(0);
  const T infinity = negative ? -Limits::infinity() : Limits::infinity();
  if (significand == 0) {
    return zero;
  }
  // Java reads the exponent as an int. Larger ones give zero or infinity.
  if (binaryExponent > std::numeric_limits<int32_t>::max()) {
    return negativeExponent ? zero : infinity;
  }
  exponent += negativeExponent ? -binaryExponent : binaryExponent;

  // Move the leading one bit to bit 63. The value is then in
  // [2^topExponent, 2^(topExponent + 1)).
  const int shift = std::countl_zero(significand);
  significand <<= shift;
  exponent -= shift;
  const int64_t topExponent = exponent + 63;
  if (topExponent > Limits::max_exponent - 1) {
    return infinity;
  }
  // Subnormal values keep fewer bits.
  const int64_t keptBits = Limits::digits -
      std::max<int64_t>(0, Limits::min_exponent - 1 - topExponent);
  if (keptBits < 0) {
    return zero;
  }
  const int droppedBits = static_cast<int>(64 - keptBits);
  uint64_t kept = droppedBits == 64 ? 0 : significand >> droppedBits;
  const uint64_t remainder = droppedBits == 64
      ? significand
      : significand & ((uint64_t{1} << droppedBits) - 1);
  const uint64_t half = uint64_t{1} << (droppedBits - 1);
  if (remainder > half || (remainder == half && (sticky || (kept & 1)))) {
    ++kept;
  }
  const T value = std::ldexp(
      static_cast<T>(kept), static_cast<int>(exponent + droppedBits));
  return negative ? -value : value;
}

// Spark parses the string with Java's Float.parseFloat or Double.parseDouble.
// If that fails, Spark accepts 'inf', 'infinity' and 'nan' in any letter case,
// the first two with an optional sign.
template <typename T>
Expected<T> castStringToFloatingPoint(std::string_view input) {
  using Limits = std::numeric_limits<T>;
  const bool negative = !input.empty() && input[0] == '-';
  const size_t signLength =
      !input.empty() && (input[0] == '+' || input[0] == '-') ? 1 : 0;
  std::string_view text = input.substr(signLength);
  // Java accepts 'NaN' with an optional sign.
  if (text == "NaN" ||
      (signLength == 0 && equalsIgnoreAsciiCase(text, "nan"))) {
    return Limits::quiet_NaN();
  }
  if (equalsIgnoreAsciiCase(text, "inf") ||
      equalsIgnoreAsciiCase(text, "infinity")) {
    return negative ? -Limits::infinity() : Limits::infinity();
  }
  // Java accepts a type suffix after a number, e.g. '1.5f' or '0x1p3d'.
  if (!text.empty() &&
      (text.back() == 'f' || text.back() == 'F' || text.back() == 'd' ||
       text.back() == 'D')) {
    text.remove_suffix(1);
  }
  if (text.size() >= 2 && text[0] == '0' &&
      (text[1] == 'x' || text[1] == 'X')) {
    if (const auto value = parseHexFloatingPoint<T>(text.substr(2), negative)) {
      return *value;
    }
  } else if (isJavaDecimal(text)) {
    constexpr auto kKind =
        std::is_same_v<T, float> ? TypeKind::REAL : TypeKind::DOUBLE;
    return util::Converter<kKind>::tryCast(
        input.substr(0, signLength + text.size()));
  }
  if (threadSkipErrorDetails()) {
    return folly::makeUnexpected(Status::UserError());
  }
  return folly::makeUnexpected(
      Status::UserError("Invalid floating-point number"));
}

} // namespace

SparkCastHooks::SparkCastHooks(
    const velox::core::QueryConfig& config,
    bool allowOverflow)
    : config_(config), allowOverflow_(allowOverflow) {
  const auto sessionTzName = config.sessionTimezone();
  if (!sessionTzName.empty()) {
    timestampToStringOptions_.timeZone = tz::locateZone(sessionTzName);
  }
}

TimestampToStringOptions SparkCastHooks::timestampUtcToStringOptions() const {
  auto options = timestampToStringOptions_;
  options.timeZone = nullptr;
  return options;
}

Expected<Timestamp> SparkCastHooks::castStringToTimestamp(
    const StringView& view,
    bool adjustTimezone) const {
  auto conversionResult = util::fromTimestampWithTimezoneString(
      view.data(), view.size(), util::TimestampParseMode::kSparkCast);
  if (conversionResult.hasError()) {
    return folly::makeUnexpected(conversionResult.error());
  }
  if (!adjustTimezone) {
    // For TIMESTAMP UTC, ignore any timezone suffix and store the parsed
    // timestamp fields as-is, not subject to session timezone adjustment.
    return conversionResult.value().timestamp;
  }
  auto sessionTimezone = config_.sessionTimezone().empty()
      ? nullptr
      : tz::locateZone(config_.sessionTimezone());
  return util::fromParsedTimestampWithTimeZone(
      conversionResult.value(), sessionTimezone);
}

template <typename T>
Expected<Timestamp> SparkCastHooks::castNumberToTimestamp(
    T value,
    bool allowOverflow) const {
  static constexpr long double kMaxTimestampMicros =
      static_cast<long double>(std::numeric_limits<int64_t>::max());
  static constexpr long double kMinTimestampMicros =
      static_cast<long double>(std::numeric_limits<int64_t>::min());

  const long double micros =
      static_cast<long double>(value) * Timestamp::kMicrosecondsInSecond;

  if (micros > kMaxTimestampMicros) {
    if (allowOverflow) {
      return Timestamp::fromMicrosNoError(std::numeric_limits<int64_t>::max());
    }
    return folly::makeUnexpected(
        Status::UserError(
            "The value cannot be cast to TIMESTAMP due to an overflow."));
  }

  if (micros < kMinTimestampMicros) {
    if (allowOverflow) {
      return Timestamp::fromMicrosNoError(std::numeric_limits<int64_t>::min());
    }
    return folly::makeUnexpected(
        Status::UserError(
            "The value cannot be cast to TIMESTAMP due to an overflow."));
  }

  return Timestamp::fromMicrosNoError(static_cast<int64_t>(micros));
}

Expected<Timestamp> SparkCastHooks::castIntToTimestamp(int64_t seconds) const {
  return castNumberToTimestamp(seconds, true);
}

Expected<int64_t> SparkCastHooks::castTimestampToBigint(
    Timestamp timestamp) const {
  auto micros = timestamp.toMicros();
  if (micros < 0) {
    return std::floor(
        static_cast<double>(micros) / Timestamp::kMicrosecondsInSecond);
  }
  return micros / Timestamp::kMicrosecondsInSecond;
}

Expected<std::optional<Timestamp>> SparkCastHooks::castDoubleToTimestamp(
    double value) const {
  if (allowOverflow_) {
    if (FOLLY_UNLIKELY(std::isnan(value) || std::isinf(value))) {
      return std::nullopt;
    }

    auto result = castNumberToTimestamp(value, allowOverflow_);
    VELOX_DCHECK(!result.hasError());
    return result.value();
  }

  if (FOLLY_UNLIKELY(std::isnan(value) || std::isinf(value))) {
    return folly::makeUnexpected(
        Status::UserError(
            "The value cannot be cast to TIMESTAMP because it is malformed."));
  }

  auto result = castNumberToTimestamp(value, allowOverflow_);
  if (result.hasError()) {
    return folly::makeUnexpected(result.error());
  }
  return result.value();
}

Expected<Timestamp> SparkCastHooks::castBooleanToTimestamp(bool val) const {
  return Timestamp::fromMicrosNoError(val ? 1 : 0);
}

bool SparkCastHooks::decimalToFloatHighPrecisionCastEnabled() const {
  return SparkQueryConfig{config_}.decimalToFloatHighPrecisionCastEnabled();
}

Expected<int32_t> SparkCastHooks::castStringToDate(
    const StringView& dateString) const {
  // Allows all patterns supported by Spark:
  // `[+-]yyyy*`
  // `[+-]yyyy*-[m]m`
  // `[+-]yyyy*-[m]m-[d]d`
  // `[+-]yyyy*-[m]m-[d]d *`
  // `[+-]yyyy*-[m]m-[d]dT*`
  // The asterisk `*` in `yyyy*` stands for any numbers.
  // For the last two patterns, the trailing `*` can represent none or any
  // sequence of characters, e.g:
  //   "1970-01-01 123"
  //   "1970-01-01 (BC)"
  return util::fromDateString(
      removeWhiteSpaces(dateString), util::ParseMode::kSparkCast);
}

Expected<int64_t> SparkCastHooks::castStringToTime(
    StringView timeString,
    const tz::TimeZone* /*timeZone*/,
    int64_t /*sessionStartTimeMs*/) const {
  try {
    return TIME_MICRO_UTC()->valueToTime(timeString, /*requireSeconds=*/false);
  } catch (const VeloxException& e) {
    return folly::makeUnexpected(Status::UserError(e.message()));
  } catch (const std::exception& e) {
    return folly::makeUnexpected(Status::UserError(e.what()));
  }
}

Expected<float> SparkCastHooks::castStringToReal(const StringView& data) const {
  return castStringToFloatingPoint<float>(data);
}

Expected<double> SparkCastHooks::castStringToDouble(
    const StringView& data) const {
  return castStringToFloatingPoint<double>(data);
}

StringView SparkCastHooks::removeWhiteSpaces(const StringView& view) const {
  StringView output;
  stringImpl::trimUnicodeWhiteSpace<true, true, StringView, StringView>(
      output, view);
  return output;
}

void SparkCastHooks::castDateTimestampToGMT(
    Timestamp& timestamp,
    const tz::TimeZone& timeZone) const {
  toGMTWithGapCorrection(timestamp, timeZone);
}

bool SparkCastHooks::isDateOverflowForTimestampUtc(int64_t days) const {
  return days > kMaxTimestampUtcDays || days < -kMaxTimestampUtcDays;
}

exec::PolicyType SparkCastHooks::getPolicy() const {
  if (!allowOverflow_) {
    return exec::PolicyType::SparkTryCastPolicy;
  }
  return exec::PolicyType::SparkCastPolicy;
}
} // namespace facebook::velox::functions::sparksql
