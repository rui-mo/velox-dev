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
#include <array>
#include <bit>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <limits>
#include <optional>
#include <string_view>

#include <folly/Range.h>

#include "velox/common/base/VeloxException.h"
#include "velox/functions/lib/Utf8Utils.h"
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

// The digit zero of each script whose decimal digits are in the Basic
// Multilingual Plane, as Java 21's Character.isDigit reports them. Each script
// encodes its digits 0 to 9 as 10 consecutive code points.
constexpr std::array<int32_t, 37> kDigitZeros = {
    0x0030, 0x0660, 0x06F0, 0x07C0, 0x0966, 0x09E6, 0x0A66, 0x0AE6,
    0x0B66, 0x0BE6, 0x0C66, 0x0CE6, 0x0D66, 0x0DE6, 0x0E50, 0x0ED0,
    0x0F20, 0x1040, 0x1090, 0x17E0, 0x1810, 0x1946, 0x19D0, 0x1A80,
    0x1A90, 0x1B50, 0x1BB0, 0x1C40, 0x1C50, 0xA620, 0xA8D0, 0xA900,
    0xA9D0, 0xA9F0, 0xAA50, 0xABF0, 0xFF10};

// Returns the value of 'codePoint' if it is a digit in kDigitZeros, or -1.
int32_t digitValue(int32_t codePoint) {
  for (const auto zero : kDigitZeros) {
    if (codePoint >= zero && codePoint < zero + 10) {
      return codePoint - zero;
    }
  }
  return -1;
}

// Returns whether the timestamp fits in Spark's range, i.e. microseconds since
// the epoch in an int64.
bool fitsInMicros(const Timestamp& timestamp) {
  const __int128_t micros = static_cast<__int128_t>(timestamp.getSeconds()) *
          Timestamp::kMicrosecondsInSecond +
      timestamp.getNanos() / Timestamp::kNanosecondsInMicrosecond;
  return micros >= std::numeric_limits<int64_t>::min() &&
      micros <= std::numeric_limits<int64_t>::max();
}

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
  auto& parsed = conversionResult.value();
  Timestamp result;
  if (!adjustTimezone) {
    // For TIMESTAMP UTC, ignore any timezone suffix and store the parsed
    // timestamp fields as-is, not subject to session timezone adjustment.
    result = parsed.timestamp;
  } else {
    const tz::TimeZone* timeZone = parsed.timeZone;
    if (timeZone == nullptr && !parsed.offsetMillis.has_value() &&
        !config_.sessionTimezone().empty()) {
      timeZone = tz::locateZone(config_.sessionTimezone());
    }
    if (timeZone != nullptr) {
      // Like Java's ZonedDateTime.of, moves a local time in a gap forward by
      // the length of the gap.
      result = parsed.timestamp;
      toGMTWithGapCorrection(result, *timeZone);
    } else {
      result = util::fromParsedTimestampWithTimeZone(parsed, nullptr);
    }
  }
  if (!fitsInMicros(result)) {
    if (threadSkipErrorDetails()) {
      return folly::makeUnexpected(Status::UserError());
    }
    return folly::makeUnexpected(
        Status::UserError(
            "The value cannot be cast to TIMESTAMP due to an overflow."));
  }
  return result;
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

Expected<double> SparkCastHooks::castTimestampToDouble(
    const Timestamp& timestamp) const {
  return static_cast<double>(timestamp.toMicros()) /
      Timestamp::kMicrosecondsInSecond;
}

Expected<bool> SparkCastHooks::castTimestampToBoolean(
    const Timestamp& timestamp) const {
  return timestamp.toMicros() != 0;
}

Expected<std::optional<Timestamp>> SparkCastHooks::castDoubleToTimestamp(
    double value) const {
  if (FOLLY_UNLIKELY(std::isnan(value) || std::isinf(value))) {
    if (allowOverflow_) {
      return std::nullopt;
    }
    return folly::makeUnexpected(
        Status::UserError(
            "The value cannot be cast to TIMESTAMP because it is malformed."));
  }

  // Spark computes the microseconds in double precision, then converts them
  // to int64. Its ANSI check compares against the int64 bounds converted to
  // double, so a product of exactly +/-2^63 clamps instead of overflowing.
  static constexpr double kTwoPow63 = 9223372036854775808.0;
  const double micros = value * Timestamp::kMicrosecondsInSecond;
  if (micros >= kTwoPow63 || micros <= -kTwoPow63) {
    if (!allowOverflow_ && (micros > kTwoPow63 || micros < -kTwoPow63)) {
      return folly::makeUnexpected(
          Status::UserError(
              "The value cannot be cast to TIMESTAMP due to an overflow."));
    }
    return Timestamp::fromMicrosNoError(
        micros > 0 ? std::numeric_limits<int64_t>::max()
                   : std::numeric_limits<int64_t>::min());
  }
  return Timestamp::fromMicrosNoError(static_cast<int64_t>(micros));
}

Expected<Timestamp> SparkCastHooks::castBooleanToTimestamp(bool val) const {
  return Timestamp::fromMicrosNoError(val ? 1 : 0);
}

bool SparkCastHooks::stringToDecimalCastRejectsZeroWithLargeExponent() const {
  return SparkQueryConfig{config_}.legacyDecimalZeroLargeExponent();
}

bool SparkCastHooks::decimalToFloatHighPrecisionCastEnabled() const {
  return SparkQueryConfig{config_}.decimalToFloatHighPrecisionCastEnabled();
}

namespace {

// Spark UTF8String.trimAll trims boundary bytes 0x00..0x20 and 0x7F.
bool isUtf8StringTrimAllByte(char value) {
  const auto byte = static_cast<uint8_t>(value);
  return byte <= 0x20 || byte == 0x7F;
}

// Java String.trim trims boundary characters <= U+0020.
bool isJavaStringTrimByte(char value) {
  return static_cast<uint8_t>(value) <= 0x20;
}

template <typename TShouldTrim>
StringView trimBytes(const StringView& view, TShouldTrim shouldTrim) {
  const auto* data = view.data();
  size_t start{0};
  size_t end{view.size()};

  while (start < end && shouldTrim(data[start])) {
    ++start;
  }

  while (end > start && shouldTrim(data[end - 1])) {
    --end;
  }

  return StringView(data + start, end - start);
}

StringView trimUtf8StringAll(const StringView& view) {
  return trimBytes(view, isUtf8StringTrimAllByte);
}

StringView trimJavaString(const StringView& view) {
  return trimBytes(view, isJavaStringTrimByte);
}

// Type::isDecimal() runs a dynamic_cast for BIGINT and HUGEINT types, which is
// too costly to call for every row. Plain BIGINT and HUGEINT types are
// singletons, so check their identity first. Decimal types are final, so a
// typeid comparison identifies them exactly.
bool isDecimalType(const Type& type) {
  if (&type == BigintType::create().get() ||
      &type == HugeintType::create().get()) {
    return false;
  }
  const auto& typeId = typeid(type);
  return typeId == typeid(ShortDecimalType) ||
      typeId == typeid(LongDecimalType);
}

} // namespace

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
      trimUtf8StringAll(dateString), util::ParseMode::kSparkCast);
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

// Keep these target-type-specific rules in sync with Spark. Spark currently
// uses UTF8String-style trimming for most casts but Java String.trim for
// floating point and decimal casts. See
// https://issues.apache.org/jira/browse/SPARK-59182.
StringView SparkCastHooks::removeWhiteSpaces(
    const StringView& view,
    const Type& toType) const {
  switch (toType.kind()) {
    case TypeKind::REAL:
    case TypeKind::DOUBLE:
      return trimJavaString(view);
    case TypeKind::BIGINT:
    case TypeKind::HUGEINT:
      return isDecimalType(toType) ? trimJavaString(view)
                                   : trimUtf8StringAll(view);
    default:
      return trimUtf8StringAll(view);
  }
}

StringView SparkCastHooks::toAsciiDigits(
    const StringView& data,
    std::string& buffer) const {
  const char* const end = data.data() + data.size();
  const char* nonAscii =
      std::find_if(data.data(), end, [](char c) { return !IS_ASCII(c); });
  if (nonAscii == end) {
    return data;
  }
  buffer.assign(data.data(), nonAscii);
  for (const char* pos = nonAscii; pos < end;) {
    int32_t codePoint;
    const auto length = tryGetUtf8CharLength(pos, end - pos, codePoint);
    // Invalid UTF-8 bytes are copied, so the parsing rejects them.
    const auto digit = length > 0 ? digitValue(codePoint) : -1;
    if (digit >= 0) {
      buffer.push_back(static_cast<char>('0' + digit));
    } else {
      buffer.append(pos, std::abs(length));
    }
    pos += std::abs(length);
  }
  return StringView(buffer);
}

void SparkCastHooks::castDateTimestampToGMT(
    Timestamp& timestamp,
    const tz::TimeZone& timeZone) const {
  toGMTWithGapCorrection(timestamp, timeZone);
}

bool SparkCastHooks::isDateOverflowForTimestampUtc(int64_t days) const {
  return days > kMaxTimestampUtcDays || days < -kMaxTimestampUtcDays;
}

bool SparkCastHooks::isDateOverflowForTimestamp(
    const Timestamp& timestamp) const {
  return !fitsInMicros(timestamp);
}

exec::PolicyType SparkCastHooks::getPolicy() const {
  if (!allowOverflow_) {
    return exec::PolicyType::SparkTryCastPolicy;
  }
  return exec::PolicyType::SparkCastPolicy;
}
} // namespace facebook::velox::functions::sparksql
