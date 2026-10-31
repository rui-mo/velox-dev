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

#include "velox/functions/sparksql/FormatNumber.h"

#include <fmt/format.h>
#include <algorithm>
#include <charconv>
#include <cmath>
#include <cstdint>
#include <limits>
#include <locale>
#include <string_view>

#include "velox/common/base/Exceptions.h"

namespace facebook::velox::functions::sparksql::detail {

namespace {

// Java DecimalFormat internally caps fraction digits at 340.
constexpr int32_t kMaxFractionDigits = 340;

constexpr double kTwoTo63 = 0x1p63;

// Custom numpunct that provides US-style thousands grouping without
// depending on system locale availability.
struct UsNumpunct : std::numpunct<char> {
  char do_thousands_sep() const override {
    return ',';
  }
  std::string do_grouping() const override {
    return "\3";
  }
  char do_decimal_point() const override {
    return '.';
  }
};

// Locale with US-style thousands grouping for fmt.
const std::locale& usLocale() {
  static const std::locale loc(std::locale::classic(), new UsNumpunct());
  return loc;
}

// Appends the number with the significant 'digits', whose first digit has the
// decimal 'exponent' (e.g. digits "123" and exponent 1 for 12.3), using a
// comma as thousands separator and exactly 'places' fraction digits. Digits
// beyond 'digits' are zeros.
void appendFixed(
    std::string_view digits,
    int32_t exponent,
    int32_t places,
    fmt::memory_buffer& buf) {
  const auto numDigits = static_cast<int32_t>(digits.size());
  const auto digitAt = [&](int32_t index) {
    return index >= 0 && index < numDigits ? digits[index] : '0';
  };
  if (exponent < 0) {
    buf.push_back('0');
  } else {
    for (int32_t i = 0; i <= exponent; ++i) {
      if (i > 0 && (exponent - i + 1) % 3 == 0) {
        buf.push_back(',');
      }
      buf.push_back(digitAt(i));
    }
  }
  if (places > 0) {
    buf.push_back('.');
    for (int32_t i = exponent + 1; i <= exponent + places; ++i) {
      buf.push_back(digitAt(i));
    }
  }
}

} // namespace

void formatInteger(
    int64_t value,
    int32_t decimalPlaces,
    exec::StringWriter& out) {
  int32_t cappedPlaces = std::min(decimalPlaces, kMaxFractionDigits);

  // Use fmt::memory_buffer (stack-allocated for typical sizes) to avoid
  // per-row heap allocation.
  fmt::memory_buffer buf;
  fmt::format_to(std::back_inserter(buf), usLocale(), "{:Ld}", value);

  if (cappedPlaces > 0) {
    buf.push_back('.');
    for (int32_t i = 0; i < cappedPlaces; ++i) {
      buf.push_back('0');
    }
  }

  out.append(std::string_view(buf.data(), buf.size()));
}

void formatFloatingPoint(
    double value,
    int32_t decimalPlaces,
    exec::StringWriter& out) {
  if (std::isnan(value)) {
    out.append("NaN");
    return;
  }
  if (std::isinf(value)) {
    // Java DecimalFormat with US locale uses the infinity symbol (U+221E).
    if (value < 0) {
      out.append("-\xE2\x88\x9E");
    } else {
      out.append("\xE2\x88\x9E");
    }
    return;
  }

  int32_t cappedPlaces = std::min(decimalPlaces, kMaxFractionDigits);

  // Use fmt::memory_buffer (stack-allocated for typical sizes) to avoid
  // per-row heap allocation.
  fmt::memory_buffer buf;
  if (std::signbit(value)) {
    buf.push_back('-');
  }

  // Java's DecimalFormat formats the decimal digits Java derives for a double
  // rather than its exact binary value, padding them with zeros. For integral
  // values below 2^63, these are the digits of the integer, with the last
  // digit rounded off (half up) from 2^58 and the last two from 2^61. For
  // other values, these are the shortest digits that uniquely distinguish the
  // value, as in Double.toString. Rounding them with HALF_EVEN, using the
  // exact value to break ties, gives the same result as rounding the exact
  // value. Java derives the digits with a legacy algorithm that, in rare
  // cases, produces more than the shortest digits (e.g. 9.999999999999999E22
  // for 1.0E23 and 4.9E-324 for Double.MIN_VALUE) or rounds a lone 5 down
  // (e.g. 5.0E-5 to 4 decimal places). These cases are not replicated.
  const double magnitude = std::abs(value);
  if (magnitude < kTwoTo63 && magnitude == std::trunc(magnitude)) {
    auto integer = static_cast<uint64_t>(magnitude);
    if (integer >= (uint64_t{1} << 58)) {
      const uint64_t scale = integer >= (uint64_t{1} << 61) ? 100 : 10;
      integer = (integer + scale / 2) / scale * scale;
    }
    const fmt::format_int digits(integer);
    appendFixed(
        std::string_view(digits.data(), digits.size()),
        static_cast<int32_t>(digits.size()) - 1,
        cappedPlaces,
        buf);
    out.append(std::string_view(buf.data(), buf.size()));
    return;
  }

  // Shortest digits in scientific notation, e.g. "1.2345e-07".
  char chars[32];
  const auto result = std::to_chars(
      chars, chars + sizeof(chars), magnitude, std::chars_format::scientific);
  VELOX_DCHECK(result.ec == std::errc());
  const std::string_view scientific(chars, result.ptr - chars);
  const auto exponentPos = scientific.find('e');
  char digits[std::numeric_limits<double>::max_digits10];
  int32_t numDigits = 0;
  for (const char c : scientific.substr(0, exponentPos)) {
    if (c != '.') {
      digits[numDigits++] = c;
    }
  }
  const char* exponentBegin = chars + exponentPos + 1;
  if (*exponentBegin == '+') {
    ++exponentBegin;
  }
  int32_t exponent = 0;
  std::from_chars(exponentBegin, result.ptr, exponent);

  if (cappedPlaces < numDigits - 1 - exponent) {
    fmt::format_to(
        std::back_inserter(buf),
        usLocale(),
        "{:.{}Lf}",
        magnitude,
        cappedPlaces);
  } else {
    appendFixed(
        std::string_view(digits, numDigits), exponent, cappedPlaces, buf);
  }
  out.append(std::string_view(buf.data(), buf.size()));
}

} // namespace facebook::velox::functions::sparksql::detail
