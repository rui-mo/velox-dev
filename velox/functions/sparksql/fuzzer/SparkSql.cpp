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
#include "velox/functions/sparksql/fuzzer/SparkSql.h"

#include <algorithm>
#include <cmath>
#include <unordered_map>
#include <unordered_set>

#include <boost/algorithm/string.hpp>
#include <fmt/format.h>
#include <folly/String.h>

#include "velox/type/DecimalUtil.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::functions::sparksql::fuzzer {
namespace {

bool isAsciiLetter(char c) {
  return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
}

bool isAsciiDigit(char c) {
  return c >= '0' && c <= '9';
}

std::string quoteIdentifier(std::string_view name) {
  std::string quoted = "`";
  for (const char c : name) {
    if (c == '`') {
      quoted += '`';
    }
    quoted += c;
  }
  quoted += '`';
  return quoted;
}

std::string toHex(std::string_view value) {
  static constexpr char kDigits[] = "0123456789ABCDEF";
  std::string hex;
  hex.reserve(value.size() * 2);
  for (const unsigned char c : value) {
    hex += kDigits[c >> 4];
    hex += kDigits[c & 0xF];
  }
  return hex;
}

std::string toBinarySql(std::string_view value) {
  return fmt::format("X'{}'", toHex(value));
}

// Spark substitutes variables like ${var} in the SQL text and the query must be
// valid UTF-8, so strings with other than printable ASCII characters or with
// '${' are converted from their bytes.
std::string toStringSql(std::string_view value) {
  const bool printable = std::all_of(
      value.begin(), value.end(), [](char c) { return c >= 0x20 && c < 0x7F; });
  if (!printable || value.find("${") != std::string_view::npos) {
    return fmt::format("CAST({} AS STRING)", toBinarySql(value));
  }
  std::string sql = "'";
  for (const char c : value) {
    if (c == '\'' || c == '\\') {
      sql += '\\';
    }
    sql += c;
  }
  sql += '\'';
  return sql;
}

// Negative values are cast from integer literals, so that the minimum values
// can be expressed and a minus sign never follows another one, which would
// start a comment.
std::string
toIntegerSql(int64_t value, std::string_view suffix, std::string_view typeSql) {
  if (value < 0) {
    return fmt::format("CAST({} AS {})", value, typeSql);
  }
  return fmt::format("{}{}", value, suffix);
}

// Spark has no literals for NaN and infinity, but casts them from strings.
template <typename T>
std::string toFloatingPointSql(T value, std::string_view typeSql) {
  if (std::isnan(value)) {
    return fmt::format("CAST('NaN' AS {})", typeSql);
  }
  if (std::isinf(value)) {
    return fmt::format(
        "CAST('{}Infinity' AS {})", value < 0 ? "-" : "", typeSql);
  }
  return fmt::format("CAST('{}' AS {})", value, typeSql);
}

std::string toTimestampSql(const Timestamp& value) {
  if (value.getNanos() % 1'000 != 0) {
    VELOX_UNSUPPORTED(
        "Spark timestamps have microsecond precision: {}", value.toString());
  }
  const auto micros = static_cast<__int128_t>(value.getSeconds()) * 1'000'000 +
      value.getNanos() / 1'000;
  if (micros < std::numeric_limits<int64_t>::min() ||
      micros > std::numeric_limits<int64_t>::max()) {
    VELOX_UNSUPPORTED("Timestamp is out of Spark range: {}", value.toString());
  }
  return fmt::format(
      "timestamp_micros({})",
      toIntegerSql(static_cast<int64_t>(micros), "L", "BIGINT"));
}

std::string toNullSql(const TypePtr& type) {
  return fmt::format("CAST(NULL AS {})", toSparkTypeSql(type));
}

std::string toArraySql(
    const TypePtr& type,
    const std::vector<std::string>& elements) {
  if (elements.empty()) {
    return fmt::format("CAST(array() AS {})", toSparkTypeSql(type));
  }
  return fmt::format("array({})", folly::join(", ", elements));
}

std::string toMapSql(
    const TypePtr& type,
    const std::vector<std::string>& keys,
    const std::vector<std::string>& values) {
  if (keys.empty()) {
    return fmt::format("CAST(map() AS {})", toSparkTypeSql(type));
  }
  std::unordered_set<std::string> uniqueKeys;
  std::vector<std::string> arguments;
  arguments.reserve(keys.size() * 2);
  for (auto i = 0; i < keys.size(); ++i) {
    // Spark fails on duplicate map keys.
    if (!uniqueKeys.insert(keys[i]).second) {
      VELOX_UNSUPPORTED("Duplicate map key: {}", keys[i]);
    }
    arguments.push_back(keys[i]);
    arguments.push_back(values[i]);
  }
  return fmt::format("map({})", folly::join(", ", arguments));
}

std::string toRowSql(
    const TypePtr& type,
    const std::vector<std::string>& children) {
  const auto names = toSparkFieldNames(type->asRow());
  if (names.empty()) {
    VELOX_UNSUPPORTED("Spark does not support structs without fields");
  }
  std::vector<std::string> arguments;
  arguments.reserve(names.size() * 2);
  for (auto i = 0; i < names.size(); ++i) {
    arguments.push_back(toStringSql(names[i]));
    arguments.push_back(children[i]);
  }
  return fmt::format("named_struct({})", folly::join(", ", arguments));
}

std::string toValueSql(const TypePtr& type, const Variant& value) {
  if (value.isNull()) {
    return toNullSql(type);
  }
  if (type->isDate()) {
    return fmt::format(
        "date_from_unix_date({})",
        toIntegerSql(value.value<TypeKind::INTEGER>(), "", "INT"));
  }
  if (type->isDecimal()) {
    const int128_t unscaled = type->isShortDecimal()
        ? value.value<TypeKind::BIGINT>()
        : value.value<TypeKind::HUGEINT>();
    return fmt::format(
        "CAST('{}' AS {})",
        DecimalUtil::toString(unscaled, *type),
        toSparkTypeSql(type));
  }
  if (type->isIntervalYearMonth()) {
    return fmt::format(
        "make_ym_interval(0, {})",
        toIntegerSql(value.value<TypeKind::INTEGER>(), "", "INT"));
  }
  switch (type->kind()) {
    case TypeKind::ARRAY: {
      std::vector<std::string> elements;
      for (const auto& element : value.array()) {
        elements.push_back(toValueSql(type->childAt(0), element));
      }
      return toArraySql(type, elements);
    }
    case TypeKind::MAP: {
      std::vector<std::string> keys;
      std::vector<std::string> values;
      for (const auto& [key, mapValue] : value.map()) {
        if (key.isNull()) {
          VELOX_UNSUPPORTED("Spark does not support null keys");
        }
        keys.push_back(toValueSql(type->childAt(0), key));
        values.push_back(toValueSql(type->childAt(1), mapValue));
      }
      return toMapSql(type, keys, values);
    }
    case TypeKind::ROW: {
      std::vector<std::string> children;
      const auto& fields = value.row();
      for (auto i = 0; i < fields.size(); ++i) {
        children.push_back(toValueSql(type->childAt(i), fields[i]));
      }
      return toRowSql(type, children);
    }
    default:
      break;
  }

  // Throws for types without a Spark equivalent.
  const auto typeSql = toSparkTypeSql(type);
  switch (type->kind()) {
    case TypeKind::BOOLEAN:
      return value.value<TypeKind::BOOLEAN>() ? "true" : "false";
    case TypeKind::TINYINT:
      return toIntegerSql(value.value<TypeKind::TINYINT>(), "Y", typeSql);
    case TypeKind::SMALLINT:
      return toIntegerSql(value.value<TypeKind::SMALLINT>(), "S", typeSql);
    case TypeKind::INTEGER:
      return toIntegerSql(value.value<TypeKind::INTEGER>(), "", typeSql);
    case TypeKind::BIGINT:
      return toIntegerSql(value.value<TypeKind::BIGINT>(), "L", typeSql);
    case TypeKind::REAL:
      return toFloatingPointSql(value.value<TypeKind::REAL>(), typeSql);
    case TypeKind::DOUBLE:
      return toFloatingPointSql(value.value<TypeKind::DOUBLE>(), typeSql);
    case TypeKind::VARCHAR:
      return toStringSql(value.value<TypeKind::VARCHAR>());
    case TypeKind::VARBINARY:
      return toBinarySql(value.value<TypeKind::VARBINARY>());
    case TypeKind::TIMESTAMP:
      return toTimestampSql(value.value<TypeKind::TIMESTAMP>());
    default:
      VELOX_UNSUPPORTED(
          "Constants of type {} are not supported in Spark SQL",
          type->toString());
  }
}

// Returns the value of 'constant'.
Variant toVariant(const core::ConstantTypedExpr& constant) {
  if (constant.hasValueVector()) {
    return constant.valueVector()->variantAt(0);
  }
  return constant.value();
}

const core::ConstantTypedExpr* asConstant(const core::TypedExprPtr& expr) {
  return expr->isConstantKind() ? expr->asUnchecked<core::ConstantTypedExpr>()
                                : nullptr;
}

// Returns the value of 'expr' if it is a non-null constant.
std::optional<Variant> toConstantValue(const core::TypedExprPtr& expr) {
  const auto* constant = asConstant(expr);
  if (constant == nullptr || constant->isNull()) {
    return std::nullopt;
  }
  return toVariant(*constant);
}

// Returns the Spark name of the field 'name' of a struct of 'type'.
std::string toSparkFieldName(const RowType& type, const std::string& name) {
  std::optional<uint32_t> index;
  for (auto i = 0; i < type.size(); ++i) {
    if (type.nameOf(i) == name) {
      if (index.has_value()) {
        VELOX_UNSUPPORTED("Struct field name is ambiguous: {}", name);
      }
      index = i;
    }
  }
  VELOX_CHECK(index.has_value(), "Struct field not found: {}", name);
  return toSparkFieldNames(type)[*index];
}

std::string toDereferenceSql(
    const core::TypedExprPtr& input,
    const std::string& sparkName) {
  return fmt::format("({}).{}", toSparkSql(input), quoteIdentifier(sparkName));
}

std::vector<std::string> toSparkSqls(
    const std::vector<core::TypedExprPtr>& exprs) {
  std::vector<std::string> sqls;
  sqls.reserve(exprs.size());
  for (const auto& expr : exprs) {
    sqls.push_back(toSparkSql(expr));
  }
  return sqls;
}

std::string toLambdaSql(const core::LambdaTypedExpr& lambda) {
  const auto& names = lambda.signature()->names();
  if (names.empty()) {
    VELOX_UNSUPPORTED(
        "Spark does not support lambda functions without arguments");
  }
  std::vector<std::string> arguments;
  arguments.reserve(names.size());
  for (const auto& name : names) {
    arguments.push_back(toSparkIdentifier(name));
  }
  // Spark only accepts parentheses around two or more arguments.
  return fmt::format(
      "{} -> {}",
      arguments.size() == 1 ? arguments[0]
                            : fmt::format("({})", folly::join(", ", arguments)),
      toSparkSql(lambda.body()));
}

// Velox Spark functions that have no Spark SQL equivalent. They are internal
// functions of Gluten, depend on the data layout or are nondeterministic.
bool isUnsupportedFunction(const std::string& name) {
  static const std::unordered_set<std::string> kNames{
      "array_sort_desc",
      "assert_not_null",
      "char_type_write_side_check",
      "empty2null",
      "hash_with_seed",
      "might_contain",
      "monotonically_increasing_id",
      "rand",
      "randn",
      "random",
      "randstr",
      "read_side_padding",
      "shuffle",
      "spark_partition_id",
      "to_pretty_string",
      "try",
      "unscaled_value",
      "uuid",
      "varchar_type_write_side_check",
      "xxhash64_with_seed",
  };
  if (kNames.contains(name) || name.starts_with("checked_") ||
      name.ends_with("_deny_precision_loss")) {
    return true;
  }
  // Companion functions of aggregate functions, e.g. sum_extract_bigint.
  return !name.starts_with("regexp_") &&
      (name.ends_with("_extract") ||
       name.find("_extract_") != std::string::npos);
}

std::string toBinaryOperatorSql(
    const core::CallTypedExpr& call,
    std::string_view op) {
  if (call.inputs().size() != 2) {
    VELOX_UNSUPPORTED("Expected two arguments for {}", call.name());
  }
  return fmt::format(
      "({} {} {})",
      toSparkSql(call.inputs()[0]),
      op,
      toSparkSql(call.inputs()[1]));
}

std::string toLikeSql(const core::CallTypedExpr& call) {
  const auto& inputs = call.inputs();
  if (inputs.size() != 2 && inputs.size() != 3) {
    VELOX_UNSUPPORTED(
        "Unexpected number of arguments for like: {}", inputs.size());
  }
  auto sql =
      fmt::format("({} LIKE {}", toSparkSql(inputs[0]), toSparkSql(inputs[1]));
  if (inputs.size() == 3) {
    // Spark only accepts a string literal with a single character.
    const auto escape = toConstantValue(inputs[2]);
    if (!escape.has_value()) {
      VELOX_UNSUPPORTED("Spark requires a constant escape character");
    }
    const auto& escapeChar = escape->value<TypeKind::VARCHAR>();
    if (escapeChar.size() != 1 || toStringSql(escapeChar).starts_with("CAST")) {
      VELOX_UNSUPPORTED(
          "Spark requires a single character to escape: {}", escapeChar);
    }
    sql += fmt::format(" ESCAPE {}", toStringSql(escapeChar));
  }
  return sql + ")";
}

std::string toSwitchSql(const core::CallTypedExpr& call) {
  const auto inputs = toSparkSqls(call.inputs());
  if (inputs.size() < 2) {
    VELOX_UNSUPPORTED("Expected at least two arguments");
  }
  std::string sql = "CASE";
  for (auto i = 0; i + 1 < inputs.size(); i += 2) {
    sql += fmt::format(" WHEN {} THEN {}", inputs[i], inputs[i + 1]);
  }
  if (inputs.size() % 2 == 1) {
    sql += fmt::format(" ELSE {}", inputs.back());
  }
  return sql + " END";
}

// Velox only uses the values of a constant array.
std::string toInSql(const core::CallTypedExpr& call) {
  const auto& inputs = call.inputs();
  if (inputs.size() != 2) {
    VELOX_UNSUPPORTED("Expected two arguments for in");
  }
  const auto list = toConstantValue(inputs[1]);
  if (!list.has_value()) {
    VELOX_UNSUPPORTED("Spark SQL requires a constant list for IN");
  }
  const auto& elementType = inputs[1]->type()->childAt(0);
  std::vector<std::string> elements;
  for (const auto& element : list->array()) {
    elements.push_back(toValueSql(elementType, element));
  }
  if (elements.empty()) {
    VELOX_UNSUPPORTED("Spark SQL requires a non-empty list");
  }
  return fmt::format(
      "({} IN ({}))", toSparkSql(inputs[0]), folly::join(", ", elements));
}

// Velox has a flag for legacySizeOfNull, which returns -1 for null input like
// Spark with ANSI mode off.
std::string toSizeSql(const core::CallTypedExpr& call) {
  const auto& inputs = call.inputs();
  if (inputs.size() != 2) {
    VELOX_UNSUPPORTED("Expected two arguments for size");
  }
  const auto legacySizeOfNull = toConstantValue(inputs[1]);
  if (!legacySizeOfNull.has_value()) {
    VELOX_UNSUPPORTED("Spark SQL requires a constant legacySizeOfNull");
  }
  const auto input = toSparkSql(inputs[0]);
  if (legacySizeOfNull->value<TypeKind::BOOLEAN>()) {
    return fmt::format("size({})", input);
  }
  return fmt::format("IF({0} IS NULL, CAST(NULL AS INT), size({0}))", input);
}

// Spark supports the legacy behavior of negative indices with a config only.
std::string toArrayInsertSql(const core::CallTypedExpr& call) {
  const auto& inputs = call.inputs();
  if (inputs.size() != 4) {
    VELOX_UNSUPPORTED("Expected four arguments for array_insert");
  }
  const auto legacyNegativeIndex = toConstantValue(inputs[3]);
  if (!legacyNegativeIndex.has_value() ||
      legacyNegativeIndex->value<TypeKind::BOOLEAN>()) {
    VELOX_UNSUPPORTED(
        "Spark SQL only supports array_insert with legacyNegativeIndex false");
  }
  return fmt::format(
      "array_insert({}, {}, {})",
      toSparkSql(inputs[0]),
      toSparkSql(inputs[1]),
      toSparkSql(inputs[2]));
}

// Velox trim functions take the characters to trim first.
std::string toTrimSql(const core::CallTypedExpr& call) {
  const auto& inputs = call.inputs();
  if (inputs.size() == 1) {
    return fmt::format("{}({})", call.name(), toSparkSql(inputs[0]));
  }
  if (inputs.size() != 2) {
    VELOX_UNSUPPORTED("Unexpected number of arguments for {}", call.name());
  }
  static const std::unordered_map<std::string, std::string> kTrimTypes{
      {"trim", "BOTH"},
      {"btrim", "BOTH"},
      {"ltrim", "LEADING"},
      {"rtrim", "TRAILING"},
  };
  return fmt::format(
      "TRIM({} {} FROM {})",
      kTrimTypes.at(call.name()),
      toSparkSql(inputs[0]),
      toSparkSql(inputs[1]));
}

// Spark takes the unit as a keyword.
std::string toTimestampAddOrDiffSql(const core::CallTypedExpr& call) {
  const auto& inputs = call.inputs();
  if (inputs.size() != 3) {
    VELOX_UNSUPPORTED("Expected three arguments for {}", call.name());
  }
  static const std::unordered_set<std::string> kUnits{
      "YEAR",
      "QUARTER",
      "MONTH",
      "WEEK",
      "DAY",
      "DAYOFYEAR",
      "HOUR",
      "MINUTE",
      "SECOND",
      "MILLISECOND",
      "MICROSECOND",
  };
  const auto unit = toConstantValue(inputs[0]);
  if (!unit.has_value()) {
    VELOX_UNSUPPORTED("Spark SQL requires a constant unit for {}", call.name());
  }
  const auto unitSql =
      boost::algorithm::to_upper_copy(unit->value<TypeKind::VARCHAR>());
  if (!kUnits.contains(unitSql)) {
    VELOX_UNSUPPORTED("Unit is not supported by Spark: {}", unitSql);
  }
  return fmt::format(
      "{}({}, {}, {})",
      call.name(),
      unitSql,
      toSparkSql(inputs[1]),
      toSparkSql(inputs[2]));
}

std::string toCallSql(const core::CallTypedExpr& call) {
  const auto& name = call.name();
  if (isUnsupportedFunction(name)) {
    VELOX_UNSUPPORTED("Function is not supported in Spark SQL: {}", name);
  }

  static const std::unordered_map<std::string, std::string> kBinaryOperators{
      {"add", "+"},
      {"subtract", "-"},
      {"multiply", "*"},
      {"divide", "/"},
      {"remainder", "%"},
      {"div", "div"},
      {"equalto", "="},
      {"decimal_equalto", "="},
      {"decimal_notequalto", "!="},
      {"greaterthan", ">"},
      {"decimal_greaterthan", ">"},
      {"greaterthanorequal", ">="},
      {"decimal_greaterthanorequal", ">="},
      {"lessthan", "<"},
      {"decimal_lessthan", "<"},
      {"lessthanorequal", "<="},
      {"decimal_lessthanorequal", "<="},
      {"equalnullsafe", "<=>"},
      {"bitwise_and", "&"},
      {"bitwise_or", "|"},
      {"bitwise_xor", "^"},
  };
  if (const auto it = kBinaryOperators.find(name);
      it != kBinaryOperators.end()) {
    return toBinaryOperatorSql(call, it->second);
  }

  const auto& inputs = call.inputs();
  if (name == "and" || name == "or") {
    return fmt::format(
        "({})",
        folly::join(name == "and" ? " AND " : " OR ", toSparkSqls(inputs)));
  }
  if (name == "not" || name == "bitwise_not" || name == "unaryminus" ||
      name == "year_of_week") {
    if (inputs.size() != 1) {
      VELOX_UNSUPPORTED("Expected one argument for {}", name);
    }
    const auto input = toSparkSql(inputs[0]);
    if (name == "not") {
      return fmt::format("(NOT {})", input);
    }
    if (name == "bitwise_not") {
      return fmt::format("(~ {})", input);
    }
    if (name == "unaryminus") {
      return fmt::format("negative({})", input);
    }
    return fmt::format("extract(YEAROFWEEK FROM {})", input);
  }
  if (name == "between") {
    if (inputs.size() != 3) {
      VELOX_UNSUPPORTED("Expected three arguments for between");
    }
    return fmt::format(
        "({} BETWEEN {} AND {})",
        toSparkSql(inputs[0]),
        toSparkSql(inputs[1]),
        toSparkSql(inputs[2]));
  }
  if (name == "like") {
    return toLikeSql(call);
  }
  if (name == "if") {
    if (inputs.size() != 2 && inputs.size() != 3) {
      VELOX_UNSUPPORTED(
          "Unexpected number of arguments for if: {}", inputs.size());
    }
    return fmt::format(
        "IF({}, {}, {})",
        toSparkSql(inputs[0]),
        toSparkSql(inputs[1]),
        inputs.size() == 3 ? toSparkSql(inputs[2]) : toNullSql(call.type()));
  }
  if (name == "switch") {
    return toSwitchSql(call);
  }
  if (name == "in") {
    return toInSql(call);
  }
  if (name == "size") {
    return toSizeSql(call);
  }
  if (name == "array_insert") {
    return toArrayInsertSql(call);
  }
  if (name == "trim" || name == "btrim" || name == "ltrim" || name == "rtrim") {
    return toTrimSql(call);
  }
  if (name == "timestampadd" || name == "timestampdiff") {
    return toTimestampAddOrDiffSql(call);
  }
  // Spark sorts by a comparator, not by a key.
  if (name == "array_sort" && inputs.size() == 2 &&
      inputs[1]->type()->size() != 3) {
    VELOX_UNSUPPORTED("Spark does not support array_sort with a key function");
  }
  // Spark takes options, not a time zone.
  if (name == "to_json" && inputs.size() != 1) {
    VELOX_UNSUPPORTED("Spark does not support to_json with a time zone");
  }

  static const std::unordered_map<std::string, std::string> kRenamedFunctions{
      {"get_timestamp", "to_timestamp"},
      {"week_of_year", "weekofyear"},
  };
  const auto it = kRenamedFunctions.find(name);
  return fmt::format(
      "{}({})",
      it != kRenamedFunctions.end() ? it->second : name,
      folly::join(", ", toSparkSqls(inputs)));
}

// Returns the integer value of a constant frame bound.
int64_t toFrameOffset(const core::TypedExprPtr& value) {
  if (value == nullptr || !value->isConstantKind()) {
    VELOX_UNSUPPORTED("Spark only supports constant frame offsets");
  }
  const auto offset = toConstantValue(value);
  if (!offset.has_value()) {
    VELOX_UNSUPPORTED("Frame offset must not be null");
  }
  switch (value->type()->kind()) {
    case TypeKind::TINYINT:
      return offset->value<TypeKind::TINYINT>();
    case TypeKind::SMALLINT:
      return offset->value<TypeKind::SMALLINT>();
    case TypeKind::INTEGER:
      return offset->value<TypeKind::INTEGER>();
    case TypeKind::BIGINT:
      return offset->value<TypeKind::BIGINT>();
    default:
      VELOX_UNSUPPORTED(
          "Unsupported frame offset type: {}", value->type()->toString());
  }
}

std::string toFrameBoundSql(
    core::WindowNode::WindowType windowType,
    core::WindowNode::BoundType boundType,
    const core::TypedExprPtr& value) {
  using BoundType = core::WindowNode::BoundType;
  switch (boundType) {
    case BoundType::kUnboundedPreceding:
      return "UNBOUNDED PRECEDING";
    case BoundType::kUnboundedFollowing:
      return "UNBOUNDED FOLLOWING";
    case BoundType::kCurrentRow:
      return "CURRENT ROW";
    case BoundType::kPreceding:
    case BoundType::kFollowing: {
      // The bounds of Velox k RANGE frames are columns with the values of the
      // bounds, not offsets.
      if (windowType == core::WindowNode::WindowType::kRange) {
        VELOX_UNSUPPORTED(
            "Spark SQL generation does not support k RANGE frames");
      }
      const auto offset = toFrameOffset(value);
      // Spark requires integer offsets for ROWS frames. Velox fails on
      // negative offsets.
      if (offset < 0 || offset > std::numeric_limits<int32_t>::max()) {
        VELOX_UNSUPPORTED("Frame offset is out of range: {}", offset);
      }
      return fmt::format(
          "{} {}",
          offset,
          boundType == BoundType::kPreceding ? "PRECEDING" : "FOLLOWING");
    }
  }
  VELOX_UNREACHABLE();
}

// Returns the Spark name and the extra arguments of an aggregate function.
std::pair<std::string, std::vector<std::string>> toSparkAggregateName(
    const std::string& name) {
  if (name == "bloom_filter_agg" || name == "regr_replacement" ||
      name.ends_with("_partial") || name.ends_with("_merge") ||
      name.find("_merge_extract") != std::string::npos) {
    VELOX_UNSUPPORTED(
        "Aggregate function is not supported in Spark SQL: {}", name);
  }
  if (name == "first_ignore_null") {
    return {"first", {"true"}};
  }
  if (name == "last_ignore_null") {
    return {"last", {"true"}};
  }
  return {name, {}};
}

} // namespace

std::vector<std::string> toSparkFieldNames(const RowType& type) {
  std::unordered_set<std::string> lowerCaseNames;
  bool rename = false;
  for (const auto& name : type.names()) {
    if (name.empty() ||
        !lowerCaseNames.insert(boost::algorithm::to_lower_copy(name)).second) {
      rename = true;
      break;
    }
  }
  if (!rename) {
    return type.names();
  }
  std::vector<std::string> names;
  names.reserve(type.size());
  for (auto i = 0; i < type.size(); ++i) {
    names.push_back(fmt::format("col{}", i + 1));
  }
  return names;
}

std::string toSparkIdentifier(const std::string& name) {
  const bool plain = !name.empty() && !isAsciiDigit(name[0]) &&
      std::all_of(name.begin(), name.end(), [](char c) {
        return isAsciiLetter(c) || isAsciiDigit(c) || c == '_';
      });
  return plain ? name : quoteIdentifier(name);
}

std::string toSparkTypeSql(const TypePtr& type) {
  if (type->isDate()) {
    return "DATE";
  }
  if (type->isDecimal()) {
    const auto [precision, scale] = getDecimalPrecisionScale(*type);
    return fmt::format("DECIMAL({}, {})", precision, scale);
  }
  if (type->isIntervalYearMonth()) {
    return "INTERVAL YEAR TO MONTH";
  }
  switch (type->kind()) {
    case TypeKind::ARRAY:
      return fmt::format("ARRAY<{}>", toSparkTypeSql(type->childAt(0)));
    case TypeKind::MAP:
      return fmt::format(
          "MAP<{}, {}>",
          toSparkTypeSql(type->childAt(0)),
          toSparkTypeSql(type->childAt(1)));
    case TypeKind::ROW: {
      const auto names = toSparkFieldNames(type->asRow());
      if (names.empty()) {
        VELOX_UNSUPPORTED("Spark does not support structs without fields");
      }
      std::vector<std::string> fields;
      fields.reserve(names.size());
      for (auto i = 0; i < names.size(); ++i) {
        fields.push_back(
            fmt::format(
                "{}: {}",
                quoteIdentifier(names[i]),
                toSparkTypeSql(type->childAt(i))));
      }
      return fmt::format("STRUCT<{}>", folly::join(", ", fields));
    }
    default:
      break;
  }

  static const std::vector<std::pair<TypePtr, std::string>> kScalarTypes{
      {BOOLEAN(), "BOOLEAN"},
      {TINYINT(), "TINYINT"},
      {SMALLINT(), "SMALLINT"},
      {INTEGER(), "INT"},
      {BIGINT(), "BIGINT"},
      {REAL(), "FLOAT"},
      {DOUBLE(), "DOUBLE"},
      {VARCHAR(), "STRING"},
      {VARBINARY(), "BINARY"},
      {TIMESTAMP(), "TIMESTAMP"},
  };
  for (const auto& [scalarType, sql] : kScalarTypes) {
    if (type->equivalent(*scalarType)) {
      return sql;
    }
  }
  VELOX_UNSUPPORTED("Type is not supported in Spark SQL: {}", type->toString());
}

std::string toSparkValueSql(const BaseVector& vector, vector_size_t index) {
  const auto& type = vector.type();
  if (vector.isNullAt(index)) {
    return toNullSql(type);
  }
  const auto* base = vector.wrappedVector();
  const auto baseIndex = vector.wrappedIndex(index);
  switch (type->kind()) {
    case TypeKind::ARRAY: {
      const auto* array = base->asUnchecked<ArrayVector>();
      const auto offset = array->offsetAt(baseIndex);
      std::vector<std::string> elements;
      for (auto i = 0; i < array->sizeAt(baseIndex); ++i) {
        elements.push_back(toSparkValueSql(*array->elements(), offset + i));
      }
      return toArraySql(type, elements);
    }
    case TypeKind::MAP: {
      const auto* map = base->asUnchecked<MapVector>();
      const auto offset = map->offsetAt(baseIndex);
      std::vector<std::string> keys;
      std::vector<std::string> values;
      for (auto i = 0; i < map->sizeAt(baseIndex); ++i) {
        if (map->mapKeys()->isNullAt(offset + i)) {
          VELOX_UNSUPPORTED("Spark does not support null keys");
        }
        keys.push_back(toSparkValueSql(*map->mapKeys(), offset + i));
        values.push_back(toSparkValueSql(*map->mapValues(), offset + i));
      }
      return toMapSql(type, keys, values);
    }
    case TypeKind::ROW: {
      const auto* row = base->asUnchecked<RowVector>();
      std::vector<std::string> children;
      for (const auto& child : row->children()) {
        children.push_back(toSparkValueSql(*child, baseIndex));
      }
      return toRowSql(type, children);
    }
    default:
      return toValueSql(type, vector.variantAt(index));
  }
}

std::string toSparkConstantSql(const core::ConstantTypedExpr& constant) {
  if (constant.hasValueVector()) {
    return toSparkValueSql(*constant.valueVector(), 0);
  }
  return toValueSql(constant.type(), constant.value());
}

std::string toSparkSql(const core::TypedExprPtr& expr) {
  switch (expr->kind()) {
    case core::ExprKind::kFieldAccess: {
      const auto* field = expr->asUnchecked<core::FieldAccessTypedExpr>();
      if (field->isInputColumn()) {
        return toSparkIdentifier(field->name());
      }
      const auto& input = field->inputs()[0];
      return toDereferenceSql(
          input, toSparkFieldName(input->type()->asRow(), field->name()));
    }
    case core::ExprKind::kDereference: {
      const auto* dereference = expr->asUnchecked<core::DereferenceTypedExpr>();
      const auto& input = dereference->inputs()[0];
      return toDereferenceSql(
          input,
          toSparkFieldNames(input->type()->asRow()).at(dereference->index()));
    }
    case core::ExprKind::kCall:
      return toCallSql(*expr->asUnchecked<core::CallTypedExpr>());
    case core::ExprKind::kCast: {
      const auto* cast = expr->asUnchecked<core::CastTypedExpr>();
      return fmt::format(
          "{}({} AS {})",
          cast->isTryCast() ? "TRY_CAST" : "CAST",
          toSparkSql(cast->inputs()[0]),
          toSparkTypeSql(cast->type()));
    }
    case core::ExprKind::kConstant:
      return toSparkConstantSql(*expr->asUnchecked<core::ConstantTypedExpr>());
    case core::ExprKind::kConcat:
      return toRowSql(expr->type(), toSparkSqls(expr->inputs()));
    case core::ExprKind::kLambda:
      return toLambdaSql(*expr->asUnchecked<core::LambdaTypedExpr>());
    case core::ExprKind::kNullIf:
      return fmt::format(
          "nullif({})", folly::join(", ", toSparkSqls(expr->inputs())));
    default:
      VELOX_UNSUPPORTED(
          "Expression is not supported in Spark SQL: {}", expr->toString());
  }
}

std::string toSparkAggregateSql(
    const core::AggregationNode::Aggregate& aggregate) {
  if (!aggregate.sortingKeys.empty()) {
    VELOX_UNSUPPORTED(
        "Spark does not support aggregate functions with sorting keys");
  }
  auto [name, extraArguments] = toSparkAggregateName(aggregate.call->name());
  auto arguments = toSparkSqls(aggregate.call->inputs());
  arguments.insert(
      arguments.end(), extraArguments.begin(), extraArguments.end());
  if (name == "count" && arguments.empty()) {
    arguments.push_back("1");
  }
  auto sql = fmt::format(
      "{}({}{})",
      name,
      aggregate.distinct ? "DISTINCT " : "",
      folly::join(", ", arguments));
  if (aggregate.mask != nullptr) {
    sql += fmt::format(
        " FILTER (WHERE {})", toSparkIdentifier(aggregate.mask->name()));
  }
  return sql;
}

std::string toSparkWindowFunctionSql(
    const core::WindowNode::Function& function) {
  auto [name, extraArguments] =
      toSparkAggregateName(function.functionCall->name());
  auto arguments = toSparkSqls(function.functionCall->inputs());
  arguments.insert(
      arguments.end(), extraArguments.begin(), extraArguments.end());
  auto sql = fmt::format("{}({})", name, folly::join(", ", arguments));
  if (function.ignoreNulls) {
    static const std::unordered_set<std::string> kIgnoreNullsFunctions{
        "first", "last", "nth_value"};
    if (!kIgnoreNullsFunctions.contains(name)) {
      VELOX_UNSUPPORTED("Spark does not support IGNORE NULLS for {}", name);
    }
    sql += " IGNORE NULLS";
  }
  return sql;
}

std::string toSparkWindowFrameSql(const core::WindowNode::Function& function) {
  // Spark requires the default frame for ranking functions, which ignore the
  // frame in Velox.
  static const std::unordered_set<std::string> kRankingFunctions{
      "row_number", "rank", "dense_rank", "ntile", "percent_rank", "cume_dist"};
  if (kRankingFunctions.contains(function.functionCall->name())) {
    return "";
  }
  const auto& frame = function.frame;
  return fmt::format(
      "{} BETWEEN {} AND {}",
      frame.type == core::WindowNode::WindowType::kRows ? "ROWS" : "RANGE",
      toFrameBoundSql(frame.type, frame.startType, frame.startValue),
      toFrameBoundSql(frame.type, frame.endType, frame.endValue));
}

} // namespace facebook::velox::functions::sparksql::fuzzer
