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

#include "velox/functions/sparksql/fuzzer/SparkQueryRunnerToSqlPlanNodeVisitor.h"
#include "velox/exec/fuzzer/ReferenceQueryRunner.h"
#include "velox/functions/sparksql/fuzzer/SparkSql.h"

namespace facebook::velox::functions::sparksql::fuzzer {
namespace {

bool containsIntervalYearMonth(const TypePtr& type) {
  if (type->isIntervalYearMonth()) {
    return true;
  }
  for (auto i = 0; i < type->size(); ++i) {
    if (containsIntervalYearMonth(type->childAt(i))) {
      return true;
    }
  }
  return false;
}

} // namespace

void SparkQueryRunnerToSqlPlanNodeVisitor::visit(
    const core::AggregationNode& node,
    core::PlanNodeVisitorContext& ctx) const {
  auto& visitorContext =
      static_cast<exec::test::PrestoSqlPlanNodeVisitorContext&>(ctx);
  if (node.step() != core::AggregationNode::Step::kSingle) {
    VELOX_UNSUPPORTED("Spark SQL only supports single aggregations");
  }

  std::vector<std::string> groupingKeys;
  for (const auto& key : node.groupingKeys()) {
    groupingKeys.push_back(toSparkIdentifier(key->name()));
  }

  auto selectItems = groupingKeys;
  const auto& aggregates = node.aggregates();
  for (auto i = 0; i < aggregates.size(); ++i) {
    selectItems.push_back(
        fmt::format(
            "{} AS {}",
            toSparkAggregateSql(aggregates[i]),
            toSparkIdentifier(node.aggregateNames()[i])));
  }
  if (selectItems.empty()) {
    VELOX_UNSUPPORTED("Aggregation without keys and aggregates");
  }

  const auto source = toSql(node.sources()[0]);
  if (!source) {
    visitorContext.sql = std::nullopt;
    return;
  }

  auto sql = fmt::format(
      "SELECT {} FROM ({})", folly::join(", ", selectItems), *source);
  if (!groupingKeys.empty()) {
    sql += fmt::format(" GROUP BY {}", folly::join(", ", groupingKeys));
  }
  visitorContext.sql = std::move(sql);
}

void SparkQueryRunnerToSqlPlanNodeVisitor::visit(
    const core::ProjectNode& node,
    core::PlanNodeVisitorContext& ctx) const {
  auto& visitorContext =
      static_cast<exec::test::PrestoSqlPlanNodeVisitorContext&>(ctx);

  std::vector<std::string> selectItems;
  for (auto i = 0; i < node.names().size(); ++i) {
    selectItems.push_back(
        fmt::format(
            "{} AS {}",
            toSparkSql(node.projections()[i]),
            toSparkIdentifier(node.names()[i])));
  }

  const auto source = toSql(node.sources()[0]);
  if (!source) {
    visitorContext.sql = std::nullopt;
    return;
  }
  visitorContext.sql = fmt::format(
      "SELECT {} FROM ({})", folly::join(", ", selectItems), *source);
}

void SparkQueryRunnerToSqlPlanNodeVisitor::visit(
    const core::ValuesNode& node,
    core::PlanNodeVisitorContext& ctx) const {
  auto& visitorContext =
      static_cast<exec::test::PrestoSqlPlanNodeVisitorContext&>(ctx);
  const auto& type = node.outputType();
  std::vector<std::string> columns;
  bool hasInterval = false;
  for (auto i = 0; i < type->size(); ++i) {
    const auto& columnType = type->childAt(i);
    // Throws for types without a Spark equivalent.
    toSparkTypeSql(columnType);
    const auto name = toSparkIdentifier(type->nameOf(i));
    // Intervals are written to Parquet as integers.
    if (columnType->isIntervalYearMonth()) {
      columns.push_back(fmt::format("make_ym_interval(0, {0}) AS {0}", name));
      hasInterval = true;
    } else {
      if (containsIntervalYearMonth(columnType)) {
        VELOX_UNSUPPORTED(
            "Spark SQL generation does not support nested intervals: {}",
            columnType->toString());
      }
      columns.push_back(name);
    }
  }
  const auto tableName = exec::test::ReferenceQueryRunner::getTableName(node);
  visitorContext.sql = hasInterval
      ? fmt::format("SELECT {} FROM {}", folly::join(", ", columns), tableName)
      : tableName;
}

void SparkQueryRunnerToSqlPlanNodeVisitor::visit(
    const core::WindowNode& node,
    core::PlanNodeVisitorContext& ctx) const {
  auto& visitorContext =
      static_cast<exec::test::PrestoSqlPlanNodeVisitorContext&>(ctx);

  std::vector<std::string> windowSpec;
  if (!node.partitionKeys().empty()) {
    std::vector<std::string> keys;
    for (const auto& key : node.partitionKeys()) {
      keys.push_back(toSparkIdentifier(key->name()));
    }
    windowSpec.push_back(
        fmt::format("PARTITION BY {}", folly::join(", ", keys)));
  }
  if (!node.sortingKeys().empty()) {
    std::vector<std::string> keys;
    for (auto i = 0; i < node.sortingKeys().size(); ++i) {
      keys.push_back(
          fmt::format(
              "{} {}",
              toSparkIdentifier(node.sortingKeys()[i]->name()),
              node.sortingOrders()[i].toString()));
    }
    windowSpec.push_back(fmt::format("ORDER BY {}", folly::join(", ", keys)));
  }

  std::vector<std::string> selectItems;
  for (const auto& name : node.inputType()->names()) {
    selectItems.push_back(toSparkIdentifier(name));
  }
  const auto& functions = node.windowFunctions();
  for (auto i = 0; i < functions.size(); ++i) {
    auto functionSpec = windowSpec;
    auto frame = toSparkWindowFrameSql(functions[i]);
    if (!frame.empty()) {
      functionSpec.push_back(std::move(frame));
    }
    selectItems.push_back(
        fmt::format(
            "{} OVER ({}) AS {}",
            toSparkWindowFunctionSql(functions[i]),
            folly::join(" ", functionSpec),
            toSparkIdentifier(node.windowColumnNames()[i])));
  }

  const auto source = toSql(node.sources()[0]);
  if (!source) {
    visitorContext.sql = std::nullopt;
    return;
  }
  visitorContext.sql = fmt::format(
      "SELECT {} FROM ({})", folly::join(", ", selectItems), *source);
}

} // namespace facebook::velox::functions::sparksql::fuzzer
