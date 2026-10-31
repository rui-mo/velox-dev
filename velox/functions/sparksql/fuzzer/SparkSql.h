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

#include "velox/core/PlanNode.h"

/// Converts Velox expressions into Spark SQL. The functions throw
/// VELOX_UNSUPPORTED for expressions, types and values that cannot be
/// expressed in Spark SQL with the same semantics.
namespace facebook::velox::functions::sparksql::fuzzer {

/// Returns the names of the fields of a struct of 'type' in Spark. Spark cannot
/// read struct fields with empty or case-insensitively duplicate names from
/// Parquet, so the fields are named col1, col2, ... if any name is empty or a
/// duplicate. Otherwise, the names are kept.
std::vector<std::string> toSparkFieldNames(const RowType& type);

/// Returns 'name' as a Spark SQL identifier, quoted with backticks unless it
/// consists of letters, digits and underscores and does not start with a digit.
std::string toSparkIdentifier(const std::string& name);

/// Returns the Spark SQL type of 'type'. Struct fields are named by
/// toSparkFieldNames.
std::string toSparkTypeSql(const TypePtr& type);

/// Returns a Spark SQL expression of type 'type' that evaluates to the value
/// at 'index' in 'vector'.
std::string toSparkValueSql(const BaseVector& vector, vector_size_t index);

/// Returns a Spark SQL expression that evaluates to 'constant'.
std::string toSparkConstantSql(const core::ConstantTypedExpr& constant);

/// Returns a Spark SQL expression that is equivalent to 'expr'. Velox Spark
/// functions are mapped to the Spark functions or operators they implement.
std::string toSparkSql(const core::TypedExprPtr& expr);

/// Returns a Spark SQL aggregate function call that is equivalent to
/// 'aggregate', including DISTINCT and the FILTER clause for the mask.
std::string toSparkAggregateSql(
    const core::AggregationNode::Aggregate& aggregate);

/// Returns a Spark SQL window function call that is equivalent to 'function',
/// including the IGNORE NULLS clause but not the OVER clause.
std::string toSparkWindowFunctionSql(
    const core::WindowNode::Function& function);

/// Returns the Spark SQL window frame clause of 'function', or an empty string
/// if Spark does not accept a frame for the function, e.g. for rank().
std::string toSparkWindowFrameSql(const core::WindowNode::Function& function);

} // namespace facebook::velox::functions::sparksql::fuzzer
