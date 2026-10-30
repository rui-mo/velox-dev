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

#include <functional>

#include "velox/expression/EvalCtx.h"
#include "velox/expression/Expr.h"

namespace facebook::velox::exec {

/// Evaluates 'input' on 'rows' and records per-row errors instead of throwing,
/// like AND and OR do for their inputs. Sets 'errors' to the new errors, or
/// nullptr if there are none. Errors already in 'context' stay unchanged.
void evalCollectingErrors(
    Expr& input,
    const SelectivityVector& rows,
    EvalCtx& context,
    VectorPtr& result,
    EvalErrorsPtr& errors);

/// Evaluates one value that a conditional expression may return on 'rows'.
using ConditionalOutcome =
    std::function<void(const SelectivityVector& rows, VectorPtr& result)>;

/// Handles rows of 'rows' where a condition of IF, SWITCH or CASE failed.
/// 'errors' holds the condition errors. 'outcomes' lists all values the
/// condition can select between. If all outcomes return the same value for a
/// failed row, that value goes to 'result' and the error is dropped. NULL
/// equals NULL. Otherwise, the error is thrown or, when 'context' doesn't throw
/// on errors, added to 'context'. Removes all failed rows from 'rows'.
void resolveConditionErrors(
    const TypePtr& type,
    const std::vector<ConditionalOutcome>& outcomes,
    const EvalErrorsPtr& errors,
    SelectivityVector& rows,
    EvalCtx& context,
    VectorPtr& result);

} // namespace facebook::velox::exec
