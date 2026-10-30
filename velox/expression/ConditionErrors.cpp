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
#include "velox/expression/ConditionErrors.h"
#include "velox/expression/ScopedVarSetter.h"

namespace facebook::velox::exec {

void evalCollectingErrors(
    Expr& input,
    const SelectivityVector& rows,
    EvalCtx& context,
    VectorPtr& result,
    EvalErrorsPtr& errors) {
  ScopedVarSetter throwOnError(context.mutableThrowOnError(), false);
  ScopedVarSetter<EvalErrorsPtr> errorsSetter(context.errorsPtr(), nullptr);
  input.eval(rows, context, result);
  errors = std::move(*context.errorsPtr());
}

void resolveConditionErrors(
    const TypePtr& type,
    const std::vector<ConditionalOutcome>& outcomes,
    const EvalErrorsPtr& errors,
    SelectivityVector& rows,
    EvalCtx& context,
    VectorPtr& result) {
  if (errors == nullptr) {
    return;
  }
  const auto end = std::min(errors->size(), rows.end());
  if (rows.begin() >= end) {
    return;
  }
  LocalSelectivityVector errorRowsHolder(context);
  auto* errorRows = errorRowsHolder.get(rows.end(), false);
  bits::andBits(
      errorRows->asMutableRange().bits(),
      rows.asRange().bits(),
      errors->errorFlags(),
      rows.begin(),
      end);
  errorRows->updateBounds();
  if (!errorRows->hasSelections()) {
    return;
  }
  rows.deselect(*errorRows);

  // Values of FUNCTION and other non-comparable types can't be compared.
  if (type->isComparable()) {
    LocalSelectivityVector agreedRowsHolder(context, *errorRows);
    auto* agreedRows = agreedRowsHolder.get();
    VectorPtr value;
    {
      // Outcomes may fail on rows the condition would never select for them.
      // Treat such failures as disagreement, like TRY does.
      ScopedVarSetter throwOnError(context.mutableThrowOnError(), false);
      ScopedVarSetter captureErrorDetails(
          context.mutableCaptureErrorDetails(), false);
      ScopedThreadSkipErrorDetails skipErrorDetails(true);
      ScopedVarSetter<EvalErrorsPtr> errorsSetter(context.errorsPtr(), nullptr);

      outcomes[0](*agreedRows, value);
      context.deselectErrors(*agreedRows);
      if (value == nullptr) {
        agreedRows->clearAll();
      }
      for (size_t i = 1; i < outcomes.size() && agreedRows->hasSelections();
           ++i) {
        VectorPtr other;
        outcomes[i](*agreedRows, other);
        context.deselectErrors(*agreedRows);
        if (other == nullptr) {
          agreedRows->clearAll();
          break;
        }

        LocalSelectivityVector comparedRows(context, *agreedRows);
        comparedRows->applyToSelected([&](auto row) {
          if (!value->equalValueAt(other.get(), row, row)) {
            agreedRows->setValid(row, false);
          }
        });
        agreedRows->updateBounds();
      }
    }

    if (agreedRows->hasSelections()) {
      context.moveOrCopyResult(value, *agreedRows, result);
      errorRows->deselect(*agreedRows);
      if (!errorRows->hasSelections()) {
        return;
      }
    }
  }

  if (context.throwOnError()) {
    errors->throwFirstError(*errorRows);
  }
  context.addErrors(*errorRows, errors, *context.errorsPtr());
}

} // namespace facebook::velox::exec
