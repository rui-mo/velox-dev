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

#include "velox/functions/sparksql/aggregates/CovarianceAggregate.h"

#include <limits>
#include "velox/functions/lib/aggregates/CovarianceAggregatesBase.h"
#include "velox/functions/sparksql/SparkQueryConfig.h"

namespace facebook::velox::functions::aggregate::sparksql {

using functions::sparksql::SparkQueryConfig;

namespace {

template <bool nullOnDivideByZero>
struct CovarSampResultAccessor {
  static bool hasResult(const CovarAccumulator& accumulator) {
    if constexpr (nullOnDivideByZero) {
      return accumulator.count() >= 2;
    } else {
      return accumulator.count() >= 1;
    }
  }

  static double result(const CovarAccumulator& accumulator) {
    if (accumulator.count() == 1) {
      VELOX_CHECK(
          !nullOnDivideByZero,
          "NaN is returned only when m2 is 0 and nullOnDivideByZero is false.");
      return std::numeric_limits<double>::quiet_NaN();
    }
    return accumulator.c2() / (accumulator.count() - 1);
  }
};

template <bool nullOnDivideByZero>
struct CorrResultAccessor {
  static bool hasResult(const CorrAccumulator& accumulator) {
    if (accumulator.count() == 1) {
      if constexpr (nullOnDivideByZero) {
        return false;
      } else {
        return true;
      }
    }

    // Returns true if any of the following conditions are met:
    // 1. m2Y or m2X or c2 is NaN.
    // 2. m2Y and m2X are not zero.
    return std::isnan(accumulator.m2Y()) || std::isnan(accumulator.m2X()) ||
        std::isnan(accumulator.c2()) ||
        (accumulator.m2Y() != 0 && accumulator.m2X() != 0);
  }

  static double result(const CorrAccumulator& accumulator) {
    if (accumulator.count() == 1) {
      if constexpr (!nullOnDivideByZero) {
        return std::numeric_limits<double>::quiet_NaN();
      } else {
        VELOX_UNREACHABLE();
      }
    }

    if (FOLLY_UNLIKELY(
            std::isnan(accumulator.m2Y()) || std::isnan(accumulator.m2X()) ||
            std::isnan(accumulator.c2()))) {
      return std::numeric_limits<double>::quiet_NaN();
    }
    double stddevX = std::sqrt(accumulator.m2X());
    double stddevY = std::sqrt(accumulator.m2Y());
    return accumulator.c2() / stddevX / stddevY;
  }
};

// Spark 4.2.1 and earlier merge a partial result with n pairs into the empty
// buffer of the final aggregation with the formulas for two non-empty buffers.
// A mean becomes 0 + mean / n * n, which may differ from the mean in the last
// bit. A co-moment of a and b, i.e. c2 of x and y, m2X of x and x or m2Y of y
// and y, becomes moment + meanA * (meanB / n) * 0 * n, which is NaN if
// meanA * (meanB / n) is infinite.
double legacyMergeMean(double mean, double n) {
  return 0.0 + mean / n * n;
}

double legacyMergeMoment(double moment, double meanA, double meanB, double n) {
  return (0.0 + moment) + meanA * (meanB / n) * 0.0 * n;
}

class LegacyCovarIntermediateInput : public CovarIntermediateInput {
 public:
  explicit LegacyCovarIntermediateInput(const RowVector* rowVector)
      : CovarIntermediateInput(rowVector) {}

  void mergeInto(CovarAccumulator& accumulator, vector_size_t row) {
    const auto count = count_->valueAt(row);
    if (accumulator.count() != 0 || count == 0) {
      CovarIntermediateInput::mergeInto(accumulator, row);
      return;
    }
    const double n = count;
    const auto meanX = meanX_->valueAt(row);
    const auto meanY = meanY_->valueAt(row);
    accumulator.merge(
        count,
        legacyMergeMean(meanX, n),
        legacyMergeMean(meanY, n),
        legacyMergeMoment(c2_->valueAt(row), meanX, meanY, n));
  }
};

class LegacyCorrIntermediateInput : public CovarIntermediateInput {
 public:
  explicit LegacyCorrIntermediateInput(const RowVector* rowVector)
      : CovarIntermediateInput(rowVector, kCorrIndices),
        m2X_{asSimpleVector<double>(rowVector, kCorrIndices.m2X)},
        m2Y_{asSimpleVector<double>(rowVector, kCorrIndices.m2Y)} {}

  void mergeInto(CorrAccumulator& accumulator, vector_size_t row) {
    const auto count = count_->valueAt(row);
    const auto meanX = meanX_->valueAt(row);
    const auto meanY = meanY_->valueAt(row);
    const auto c2 = c2_->valueAt(row);
    const auto m2X = m2X_->valueAt(row);
    const auto m2Y = m2Y_->valueAt(row);
    if (accumulator.count() != 0 || count == 0) {
      accumulator.merge(count, meanX, meanY, c2, m2X, m2Y);
      return;
    }
    const double n = count;
    accumulator.merge(
        count,
        legacyMergeMean(meanX, n),
        legacyMergeMean(meanY, n),
        legacyMergeMoment(c2, meanX, meanY, n),
        legacyMergeMoment(m2X, meanX, meanX, n),
        legacyMergeMoment(m2Y, meanY, meanY, n));
  }

 private:
  SimpleVector<double>* m2X_;
  SimpleVector<double>* m2Y_;
};

template <
    typename TAccumulator,
    typename TIntermediateInput,
    typename TIntermediateResult,
    typename TResultAccessor>
std::unique_ptr<exec::Aggregate> makeCovariance(const TypePtr& resultType) {
  return std::make_unique<CovarianceAggregate<
      double,
      TAccumulator,
      TIntermediateInput,
      TIntermediateResult,
      TResultAccessor>>(resultType);
}

template <typename TIntermediateInput>
std::unique_ptr<exec::Aggregate> createCovarSamp(
    const TypePtr& resultType,
    const SparkQueryConfig& config) {
  if (config.legacyStatisticalAggregate()) {
    return makeCovariance<
        CovarAccumulator,
        TIntermediateInput,
        CovarIntermediateResult,
        CovarSampResultAccessor<false>>(resultType);
  }
  return makeCovariance<
      CovarAccumulator,
      TIntermediateInput,
      CovarIntermediateResult,
      CovarSampResultAccessor<true>>(resultType);
}

template <typename TIntermediateInput>
std::unique_ptr<exec::Aggregate> createCorr(
    const TypePtr& resultType,
    const SparkQueryConfig& config) {
  if (config.legacyStatisticalAggregate()) {
    return makeCovariance<
        CorrAccumulator,
        TIntermediateInput,
        CorrIntermediateResult,
        CorrResultAccessor<false>>(resultType);
  }
  return makeCovariance<
      CorrAccumulator,
      TIntermediateInput,
      CorrIntermediateResult,
      CorrResultAccessor<true>>(resultType);
}

using CovarianceFactory = std::unique_ptr<exec::Aggregate> (*)(
    const TypePtr& resultType,
    const SparkQueryConfig& config);

// 'create' creates the aggregate, and 'createLegacy' creates it when
// spark.legacy_covariance_merge_empty_buffer is true.
exec::AggregateRegistrationResult registerCovariance(
    const std::string& name,
    const std::string& intermediateType,
    CovarianceFactory create,
    CovarianceFactory createLegacy,
    bool withCompanionFunctions,
    bool overwrite) {
  std::vector<std::shared_ptr<exec::AggregateFunctionSignature>> signatures = {
      exec::AggregateFunctionSignatureBuilder()
          .returnType("double")
          .intermediateType(intermediateType)
          .argumentType("double")
          .argumentType("double")
          .build(),
  };

  return exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [create, createLegacy](
          core::AggregationNode::Step step,
          const std::vector<TypePtr>& argTypes,
          const TypePtr& resultType,
          const core::QueryConfig& config) -> std::unique_ptr<exec::Aggregate> {
        auto inputType = exec::isRawInput(step)
            ? argTypes[0]
            : (exec::isPartialOutput(step) ? DOUBLE() : resultType);
        VELOX_USER_CHECK_EQ(
            inputType->kind(),
            TypeKind::DOUBLE,
            "Unsupported input type: {}. "
            "Expected DOUBLE.",
            inputType->toString());

        const SparkQueryConfig sparkConfig{config};
        if (sparkConfig.legacyCovarianceMergeEmptyBuffer()) {
          return createLegacy(resultType, sparkConfig);
        }
        return create(resultType, sparkConfig);
      },
      withCompanionFunctions,
      overwrite);
}
} // namespace

void registerCovarianceAggregates(
    const std::string& prefix,
    bool withCompanionFunctions,
    bool overwrite) {
  registerCovariance(
      prefix + "covar_samp",
      CovarIntermediateResult::type(),
      createCovarSamp<CovarIntermediateInput>,
      createCovarSamp<LegacyCovarIntermediateInput>,
      withCompanionFunctions,
      overwrite);
  registerCovariance(
      prefix + "corr",
      CorrIntermediateResult::type(),
      createCorr<CorrIntermediateInput>,
      createCorr<LegacyCorrIntermediateInput>,
      withCompanionFunctions,
      overwrite);
}

} // namespace facebook::velox::functions::aggregate::sparksql
