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

#include <gtest/gtest.h>
#include <fstream>

#include "velox/expression/tests/ExpressionVerifier.h"

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/testutil/TempDirectoryPath.h"
#include "velox/exec/fuzzer/DuckQueryRunner.h"
#include "velox/functions/Registerer.h"
#include "velox/parse/ExpressionsParser.h"
#include "velox/parse/TypeResolver.h"
#include "velox/type/Type.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox::common::testutil;

namespace facebook::velox::test {

namespace {

template <typename T>
struct AlwaysThrowsUserErrorFunction {
  template <typename TResult, typename TInput>
  FOLLY_ALWAYS_INLINE void call(TResult&, const TInput&) {
    VELOX_USER_FAIL("expected");
  }
};

template <typename T>
struct AlwaysThrowsRuntimeErrorFunction {
  template <typename TResult, typename TInput>
  FOLLY_ALWAYS_INLINE void call(TResult&, const TInput&) {
    VELOX_FAIL("expected");
  }
};

template <typename T>
struct ThrowsRuntimeErrorOnEvenInputFunction {
  template <typename TResult, typename TInput>
  FOLLY_ALWAYS_INLINE void call(TResult&, const TInput& in) {
    if (in % 2 == 0) {
      VELOX_FAIL("expected");
    }
  }
};

template <typename T>
struct IdentityFunction {
  template <typename TResult, typename TInput>
  FOLLY_ALWAYS_INLINE void call(TResult& result, const TInput& in) {
    result = in;
  }
};

template <typename T>
struct AddFunction {
  template <typename TResult, typename TInput>
  FOLLY_ALWAYS_INLINE void
  call(TResult& result, const TInput& a, const TInput& b) {
    result = a + b;
  }
};

// Reference query runner that fails queries calling 'failingFunction' and
// returns no rows for other queries. Queries that call 'nonFailingFunction' at
// the top level don't fail, like Spark functions that skip their remaining
// arguments after a null one.
class FailingFunctionQueryRunner : public exec::test::ReferenceQueryRunner {
 public:
  FailingFunctionQueryRunner(
      memory::MemoryPool* pool,
      std::string failingFunction,
      std::string nonFailingFunction = "")
      : ReferenceQueryRunner(pool),
        failingFunction_(std::move(failingFunction)),
        nonFailingFunction_(std::move(nonFailingFunction)) {}

  RunnerType runnerType() const override {
    return RunnerType::kSparkQueryRunner;
  }

  const std::unordered_map<std::string, exec::test::DataSpec>&
  aggregationFunctionDataSpecs() const override {
    static const std::unordered_map<std::string, exec::test::DataSpec>
        kDataSpecs;
    return kDataSpecs;
  }

  std::optional<std::string> toSql(const core::PlanNodePtr& plan) override {
    return projection(plan)->toString();
  }

  std::pair<
      std::optional<std::multiset<std::vector<velox::variant>>>,
      exec::test::ReferenceQueryErrorCode>
  execute(const core::PlanNodePtr& plan) override {
    const auto expr = projection(plan);
    const bool nonFailing = expr->isCallKind() &&
        expr->asUnchecked<core::CallTypedExpr>()->name() == nonFailingFunction_;
    if (!nonFailing && calls(expr)) {
      return {
          std::nullopt,
          exec::test::ReferenceQueryErrorCode::kReferenceQueryFail};
    }
    return {
        std::multiset<std::vector<velox::variant>>{},
        exec::test::ReferenceQueryErrorCode::kSuccess};
  }

 private:
  static core::TypedExprPtr projection(const core::PlanNodePtr& plan) {
    const auto project =
        std::dynamic_pointer_cast<const core::ProjectNode>(plan);
    VELOX_CHECK_NOT_NULL(project);
    VELOX_CHECK_EQ(project->projections().size(), 1);
    return project->projections()[0];
  }

  bool calls(const core::TypedExprPtr& expr) const {
    if (expr->isCallKind() &&
        expr->asUnchecked<core::CallTypedExpr>()->name() == failingFunction_) {
      return true;
    }
    for (const auto& input : expr->inputs()) {
      if (calls(input)) {
        return true;
      }
    }
    return false;
  }

  const std::string failingFunction_;
  const std::string nonFailingFunction_;
};

void removeDirecrtoryIfExist(
    std::shared_ptr<filesystems::FileSystem>& localFs,
    const std::string& folderPath) {
  if (localFs->exists(folderPath)) {
    localFs->rmdir(folderPath);
  }
  EXPECT_FALSE(localFs->exists(folderPath));
}

} // namespace

class ExpressionVerifierUnitTest : public testing::Test, public VectorTestBase {
 public:
  ExpressionVerifierUnitTest() {
    parse::registerTypeResolver();
  }

 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  core::TypedExprPtr parseExpression(
      const std::string& text,
      const RowTypePtr& rowType) {
    auto untyped = parse::DuckSqlExpressionsParser().parseExpr(text);
    return core::Expressions::inferTypes(untyped, rowType, pool_.get());
  }

  std::shared_ptr<memory::MemoryPool> pool_{
      memory::memoryManager()->addLeafPool()};
  std::shared_ptr<core::QueryCtx> queryCtx_{core::QueryCtx::create()};
  core::ExecCtx execCtx_{pool_.get(), queryCtx_.get()};
};

TEST_F(ExpressionVerifierUnitTest, persistReproInfo) {
  filesystems::registerLocalFileSystem();
  auto reproFolder = TempDirectoryPath::create();
  const auto reproPath = reproFolder->getPath();
  auto localFs = filesystems::getFileSystem(reproPath, nullptr);

  ExpressionVerifierOptions options{false, reproPath.c_str(), false};
  ExpressionVerifier verifier{&execCtx_, options, nullptr};

  auto testReproPersistency = [this](
                                  ExpressionVerifier& verifier,
                                  const std::string& reproPath,
                                  auto& localFs) {
    auto data = makeRowVector({makeFlatVector<int32_t>({1, 2, 3})});
    auto plan = parseExpression("always_throws(c0)", asRowType(data->type()));

    removeDirecrtoryIfExist(localFs, reproPath);
    VELOX_ASSERT_THROW(
        verifier.verify(
            {plan}, {{data, SelectivityVector(data->size())}}, nullptr, false),
        "");
    EXPECT_TRUE(localFs->exists(reproPath));
    EXPECT_FALSE(localFs->list(reproPath).empty());
    removeDirecrtoryIfExist(localFs, reproPath);
  };

  // User errors.
  {
    registerFunction<AlwaysThrowsUserErrorFunction, int32_t, int32_t>(
        {"always_throws"});
    testReproPersistency(verifier, reproPath, localFs);
  }

  // Runtime errors.
  {
    registerFunction<AlwaysThrowsRuntimeErrorFunction, int32_t, int32_t>(
        {"always_throws"});
    testReproPersistency(verifier, reproPath, localFs);
  }
}

TEST_F(ExpressionVerifierUnitTest, subsetOfRowsToVerify) {
  // Verify that rows passed to verify() are the only ones evaluated.
  ExpressionVerifierOptions options;
  ExpressionVerifier verifier{&execCtx_, options, nullptr};
  {
    registerFunction<ThrowsRuntimeErrorOnEvenInputFunction, int32_t, int32_t>(
        {"always_throws"});
    auto data = makeRowVector({makeFlatVector<int32_t>({1, 2, 3})});
    auto plan = parseExpression("always_throws(c0)", asRowType(data->type()));
    VELOX_ASSERT_THROW(
        verifier.verify(
            {plan}, {{data, SelectivityVector(data->size())}}, nullptr, false),
        "");

    SelectivityVector rows(data->size());
    rows.setValid(1, false);
    rows.updateBounds();
    verifier.verify({plan}, {{data, rows}}, nullptr, false);
  }
}

TEST_F(ExpressionVerifierUnitTest, referenceErrorSuppressedByVelox) {
  // Velox skips evaluating 'always_throws(c0)' because 'c1' is null in all
  // rows, while the reference DB throws. The error is accepted because Velox
  // also throws for 'always_throws(c0)' alone.
  registerFunction<AlwaysThrowsUserErrorFunction, int32_t, int32_t>(
      {"always_throws"});
  registerFunction<AddFunction, int32_t, int32_t, int32_t>({"add"});
  auto referenceRunner = std::make_shared<FailingFunctionQueryRunner>(
      pool_.get(), "always_throws");
  ExpressionVerifier verifier{&execCtx_, {}, referenceRunner};

  auto data = makeRowVector(
      {makeFlatVector<int32_t>({1, 2, 3}), makeAllNullFlatVector<int32_t>(3)});
  auto plan =
      parseExpression("add(always_throws(c0), c1)", asRowType(data->type()));
  auto states = verifier
                    .verify(
                        {plan},
                        {{data, SelectivityVector(data->size())}},
                        nullptr,
                        /*canThrow=*/true)
                    .second;
  ASSERT_EQ(states.size(), 1);
  EXPECT_EQ(
      states[0],
      ExpressionVerifier::VerificationState::kReferencePathUnsupported);
}

TEST_F(ExpressionVerifierUnitTest, referenceErrorNotThrownByVelox) {
  // The reference DB throws for 'identity(c0)' but Velox doesn't.
  registerFunction<IdentityFunction, int32_t, int32_t>({"identity"});
  registerFunction<AddFunction, int32_t, int32_t, int32_t>({"add"});
  auto referenceRunner =
      std::make_shared<FailingFunctionQueryRunner>(pool_.get(), "identity");
  ExpressionVerifier verifier{&execCtx_, {}, referenceRunner};

  auto data = makeRowVector(
      {makeFlatVector<int32_t>({1, 2, 3}), makeAllNullFlatVector<int32_t>(3)});
  auto plan = parseExpression("add(identity(c0), c1)", asRowType(data->type()));
  VELOX_ASSERT_THROW(
      verifier.verify(
          {plan},
          {{data, SelectivityVector(data->size())}},
          nullptr,
          /*canThrow=*/true),
      "Reference path throws for query: add(identity(ROW[\"c0\"]),ROW[\"c1\"])");
}

TEST_F(ExpressionVerifierUnitTest, referenceErrorOnlyInsideExpression) {
  // The reference DB throws for 'identity(add(always_throws(c0), c1))' but not
  // for 'add(always_throws(c0), c1)', so the smallest failing expression is the
  // whole expression, for which Velox doesn't throw. The error is not accepted
  // even though both throw for 'always_throws(c0)' alone, because that doesn't
  // explain why the reference DB throws for the whole expression.
  registerFunction<AlwaysThrowsUserErrorFunction, int32_t, int32_t>(
      {"always_throws"});
  registerFunction<AddFunction, int32_t, int32_t, int32_t>({"add"});
  registerFunction<IdentityFunction, int32_t, int32_t>({"identity"});
  auto referenceRunner = std::make_shared<FailingFunctionQueryRunner>(
      pool_.get(), "always_throws", "add");
  ExpressionVerifier verifier{&execCtx_, {}, referenceRunner};

  auto data = makeRowVector(
      {makeFlatVector<int32_t>({1, 2, 3}), makeAllNullFlatVector<int32_t>(3)});
  auto plan = parseExpression(
      "identity(add(always_throws(c0), c1))", asRowType(data->type()));
  VELOX_ASSERT_THROW(
      verifier.verify(
          {plan},
          {{data, SelectivityVector(data->size())}},
          nullptr,
          /*canThrow=*/true),
      "Reference path throws for query: identity(add(always_throws(ROW[\"c0\"]),ROW[\"c1\"]))");
}

TEST_F(ExpressionVerifierUnitTest, veloxErrorSkippedByReference) {
  // Velox throws for 'identity(always_throws(c0))', while the reference DB
  // doesn't evaluate the argument of 'identity', like Spark doesn't evaluate
  // the arguments of elt that it doesn't return. The error is accepted because
  // the reference DB also throws for 'always_throws(c0)' alone.
  registerFunction<AlwaysThrowsUserErrorFunction, int32_t, int32_t>(
      {"always_throws"});
  registerFunction<IdentityFunction, int32_t, int32_t>({"identity"});
  auto referenceRunner = std::make_shared<FailingFunctionQueryRunner>(
      pool_.get(), "always_throws", "identity");
  ExpressionVerifier verifier{&execCtx_, {}, referenceRunner};

  auto data = makeRowVector({makeFlatVector<int32_t>({1, 2, 3})});
  auto plan =
      parseExpression("identity(always_throws(c0))", asRowType(data->type()));
  auto states = verifier
                    .verify(
                        {plan},
                        {{data, SelectivityVector(data->size())}},
                        nullptr,
                        /*canThrow=*/true)
                    .second;
  ASSERT_EQ(states.size(), 1);
  EXPECT_EQ(
      states[0],
      ExpressionVerifier::VerificationState::kReferencePathUnsupported);
}

TEST_F(ExpressionVerifierUnitTest, veloxErrorNotThrownByReference) {
  // Velox throws for 'always_throws(c0)' but the reference DB doesn't.
  registerFunction<AlwaysThrowsUserErrorFunction, int32_t, int32_t>(
      {"always_throws"});
  auto referenceRunner =
      std::make_shared<FailingFunctionQueryRunner>(pool_.get(), "identity");
  ExpressionVerifier verifier{&execCtx_, {}, referenceRunner};

  auto data = makeRowVector({makeFlatVector<int32_t>({1, 2, 3})});
  auto plan = parseExpression("always_throws(c0)", asRowType(data->type()));
  VELOX_ASSERT_THROW(
      verifier.verify(
          {plan},
          {{data, SelectivityVector(data->size())}},
          nullptr,
          /*canThrow=*/true),
      "expected");
}

TEST_F(ExpressionVerifierUnitTest, emptyActiveRowsWithReferenceRunner) {
  // When no rows are selected, the reference-runner path reduces both the input
  // and the result to zero rows and verifies the empty result against the
  // reference DB.
  auto referenceRunner =
      std::make_shared<exec::test::DuckQueryRunner>(pool_.get());
  ExpressionVerifierOptions options;
  ExpressionVerifier verifier{&execCtx_, options, referenceRunner};

  auto data = makeRowVector({makeFlatVector<int32_t>({1, 2, 3})});
  auto plan = parseExpression("c0", asRowType(data->type()));

  SelectivityVector emptyRows(data->size());
  emptyRows.clearAll();
  ASSERT_FALSE(emptyRows.hasSelections());

  auto states =
      verifier.verify({plan}, {{data, emptyRows}}, nullptr, /*canThrow=*/false)
          .second;
  ASSERT_EQ(states.size(), 1);
  EXPECT_EQ(
      states[0],
      ExpressionVerifier::VerificationState::kVerifiedAgainstReference);
}

} // namespace facebook::velox::test
