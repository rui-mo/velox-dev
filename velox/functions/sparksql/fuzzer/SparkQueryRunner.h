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

#include <chrono>
#include <unordered_map>

#include "grpc++/channel.h" // @manual
#include "grpc++/client_context.h" // @manual
#include "grpc++/create_channel.h" // @manual
#include "grpc++/security/credentials.h" // @manual
#include "velox/common/fuzzer/Utils.h"
#include "velox/common/memory/Memory.h"
#include "velox/exec/fuzzer/ReferenceQueryRunner.h"
#include "velox/functions/sparksql/fuzzer/spark/connect/base.grpc.pb.h" // @manual
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::functions::sparksql::fuzzer {

using facebook::velox::fuzzer::DataSpec;

/// Query runner that uses Spark as a reference database. It converts Velox
/// query plan to Spark SQL and executes it in Spark through Spark Connect. The
/// results are returned as Velox compatible format.
///
/// Spark errors are classified by Java exception and SQLSTATE. Analysis errors
/// (SQLSTATE class 42), feature not supported errors (class 0A), resource
/// limit errors (classes 53 and 54), Spark internal errors (class XX) and fatal
/// JVM errors such as OutOfMemoryError are reported as
/// kReferenceQueryUnsupported because they mean that Spark cannot run the
/// query, not that the query fails on the data. Other errors are runtime
/// errors and are reported as kReferenceQueryFail. A Spark parse error means
/// the generated SQL is invalid and throws, as does a stopped SparkContext.
/// Each error is logged with the 'Spark query' prefix to help with triaging
/// fuzzer runs.
class SparkQueryRunner : public velox::exec::test::ReferenceQueryRunner {
 public:
  static constexpr std::chrono::seconds kDefaultQueryTimeout{300};

  /// @param coordinatorUri Spark connect server endpoint, e.g. localhost:15002.
  /// @param queryTimeout Maximum time a query is allowed to run. A query that
  /// exceeds it is interrupted and reported as kReferenceQueryUnsupported.
  /// @param sessionConfigs Spark SQL configs to set for the session before
  /// running the first query. These override 'defaultSessionConfigs()'.
  SparkQueryRunner(
      memory::MemoryPool* pool,
      const std::string& coordinatorUri,
      const std::string& userId,
      const std::string& userName,
      std::chrono::seconds queryTimeout = kDefaultQueryTimeout,
      const std::unordered_map<std::string, std::string>& sessionConfigs = {});

  ~SparkQueryRunner() override;

  /// Spark SQL configs that make Spark match the default behavior of Velox
  /// Spark functions: ANSI mode off and America/Los_Angeles session time zone.
  static const std::unordered_map<std::string, std::string>&
  defaultSessionConfigs();

  /// Returns how a Spark query that failed with 'status' should be reported.
  /// Throws if it is a parse error or the Spark Connect server is not usable.
  static exec::test::ReferenceQueryErrorCode classifyError(
      const grpc::Status& status);

  RunnerType runnerType() const override {
    return RunnerType::kSparkQueryRunner;
  }

  const std::vector<TypePtr>& supportedScalarTypes() const override;

  const std::unordered_map<std::string, DataSpec>&
  aggregationFunctionDataSpecs() const override;

  /// Returns false if the signature uses types without a Spark equivalent,
  /// e.g. UNKNOWN, TIME, HUGEINT or INTERVAL DAY TO SECOND.
  bool isSupported(const exec::FunctionSignature& signature) override;

  /// Returns true if 'expr' can be converted to Spark SQL. The fuzzers call it
  /// for constant expressions.
  bool isConstantExprSupported(const core::TypedExprPtr& expr) override;

  /// Converts Velox query plan to Spark SQL. Supports Project, Aggregation and
  /// Window nodes over a Values node, which is converted into reading from a
  /// temporary view of the input data. Velox Spark functions are mapped to the
  /// Spark functions or operators they implement.
  /// @return std::nullopt for unsupported cases.
  std::optional<std::string> toSql(
      const velox::core::PlanNodePtr& plan) override;

  // Converts 'plan' into an SQL query and executes it. Result is returned as a
  // MaterializedRowMultiset with the ReferenceQueryErrorCode::kSuccess if
  // successful, or an std::nullopt with a ReferenceQueryErrorCode if the query
  // fails.
  std::pair<
      std::optional<std::multiset<std::vector<variant>>>,
      exec::test::ReferenceQueryErrorCode>
  execute(const core::PlanNodePtr& plan) override;

  /// Similar to 'execute' but returns results in RowVector format.
  /// Caller should ensure 'supportsVeloxVectorResults' returns true.
  std::pair<
      std::optional<std::vector<RowVectorPtr>>,
      exec::test::ReferenceQueryErrorCode>
  executeAndReturnVector(const core::PlanNodePtr& plan) override;

  /// Executes Spark SQL query and returns the results. Tables referenced by
  /// the query must already exist. Throws if the query fails.
  std::vector<velox::RowVectorPtr> execute(const std::string& sql) override;

  bool supportsVeloxVectorResults() const override {
    return true;
  }

 private:
  using ReferenceQueryRunner::toSql;

  struct QueryResult {
    std::vector<velox::RowVectorPtr> data;
    // Empty if the query succeeded.
    std::optional<exec::test::ReferenceQueryErrorCode> errorCode;
    std::string errorMessage;
  };

  // Executes 'sql' and returns the results or the classified error. Throws if
  // the Spark Connect server cannot be reached.
  QueryResult executeQuery(const std::string& sql);

  // Sets the session configs on the first call.
  void ensureSessionConfigured();

  // Interrupts all running operations of the session.
  void interruptSession();

  spark::connect::UserContext userContext() const;

  // Generates a random UUID string for Spark. It must be of the format
  // '00112233-4455-6677-8899-aabbccddeeff'.
  static std::string generateUUID();

  velox::memory::MemoryPool* pool() {
    return pool_.get();
  }

  // Reads the arrow IPC-format string data with arrow IPC reader and convert
  // them into Velox RowVectors.
  std::vector<velox::RowVectorPtr> readArrowData(const std::string& data);

  const std::string userId_;
  const std::string userName_;
  const std::string sessionId_;
  const std::chrono::seconds queryTimeout_;
  std::unordered_map<std::string, std::string> sessionConfigs_;
  bool sessionConfigured_{false};
  // Used to make gRPC calls to the SparkConnectService.
  std::unique_ptr<spark::connect::SparkConnectService::Stub> stub_;
  std::shared_ptr<velox::memory::MemoryPool> pool_;
  std::shared_ptr<velox::memory::MemoryPool> copyPool_;
};
} // namespace facebook::velox::functions::sparksql::fuzzer
