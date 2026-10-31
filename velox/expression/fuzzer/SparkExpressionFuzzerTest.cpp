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

#include <folly/String.h>
#include <folly/init/Init.h>
#include <gtest/gtest.h>
#include <chrono>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "velox/common/memory/Memory.h"
#include "velox/exec/fuzzer/ReferenceQueryRunner.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/expression/fuzzer/ArgValuesGenerators.h"
#include "velox/expression/fuzzer/FuzzerRunner.h"
#include "velox/expression/fuzzer/SparkSpecialFormSignatureGenerator.h"
#include "velox/expression/signature_parser/ParseUtil.h"
#include "velox/functions/prestosql/fuzzer/FloorCeilRoundArgTypesGenerator.h"
#include "velox/functions/sparksql/SparkQueryConfig.h"
#include "velox/functions/sparksql/fuzzer/AbsUnaryMinusArgTypesGenerator.h"
#include "velox/functions/sparksql/fuzzer/AddSubtractArgTypesGenerator.h"
#include "velox/functions/sparksql/fuzzer/DivideArgTypesGenerator.h"
#include "velox/functions/sparksql/fuzzer/MakeTimestampArgTypesGenerator.h"
#include "velox/functions/sparksql/fuzzer/MultiplyArgTypesGenerator.h"
#include "velox/functions/sparksql/fuzzer/UnscaledValueArgTypesGenerator.h"
#include "velox/functions/sparksql/registration/Register.h"

#ifdef VELOX_ENABLE_PARQUET
#include "velox/dwio/parquet/RegisterParquetWriter.h"
#include "velox/functions/sparksql/fuzzer/SparkQueryRunner.h"
#include "velox/functions/sparksql/fuzzer/SparkSql.h"
#endif

using namespace facebook::velox::functions::sparksql::fuzzer;
using facebook::velox::functions::sparksql::SparkQueryConfig;
using facebook::velox::fuzzer::ArgTypesGenerator;
using facebook::velox::test::ReferenceQueryRunner;

DEFINE_int64(
    seed,
    123456,
    "Initial seed for random number generator "
    "(use it to reproduce previous results).");

DEFINE_string(
    spark_url,
    "",
    "Spark Connect server address along with port. If set, Spark is used as "
    "the source of truth. Otherwise, the Velox simplified expression "
    "evaluation is used. Example: --spark_url=localhost:15002");

DEFINE_uint32(
    spark_query_timeout_sec,
    60,
    "Timeout in seconds for queries run in Spark. Queries that time out are "
    "treated as not supported by Spark. Example: "
    "--spark_query_timeout_sec=120");

DEFINE_string(
    skip_functions,
    "",
    "Comma separated list of function names to skip in addition to the "
    "built-in skip list, e.g. to exclude functions with known issues. "
    "Example: --skip_functions=\"array_sort,sequence\"");

using facebook::velox::fuzzer::ArgValuesGenerator;
using facebook::velox::fuzzer::FuzzerRunner;
using facebook::velox::fuzzer::LevenshteinArgValuesGenerator;
using facebook::velox::fuzzer::MakeTimestampArgValuesGenerator;
using facebook::velox::fuzzer::PadArgValuesGenerator;
using facebook::velox::fuzzer::RepeatArgValuesGenerator;
using facebook::velox::fuzzer::RoundArgValuesGenerator;

namespace {

#ifdef VELOX_ENABLE_PARQUET
// Returns the functions that cannot be verified against Spark.
std::unordered_set<std::string> sparkSkippedFunctions() {
  std::unordered_set<std::string> skipFunctions = {
      // Velox limits the number of elements by
      // 'max_elements_size_in_repeat_and_sequence', while Spark doesn't.
      "sequence",
      // Spark's array_sort only accepts comparator lambdas.
      "array_sort(array(T),constant function(T,U)) -> array(T)",
      // Spark's interpreted levenshtein treats a null threshold as 0, while
      // its generated code returns null, as Velox does. SparkQueryRunner
      // evaluates expressions without generated code.
      "levenshtein(varchar,varchar,integer) -> integer",
  };
  for (const auto& [name, _] : facebook::velox::getFunctionSignatures()) {
    if (!isSupportedFunction(name)) {
      skipFunctions.insert(name);
    }
  }
  return skipFunctions;
}
#endif

} // namespace

int main(int argc, char** argv) {
  facebook::velox::functions::sparksql::registerFunctions("");

  ::testing::InitGoogleTest(&argc, argv);

  // Calls common init functions in the necessary order, initializing
  // singletons, installing proper signal handlers for better debugging
  // experience, and initialize glog and gflags.
  folly::Init init(&argc, &argv);

  facebook::velox::memory::MemoryManager::initialize(
      facebook::velox::memory::MemoryManager::Options{});

  // The following list are the Spark UDFs that hit issues
  // For rlike you need the following combo in the only list:
  // rlike, md5 and upper
  std::unordered_set<std::string> skipFunctions = {
      "regexp_extract",
      // https://github.com/facebookincubator/velox/issues/8438
      "regexp_replace",
      // https://github.com/facebookincubator/velox/issues/17697
      "regexp_instr",
      "rlike",
      "chr",
      "replace",
      "might_contain",
      // unix_timestamp with empty parameter returns current unix timestamp so
      // the results are different for each evaluation.
      "unix_timestamp",
      // from_unixtime throws VeloxRuntimeError when the timestamp is out of the
      // supported range.
      "from_unixtime",
      // timestamp_millis(bigint) can generate timestamps out of the supported
      // range that make other functions throw VeloxRuntimeErrors.
      "timestamp_millis(bigint) -> timestamp",
      // Fuzzer cannot generate valid 'comparator' lambda.
      "array_sort(array(T),constant function(T,T,integer)) -> array(T)",
  };

  // Required by spark_partition_id function.
  std::unordered_map<std::string, std::string> queryConfigs = {
      {SparkQueryConfig::qualify(SparkQueryConfig::kPartitionId), "123"},
      {facebook::velox::core::QueryConfig::kSessionTimezone,
       "America/Los_Angeles"}};

  std::unordered_map<std::string, std::shared_ptr<ArgTypesGenerator>>
      argTypesGenerators = {
          {"add", std::make_shared<AddSubtractArgTypesGenerator>(true)},
          {"add_deny_precision_loss",
           std::make_shared<AddSubtractArgTypesGenerator>(false)},
          {"checked_add", std::make_shared<AddSubtractArgTypesGenerator>(true)},
          {"checked_add_deny_precision_loss",
           std::make_shared<AddSubtractArgTypesGenerator>(false)},
          {"subtract", std::make_shared<AddSubtractArgTypesGenerator>(true)},
          {"subtract_deny_precision_loss",
           std::make_shared<AddSubtractArgTypesGenerator>(false)},
          {"checked_subtract",
           std::make_shared<AddSubtractArgTypesGenerator>(true)},
          {"checked_subtract_deny_precision_loss",
           std::make_shared<AddSubtractArgTypesGenerator>(false)},
          {"multiply", std::make_shared<MultiplyArgTypesGenerator>(true)},
          {"multiply_deny_precision_loss",
           std::make_shared<MultiplyArgTypesGenerator>(false)},
          {"checked_multiply",
           std::make_shared<MultiplyArgTypesGenerator>(true)},
          {"checked_multiply_deny_precision_loss",
           std::make_shared<MultiplyArgTypesGenerator>(false)},
          {"divide", std::make_shared<DivideArgTypesGenerator>(true)},
          {"divide_deny_precision_loss",
           std::make_shared<DivideArgTypesGenerator>(false)},
          {"ceil",
           std::make_shared<
               facebook::velox::exec::test::FloorCeilRoundArgTypesGenerator>()},
          {"floor",
           std::make_shared<
               facebook::velox::exec::test::FloorCeilRoundArgTypesGenerator>()},
          {"unscaled_value",
           std::make_shared<UnscaledValueArgTypesGenerator>()},
          {"make_timestamp",
           std::make_shared<MakeTimestampArgTypesGenerator>()},
          {"try_make_timestamp",
           std::make_shared<MakeTimestampArgTypesGenerator>()},
          {"make_timestamp_ntz",
           std::make_shared<MakeTimestampArgTypesGenerator>()},
          {"try_make_timestamp_ntz",
           std::make_shared<MakeTimestampArgTypesGenerator>()}};

  std::unordered_map<std::string, std::shared_ptr<ArgValuesGenerator>>
      argValuesGenerators = {
          {"levenshtein", std::make_shared<LevenshteinArgValuesGenerator>()},
          {"lpad", std::make_shared<PadArgValuesGenerator>()},
          {"make_timestamp",
           std::make_shared<MakeTimestampArgValuesGenerator>()},
          {"make_timestamp_ntz",
           std::make_shared<MakeTimestampArgValuesGenerator>()},
          {"repeat", std::make_shared<RepeatArgValuesGenerator>()},
          {"round", std::make_shared<RoundArgValuesGenerator>()},
          {"rpad", std::make_shared<PadArgValuesGenerator>()},
          {"try_make_timestamp",
           std::make_shared<MakeTimestampArgValuesGenerator>()},
          {"try_make_timestamp_ntz",
           std::make_shared<MakeTimestampArgValuesGenerator>()}};

  std::shared_ptr<facebook::velox::memory::MemoryPool> rootPool{
      facebook::velox::memory::memoryManager()->addRootPool()};
  std::shared_ptr<ReferenceQueryRunner> referenceQueryRunner{nullptr};
  if (!FLAGS_spark_url.empty()) {
#ifdef VELOX_ENABLE_PARQUET
    // The input data is written to Parquet files for Spark to read.
    facebook::velox::parquet::registerParquetWriterFactory();
    const auto sparkSkipped = sparkSkippedFunctions();
    skipFunctions.insert(sparkSkipped.begin(), sparkSkipped.end());
    // Spark rounds the results of abs and unaryminus to 34 significant digits
    // for decimals.
    const auto absUnaryMinus =
        std::make_shared<AbsUnaryMinusArgTypesGenerator>();
    argTypesGenerators.emplace("abs", absUnaryMinus);
    argTypesGenerators.emplace("unaryminus", absUnaryMinus);
    // Spark interprets timestamps in the session time zone.
    queryConfigs.emplace(
        facebook::velox::core::QueryConfig::kAdjustTimestampToTimezone, "true");
    referenceQueryRunner = std::make_shared<SparkQueryRunner>(
        rootPool.get(),
        FLAGS_spark_url,
        "fuzzer",
        "expression",
        std::chrono::seconds(FLAGS_spark_query_timeout_sec),
        SparkQueryRunner::projectionSessionConfigs());
    LOG(INFO) << "Using Spark as the reference DB.";
#else
    LOG(ERROR) << "Spark as the reference DB requires Parquet support. "
               << "Build with VELOX_ENABLE_PARQUET=ON.";
    return 1;
#endif
  }

  if (!FLAGS_skip_functions.empty()) {
    const auto names = facebook::velox::exec::splitNames(FLAGS_skip_functions);
    skipFunctions.insert(names.begin(), names.end());
  }

  return FuzzerRunner::run(
      FLAGS_seed,
      skipFunctions,
      {{}},
      queryConfigs,
      argTypesGenerators,
      argValuesGenerators,
      referenceQueryRunner,
      std::make_shared<
          facebook::velox::fuzzer::SparkSpecialFormSignatureGenerator>());
}
