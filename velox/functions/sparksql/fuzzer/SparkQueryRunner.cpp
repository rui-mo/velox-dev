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
#include <cctype>
#include <memory>
#include <numeric>
#include <string>
#include <string_view>
#include <unordered_set>

#include <boost/algorithm/string.hpp>
#include <folly/ScopeGuard.h>
#include <google/protobuf/unknown_field_set.h>

#include "velox/functions/sparksql/fuzzer/SparkQueryRunner.h"

#include "arrow/buffer.h"
#include "arrow/c/bridge.h"
#include "arrow/io/api.h"
#include "arrow/ipc/api.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "grpc/grpc.h" // @manual
#include "velox/common/base/Fs.h"
#include "velox/common/testutil/TempFilePath.h"
#include "velox/dwio/common/WriterFactory.h"
#include "velox/dwio/parquet/writer/Writer.h"
#include "velox/exec/fuzzer/FuzzerUtil.h"
#include "velox/exec/fuzzer/PrestoSql.h"
#include "velox/exec/tests/utils/QueryAssertions.h"
#include "velox/functions/sparksql/fuzzer/SparkQueryRunnerToSqlPlanNodeVisitor.h"
#include "velox/functions/sparksql/fuzzer/SparkSql.h"
#include "velox/functions/sparksql/fuzzer/spark/connect/base.pb.h"
#include "velox/functions/sparksql/fuzzer/spark/connect/relations.pb.h"
#include "velox/vector/arrow/Bridge.h"

using namespace spark::connect;

namespace facebook::velox::functions::sparksql::fuzzer {
using namespace facebook::velox::common::testutil;

namespace {

// Returns 'vector' with the children of null rows set to null and the fields
// of nested structs named by toSparkFieldNames recursively. Velox leaves the
// children of null rows undefined, e.g. uninitialized after flattening a
// dictionary vector with nulls, but they are converted when exported to Arrow,
// which fails for timestamps that cannot be converted to microseconds. The
// names of the top-level fields are kept. 'vector' must be flattened.
VectorPtr toSparkVector(const VectorPtr& vector, bool topLevel = false) {
  auto* pool = vector->pool();
  switch (vector->encoding()) {
    case VectorEncoding::Simple::ROW: {
      auto* row = vector->asUnchecked<RowVector>();
      BufferPtr indices;
      if (row->mayHaveNulls()) {
        indices = allocateIndices(row->size(), pool);
        auto* rawIndices = indices->asMutable<vector_size_t>();
        std::iota(rawIndices, rawIndices + row->size(), 0);
      }
      std::vector<VectorPtr> children;
      std::vector<TypePtr> childTypes;
      children.reserve(row->childrenSize());
      childTypes.reserve(row->childrenSize());
      for (const auto& child : row->children()) {
        VectorPtr newChild = child;
        if (indices) {
          newChild = BaseVector::wrapInDictionary(
              row->nulls(), indices, row->size(), child);
          BaseVector::flattenVector(newChild);
        }
        children.push_back(toSparkVector(newChild));
        childTypes.push_back(children.back()->type());
      }
      const auto& rowType = row->type()->asRow();
      return std::make_shared<RowVector>(
          pool,
          ROW(topLevel ? rowType.names() : toSparkFieldNames(rowType),
              std::move(childTypes)),
          row->nulls(),
          row->size(),
          std::move(children));
    }
    case VectorEncoding::Simple::ARRAY: {
      auto* array = vector->asUnchecked<ArrayVector>();
      auto elements = toSparkVector(array->elements());
      return std::make_shared<ArrayVector>(
          pool,
          ARRAY(elements->type()),
          array->nulls(),
          array->size(),
          array->offsets(),
          array->sizes(),
          std::move(elements));
    }
    case VectorEncoding::Simple::MAP: {
      auto* map = vector->asUnchecked<MapVector>();
      auto keys = toSparkVector(map->mapKeys());
      auto values = toSparkVector(map->mapValues());
      return std::make_shared<MapVector>(
          pool,
          MAP(keys->type(), values->type()),
          map->nulls(),
          map->size(),
          map->offsets(),
          map->sizes(),
          std::move(keys),
          std::move(values));
    }
    case VectorEncoding::Simple::FLAT: {
      // Parquet cannot store year-month intervals. Write the number of months
      // as integers, which the generated SQL converts back to intervals.
      if (vector->type()->isIntervalYearMonth()) {
        auto* flat = vector->asUnchecked<FlatVector<int32_t>>();
        return std::make_shared<FlatVector<int32_t>>(
            pool,
            INTEGER(),
            flat->nulls(),
            flat->size(),
            flat->values(),
            std::vector<BufferPtr>{});
      }
      return vector;
    }
    default:
      return vector;
  }
}

void writeToFile(
    const std::string& path,
    const std::vector<RowVectorPtr>& data,
    memory::MemoryPool* pool) {
  VELOX_CHECK_GT(data.size(), 0);

  std::vector<VectorPtr> sparkData;
  sparkData.reserve(data.size());
  for (const auto& vector : data) {
    // When vector is dictionary-encoded, complex types are not supported in
    // exportFlattenedVector. Flatten the vector before writing to Parquet.
    // https://github.com/facebookincubator/velox/issues/10397
    VectorPtr flattened = vector;
    BaseVector::flattenVector(flattened);
    sparkData.push_back(toSparkVector(flattened, /*topLevel=*/true));
  }

  auto options = std::make_shared<dwio::common::WriterOptions>();
  options->schema = asRowType(sparkData[0]->type());
  options->memoryPool = pool;
  // Spark does not recognize int64-timestamp written as nano precision in
  // Parquet.
  auto parquetOptions = std::make_shared<parquet::ParquetWriterOptions>();
  parquetOptions->parquetWriteTimestampUnit = TimestampPrecision::kMicroseconds;
  // Velox timestamps are relative to the UTC epoch like Spark TIMESTAMP values.
  // Without a time zone, they are marked as not adjusted to UTC and Spark reads
  // them as TIMESTAMP_NTZ.
  parquetOptions->parquetWriteTimestampTimeZone = "UTC";
  options->formatSpecificOptions = parquetOptions;

  auto writeFile = std::make_unique<LocalWriteFile>(path, true, false);
  auto sink =
      std::make_unique<dwio::common::WriteFileSink>(std::move(writeFile), path);
  auto writer =
      dwio::common::getWriterFactory(dwio::common::FileFormat::PARQUET)
          ->createWriter(std::move(sink), options);

  for (const auto& vector : sparkData) {
    writer->write(vector);
  }
  writer->close();
}

// Returns the length-delimited fields with field number 'number' in a
// serialized protobuf message.
std::vector<std::string> getLengthDelimitedFields(
    const std::string& message,
    int number) {
  google::protobuf::UnknownFieldSet fields;
  if (!fields.ParseFromString(message)) {
    return {};
  }
  std::vector<std::string> values;
  for (auto i = 0; i < fields.field_count(); ++i) {
    const auto& field = fields.field(i);
    if (field.number() == number &&
        field.type() == google::protobuf::UnknownField::TYPE_LENGTH_DELIMITED) {
      values.push_back(field.length_delimited());
    }
  }
  return values;
}

// Details of an error reported by Spark Connect.
struct SparkError {
  // JSON list with the names of the Java exception class and its superclasses.
  std::string exceptionClasses;
  // Name of the Java exception class.
  std::string exceptionClass;
  // Error condition, e.g. DIVIDE_BY_ZERO. Empty for legacy errors.
  std::string condition;
  std::string sqlState;
};

// Returns the condition at the start of 'message', which looks like
// '[<condition>] <description>', or an empty string if there is none.
std::string extractCondition(std::string_view message) {
  const auto begin = message.find_first_not_of(" \t\r\n");
  if (begin == std::string_view::npos || message[begin] != '[') {
    return "";
  }
  const auto end = message.find(']', begin);
  if (end == std::string_view::npos || end == begin + 1) {
    return "";
  }
  const auto condition = message.substr(begin + 1, end - begin - 1);
  for (const unsigned char c : condition) {
    if (!std::isupper(c) && !std::isdigit(c) && c != '_' && c != '.') {
      return "";
    }
  }
  return std::string(condition);
}

// Returns the details of a failed Spark query. Spark Connect reports them in
// the google.rpc.ErrorInfo in the status details. The details are decoded from
// the wire format since the google.rpc protos are not compiled:
// google.rpc.Status has the details as google.protobuf.Any in field 3, Any has
// the type URL in field 1 and the value in field 2, ErrorInfo has the
// exception class name in field 1 and metadata entries in field 3, and each
// entry has a key in field 1 and a value in field 2. The condition and SQLSTATE
// fall back to the status message, which looks like '[<condition>] ...
// SQLSTATE: <state>' but might be truncated.
SparkError extractSparkError(const grpc::Status& status) {
  SparkError error;
  for (const auto& detail :
       getLengthDelimitedFields(status.error_details(), 3)) {
    const auto typeUrls = getLengthDelimitedFields(detail, 1);
    if (typeUrls.size() != 1 ||
        typeUrls[0] != "type.googleapis.com/google.rpc.ErrorInfo") {
      continue;
    }
    for (const auto& errorInfo : getLengthDelimitedFields(detail, 2)) {
      const auto reasons = getLengthDelimitedFields(errorInfo, 1);
      if (reasons.size() == 1) {
        error.exceptionClass = reasons[0];
      }
      for (const auto& entry : getLengthDelimitedFields(errorInfo, 3)) {
        const auto keys = getLengthDelimitedFields(entry, 1);
        const auto values = getLengthDelimitedFields(entry, 2);
        if (keys.size() != 1 || values.size() != 1) {
          continue;
        }
        if (keys[0] == "classes") {
          error.exceptionClasses = values[0];
        } else if (keys[0] == "errorClass") {
          error.condition = values[0];
        } else if (keys[0] == "sqlState") {
          error.sqlState = values[0];
        }
      }
    }
  }

  const auto& message = status.error_message();
  if (error.condition.empty()) {
    error.condition = extractCondition(message);
  }
  if (error.sqlState.empty()) {
    static constexpr std::string_view kSqlStatePrefix = "SQLSTATE: ";
    static constexpr size_t kSqlStateLength = 5;
    const auto pos = message.find(kSqlStatePrefix);
    if (pos != std::string::npos &&
        pos + kSqlStatePrefix.size() + kSqlStateLength <= message.size()) {
      error.sqlState =
          message.substr(pos + kSqlStatePrefix.size(), kSqlStateLength);
    }
  }
  return error;
}

// Returns true if 'name' is a type name in function signatures that has a
// Spark equivalent.
bool isSupportedTypeName(const std::string& name) {
  static const std::unordered_set<std::string> kTypeNames{
      "boolean",
      "tinyint",
      "smallint",
      "integer",
      "bigint",
      "real",
      "double",
      "varchar",
      "varbinary",
      "timestamp",
      "date",
      "decimal",
      "array",
      "map",
      "row",
      "function",
      "interval year to month",
      "any",
  };
  return kTypeNames.contains(boost::algorithm::to_lower_copy(name));
}

bool isSupportedType(
    const exec::TypeSignature& type,
    const std::unordered_set<std::string>& variables) {
  if (!variables.contains(type.baseName()) &&
      !isSupportedTypeName(type.baseName())) {
    return false;
  }
  for (const auto& parameter : type.parameters()) {
    if (!isSupportedType(parameter, variables)) {
      return false;
    }
  }
  return true;
}

} // namespace

SparkQueryRunner::SparkQueryRunner(
    memory::MemoryPool* pool,
    const std::string& coordinatorUri,
    const std::string& userId,
    const std::string& userName,
    std::chrono::seconds queryTimeout,
    const std::unordered_map<std::string, std::string>& sessionConfigs)
    : ReferenceQueryRunner(pool),
      userId_(userId),
      userName_(userName),
      sessionId_(generateUUID()),
      queryTimeout_(queryTimeout),
      sessionConfigs_(defaultSessionConfigs()) {
  for (const auto& [key, value] : sessionConfigs) {
    sessionConfigs_[key] = value;
  }
  grpc::ChannelArguments channelArguments;
  // Query results may exceed the default limit of 4MB.
  channelArguments.SetMaxReceiveMessageSize(-1);
  // Error messages and details are sent in trailing metadata, which may exceed
  // the default limit of 8KB.
  channelArguments.SetInt(GRPC_ARG_MAX_METADATA_SIZE, 1 << 20);
  stub_ = SparkConnectService::NewStub(
      grpc::CreateCustomChannel(
          coordinatorUri,
          grpc::InsecureChannelCredentials(),
          channelArguments));
  pool_ = aggregatePool()->addLeafChild("leaf");
  copyPool_ = aggregatePool()->addLeafChild("copy");
}

SparkQueryRunner::~SparkQueryRunner() {
  if (!sessionConfigured_) {
    return;
  }
  // Release the server-side session to free its resources.
  ReleaseSessionRequest request;
  request.set_session_id(sessionId_);
  *request.mutable_user_context() = userContext();
  ReleaseSessionResponse response;
  grpc::ClientContext context;
  context.set_deadline(
      std::chrono::system_clock::now() + std::chrono::seconds(10));
  const auto status = stub_->ReleaseSession(&context, request, &response);
  if (!status.ok()) {
    LOG(WARNING) << "Failed to release Spark session: "
                 << status.error_message() << ". Session: " << sessionId_;
  }
}

// static
const std::unordered_map<std::string, std::string>&
SparkQueryRunner::defaultSessionConfigs() {
  static const std::unordered_map<std::string, std::string> kConfigs{
      {"spark.sql.ansi.enabled", "false"},
      {"spark.sql.session.timeZone", "America/Los_Angeles"},
      {"spark.sql.legacy.timeParserPolicy", "CORRECTED"},
      // Velox writes dates and timestamps in the proleptic Gregorian calendar.
      {"spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED"},
      {"spark.sql.parquet.int96RebaseModeInRead", "CORRECTED"},
      // Velox keeps the last value of duplicate map keys unless
      // 'throw_exception_on_duplicate_map_keys' is enabled.
      {"spark.sql.mapKeyDedupPolicy", "LAST_WIN"},
      // Spark's generated code differs from the interpreted evaluation of
      // some expressions. See the comment of the class.
      {"spark.sql.codegen.wholeStage", "false"},
      {"spark.sql.codegen.factoryMode", "NO_CODEGEN"},
  };
  return kConfigs;
}

// static
const std::unordered_map<std::string, std::string>&
SparkQueryRunner::projectionSessionConfigs() {
  static const std::unordered_map<std::string, std::string> kConfigs{
      // ReorderAssociativeOperator ignores the eval mode of nested Add and
      // Multiply, e.g. it evaluates 1 + try_add(c0, 2) as c0 + (1 + 2), which
      // doesn't return null on overflow. SimplifyConditionals removes the
      // condition of an if whose branches are the same, so errors in the
      // condition are lost. NullPropagation, SimplifyBinaryComparison and
      // SimplifyExtractValueOps replace isnull(e) and e = e with false and
      // true if e is not nullable, and get(array(e0, e1), 0) with e0, without
      // evaluating e and e1, so errors in them are lost too.
      {"spark.sql.optimizer.excludedRules",
       "org.apache.spark.sql.catalyst.optimizer.ConstantFolding,"
       "org.apache.spark.sql.catalyst.optimizer.ReorderAssociativeOperator,"
       "org.apache.spark.sql.catalyst.optimizer.SimplifyConditionals,"
       "org.apache.spark.sql.catalyst.optimizer.NullPropagation,"
       "org.apache.spark.sql.catalyst.optimizer.SimplifyBinaryComparison,"
       "org.apache.spark.sql.catalyst.optimizer.SimplifyExtractValueOps"},
      {"spark.sql.subexpressionElimination.enabled", "false"},
  };
  return kConfigs;
}

// static
exec::test::ReferenceQueryErrorCode SparkQueryRunner::classifyError(
    const grpc::Status& status) {
  const auto& errorMessage = status.error_message();
  const auto error = extractSparkError(status);
  if (error.condition == "PARSE_SYNTAX_ERROR" ||
      error.exceptionClass ==
          "org.apache.spark.sql.catalyst.parser.ParseException") {
    // The SQL generated from the plan is invalid.
    VELOX_FAIL("Spark failed to parse the query: {}", errorMessage);
  }
  if (errorMessage.find("stopped SparkContext") != std::string::npos) {
    VELOX_FAIL("Spark Connect server is not usable: {}", errorMessage);
  }

  static constexpr auto kUnsupported =
      exec::test::ReferenceQueryErrorCode::kReferenceQueryUnsupported;
  // Fatal JVM errors in the Spark driver, e.g. OutOfMemoryError, are reported
  // without details.
  if (status.error_code() == grpc::StatusCode::UNKNOWN &&
      status.error_details().empty()) {
    return kUnsupported;
  }
  // Fatal JVM errors in Spark executors, which make the executors exit.
  for (const auto* fatalError :
       {"java.lang.OutOfMemoryError",
        "java.lang.StackOverflowError",
        "ExecutorLostFailure"}) {
    if (errorMessage.find(fatalError) != std::string::npos) {
      return kUnsupported;
    }
  }
  // Analysis errors mean that Spark cannot run the query, e.g. because of an
  // unknown function or unexpected argument types, not that the query fails on
  // the data. Legacy analysis errors have no SQLSTATE.
  if (error.exceptionClasses.find(
          "\"org.apache.spark.sql.AnalysisException\"") != std::string::npos) {
    return kUnsupported;
  }
  // Spark raises these errors for datetime patterns and strings while
  // evaluating expressions, although their SQLSTATE 42K0B is in class 42.
  static const std::unordered_set<std::string> kRuntimeConditions{
      "INCONSISTENT_BEHAVIOR_CROSS_VERSION.DATETIME_PATTERN_RECOGNITION",
      "INCONSISTENT_BEHAVIOR_CROSS_VERSION.DATETIME_WEEK_BASED_PATTERN",
      "INCONSISTENT_BEHAVIOR_CROSS_VERSION.PARSE_DATETIME_BY_NEW_PARSER"};
  if (kRuntimeConditions.contains(error.condition)) {
    return exec::test::ReferenceQueryErrorCode::kReferenceQueryFail;
  }
  // Syntax errors and access rule violations (42), unsupported features (0A),
  // resource limits (53, 54) and internal errors (XX).
  static const std::unordered_set<std::string> kUnsupportedClasses{
      "42", "0A", "53", "54", "XX"};
  if (kUnsupportedClasses.contains(error.sqlState.substr(0, 2))) {
    return kUnsupported;
  }
  return exec::test::ReferenceQueryErrorCode::kReferenceQueryFail;
}

const std::vector<TypePtr>& SparkQueryRunner::supportedScalarTypes() const {
  static const std::vector<TypePtr> kScalarTypes{
      BOOLEAN(),
      TINYINT(),
      SMALLINT(),
      INTEGER(),
      BIGINT(),
      REAL(),
      DOUBLE(),
      VARCHAR(),
      VARBINARY(),
      TIMESTAMP(),
      DATE(),
  };
  return kScalarTypes;
}

const std::unordered_map<std::string, DataSpec>&
SparkQueryRunner::aggregationFunctionDataSpecs() const {
  static const std::unordered_map<std::string, DataSpec>
      kAggregationFunctionDataSpecs{};

  return kAggregationFunctionDataSpecs;
}

bool SparkQueryRunner::isSupported(const exec::FunctionSignature& signature) {
  std::unordered_set<std::string> variables;
  for (const auto& [name, _] : signature.variables()) {
    variables.insert(name);
  }
  if (!isSupportedType(signature.returnType(), variables)) {
    return false;
  }
  for (const auto& argument : signature.argumentTypes()) {
    if (!isSupportedType(argument, variables)) {
      return false;
    }
  }
  return true;
}

bool SparkQueryRunner::isConstantExprSupported(const core::TypedExprPtr& expr) {
  try {
    toSparkSql(expr);
  } catch (const VeloxException& e) {
    if (e.errorCode() != error_code::kUnsupported) {
      throw;
    }
    return false;
  }
  return true;
}

std::optional<std::string> SparkQueryRunner::toSql(
    const velox::core::PlanNodePtr& plan) {
  exec::test::PrestoSqlPlanNodeVisitorContext context;
  SparkQueryRunnerToSqlPlanNodeVisitor visitor;
  try {
    plan->accept(visitor, context);
  } catch (const VeloxException& e) {
    if (e.errorCode() != error_code::kUnsupported &&
        e.errorCode() != error_code::kNotImplemented) {
      throw;
    }
    LOG(INFO) << "Spark query not supported: " << e.message();
    return std::nullopt;
  }

  return context.sql;
}

std::pair<
    std::optional<std::multiset<std::vector<variant>>>,
    exec::test::ReferenceQueryErrorCode>
SparkQueryRunner::execute(const core::PlanNodePtr& plan) {
  std::pair<
      std::optional<std::vector<RowVectorPtr>>,
      exec::test::ReferenceQueryErrorCode>
      result = executeAndReturnVector(plan);
  if (result.first) {
    return std::make_pair(
        exec::test::materialize(*result.first), result.second);
  }
  return std::make_pair(std::nullopt, result.second);
}

std::pair<
    std::optional<std::vector<RowVectorPtr>>,
    exec::test::ReferenceQueryErrorCode>
SparkQueryRunner::executeAndReturnVector(const core::PlanNodePtr& plan) {
  const std::optional<std::string> sql = toSql(plan);
  if (!sql.has_value()) {
    return std::make_pair(
        std::nullopt,
        exec::test::ReferenceQueryErrorCode::kReferenceQueryUnsupported);
  }

  std::unordered_map<std::string, std::vector<RowVectorPtr>> inputMap =
      getAllTables(plan);
  for (const auto& [tableName, input] : inputMap) {
    auto inputType = asRowType(input[0]->type());
    if (inputType->size() == 0) {
      inputMap[tableName] = {exec::test::makeNullRows(
          input, fmt::format("{}x", tableName), pool())};
    }
  }

  // Temporary files must outlive the query.
  auto writerPool = aggregatePool()->addAggregateChild("writer");
  std::vector<std::shared_ptr<TempFilePath>> tempFiles;
  tempFiles.reserve(inputMap.size());
  // The views read the temporary files, so they are dropped with the files.
  std::vector<std::string> viewNames;
  SCOPE_EXIT {
    dropViews(viewNames);
  };
  for (const auto& [tableName, input] : inputMap) {
    auto tempFile = TempFilePath::create();
    tempFiles.emplace_back(tempFile);
    const auto& filePath = tempFile->getPath();
    try {
      writeToFile(filePath, input, writerPool.get());
    } catch (const VeloxException& e) {
      LOG(WARNING) << "Spark query unsupported: Failed to write input to "
                   << "Parquet: " << e.message();
      return std::make_pair(
          std::nullopt,
          exec::test::ReferenceQueryErrorCode::kReferenceQueryUnsupported);
    }
    // Create temporary view for this table in Spark by reading the generated
    // Parquet file.
    const auto viewSql = fmt::format(
        "CREATE OR REPLACE TEMPORARY VIEW {} AS SELECT * FROM parquet.`file://{}`",
        tableName,
        filePath);
    const auto viewResult = executeQuery(viewSql);
    if (viewResult.errorCode.has_value()) {
      // For example, the view fails to read the file if a previous query ran
      // an executor out of memory.
      LOG(WARNING) << "Spark query unsupported: Failed to create Spark view: "
                   << viewResult.errorMessage << "\nQuery: " << viewSql;
      return std::make_pair(
          std::nullopt,
          exec::test::ReferenceQueryErrorCode::kReferenceQueryUnsupported);
    }
    viewNames.push_back(tableName);
  }

  auto result = executeQuery(*sql);
  VLOG(1) << "Spark query: " << *sql;
  if (!result.errorCode.has_value()) {
    return std::make_pair(
        std::move(result.data), exec::test::ReferenceQueryErrorCode::kSuccess);
  }

  LOG(WARNING) << fmt::format(
      "Spark query {}: {}\nQuery: {}",
      result.errorCode ==
              exec::test::ReferenceQueryErrorCode::kReferenceQueryUnsupported
          ? "unsupported"
          : "failed",
      result.errorMessage,
      *sql);
  return std::make_pair(std::nullopt, *result.errorCode);
}

std::vector<RowVectorPtr> SparkQueryRunner::execute(const std::string& sql) {
  auto result = executeQuery(sql);
  VELOX_USER_CHECK(
      !result.errorCode.has_value(),
      "Spark query failed: {}. Query: {}",
      result.errorMessage,
      sql);
  return std::move(result.data);
}

void SparkQueryRunner::dropViews(const std::vector<std::string>& viewNames) {
  for (const auto& viewName : viewNames) {
    // Must not throw because it runs during stack unwinding.
    try {
      const auto result =
          executeQuery(fmt::format("DROP VIEW IF EXISTS {}", viewName));
      if (result.errorCode.has_value()) {
        LOG(WARNING) << "Failed to drop Spark view " << viewName << ": "
                     << result.errorMessage;
      }
    } catch (const std::exception& e) {
      LOG(WARNING) << "Failed to drop Spark view " << viewName << ": "
                   << e.what();
    }
  }
}

UserContext SparkQueryRunner::userContext() const {
  UserContext context;
  context.set_user_id(userId_);
  context.set_user_name(userName_);
  return context;
}

void SparkQueryRunner::ensureSessionConfigured() {
  if (sessionConfigured_) {
    return;
  }
  setSessionConfigs(sessionConfigs_);
  sessionConfigured_ = true;
}

void SparkQueryRunner::setSessionConfigs(
    const std::unordered_map<std::string, std::string>& configs) {
  ConfigRequest::Operation operation;
  auto* set = operation.mutable_set();
  for (const auto& [key, value] : configs) {
    auto* pair = set->add_pairs();
    pair->set_key(key);
    pair->set_value(value);
  }
  updateSessionConfigs(operation);
}

void SparkQueryRunner::updateSessionConfigs(
    const ConfigRequest::Operation& operation) {
  ConfigRequest request;
  request.set_session_id(sessionId_);
  *request.mutable_user_context() = userContext();
  *request.mutable_operation() = operation;

  ConfigResponse response;
  grpc::ClientContext context;
  context.set_deadline(std::chrono::system_clock::now() + queryTimeout_);
  const auto status = stub_->Config(&context, request, &response);
  VELOX_CHECK(
      status.ok(),
      "Failed to update Spark session configs: {}",
      status.error_message());
}

void SparkQueryRunner::interruptSession() {
  InterruptRequest request;
  request.set_session_id(sessionId_);
  *request.mutable_user_context() = userContext();
  request.set_interrupt_type(InterruptRequest::INTERRUPT_TYPE_ALL);

  InterruptResponse response;
  grpc::ClientContext context;
  context.set_deadline(
      std::chrono::system_clock::now() + std::chrono::seconds(30));
  const auto status = stub_->Interrupt(&context, request, &response);
  if (!status.ok()) {
    LOG(WARNING) << "Failed to interrupt Spark session: "
                 << status.error_message();
  }
}

SparkQueryRunner::QueryResult SparkQueryRunner::executeQuery(
    const std::string& sql) {
  ensureSessionConfigured();

  ExecutePlanRequest request;
  request.set_session_id(sessionId_);
  *request.mutable_user_context() = userContext();
  request.mutable_plan()->mutable_root()->mutable_sql()->set_query(sql);

  grpc::ClientContext clientContext;
  clientContext.set_deadline(std::chrono::system_clock::now() + queryTimeout_);
  auto reader = stub_->ExecutePlan(&clientContext, request);
  ExecutePlanResponse response;
  QueryResult result;
  while (reader->Read(&response)) {
    VELOX_CHECK_EQ(
        response.session_id(),
        sessionId_,
        "The session id of the response does not match that of the request.");
    if (response.has_arrow_batch()) {
      auto batchResults = readArrowData(response.arrow_batch().data());
      result.data.insert(
          result.data.end(),
          std::make_move_iterator(batchResults.begin()),
          std::make_move_iterator(batchResults.end()));
    }
  }

  const auto status = reader->Finish();
  if (status.ok()) {
    return result;
  }

  result.data.clear();
  result.errorMessage = status.error_message();
  switch (status.error_code()) {
    case grpc::StatusCode::DEADLINE_EXCEEDED:
      interruptSession();
      result.errorCode =
          exec::test::ReferenceQueryErrorCode::kReferenceQueryUnsupported;
      result.errorMessage = fmt::format(
          "Query timed out after {} seconds", queryTimeout_.count());
      break;
    case grpc::StatusCode::UNAVAILABLE:
    case grpc::StatusCode::UNAUTHENTICATED:
    case grpc::StatusCode::PERMISSION_DENIED:
    case grpc::StatusCode::UNIMPLEMENTED:
    case grpc::StatusCode::CANCELLED:
      VELOX_FAIL(
          "Failed to run query in Spark Connect server: {}",
          status.error_message());
    case grpc::StatusCode::RESOURCE_EXHAUSTED:
      result.errorCode =
          exec::test::ReferenceQueryErrorCode::kReferenceQueryUnsupported;
      break;
    default:
      result.errorCode = classifyError(status);
  }
  return result;
}

std::string SparkQueryRunner::generateUUID() {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dis(0, 15);
  std::stringstream ss;
  ss << std::hex;
  for (int i = 0; i < 32; i++) {
    ss << "0123456789abcdef"[dis(gen)];
    if (i == 7 || i == 11 || i == 15 || i == 19)
      ss << "-";
  }
  return ss.str();
}

std::vector<RowVectorPtr> SparkQueryRunner::readArrowData(
    const std::string& data) {
  auto buffer = std::make_shared<arrow::Buffer>(data);
  auto bufferReader = std::make_shared<arrow::io::BufferReader>(buffer);
  arrow::Result<std::shared_ptr<arrow::ipc::RecordBatchReader>> result =
      arrow::ipc::RecordBatchStreamReader::Open(bufferReader);
  VELOX_CHECK(
      result.ok(),
      "Failed to open RecordBatchReader: {}.",
      result.status().ToString());

  std::shared_ptr<arrow::ipc::RecordBatchReader> reader = result.ValueUnsafe();
  std::shared_ptr<arrow::Schema> schema = reader->schema();

  arrow::Result<std::shared_ptr<arrow::RecordBatch>> batchResult;
  std::vector<RowVectorPtr> results;
  while ((batchResult = reader->Next()).ok() && batchResult.ValueUnsafe()) {
    std::shared_ptr<arrow::RecordBatch> batch = batchResult.ValueUnsafe();
    // Export the schema for each batch because importFromArrowAsOwner takes
    // ownership of it.
    ArrowSchema arrowSchema;
    auto status = arrow::ExportSchema(*schema, &arrowSchema);
    VELOX_CHECK(status.ok(), "Failed to export schema: {}.", status.ToString());
    ArrowArray arrowArray;
    status = ExportRecordBatch(*batch, &arrowArray);
    VELOX_CHECK(status.ok(), "Failed to export array: {}.", status.ToString());
    auto rv = std::dynamic_pointer_cast<RowVector>(
        importFromArrowAsOwner(arrowSchema, arrowArray, pool()));
    auto copy =
        BaseVector::create<RowVector>(rv->type(), rv->size(), copyPool_.get());
    copy->copy(rv.get(), 0, 0, rv->size());
    results.push_back(copy);
  }
  VELOX_CHECK(
      batchResult.ok(),
      "Failed to read batch: {}.",
      batchResult.status().ToString());
  return results;
}
} // namespace facebook::velox::functions::sparksql::fuzzer
