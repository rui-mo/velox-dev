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
#include <folly/container/F14Map.h>
#include <limits>
#include "velox/functions/lib/Re2Functions.h"
#include "velox/functions/lib/Utf8Utils.h"
#include "velox/functions/lib/string/StringImpl.h"

namespace facebook::velox::functions::sparksql {
namespace {

using ::re2::RE2;

void ensureRegexIsConstant(
    const char* functionName,
    const VectorPtr& patternVector) {
  if (!patternVector || !patternVector->isConstantEncoding()) {
    VELOX_USER_FAIL("{} requires a constant pattern.", functionName);
  }
}

// The replacement of regexp_replace parsed following
// java.util.regex.Matcher.appendReplacement. A backslash escapes the next
// character, '$n' refers to a numbered group and '${name}' to a named group.
class JavaReplacement {
 public:
  JavaReplacement(const RE2& re, std::string_view replacement)
      : replacement_(replacement) {
    error_ = parse(re);
  }

  // Returns the number of submatches needed to expand the replacement.
  int numSubmatches() const {
    return maxGroup_ + 1;
  }

  // Appends the replacement of a match to 'result'. Throws if the replacement
  // is invalid. Like Java, the error is only reported if the pattern matches.
  template <typename TResult>
  void appendTo(TResult& result, const re2::StringPiece* submatches) const {
    if (error_.has_value()) {
      VELOX_USER_FAIL(
          "Invalid replacement '{}': {}.", replacement_, error_.value());
    }
    for (const auto& piece : pieces_) {
      if (piece.group < 0) {
        result.append(piece.literal);
      } else {
        const auto& submatch = submatches[piece.group];
        result.append(std::string_view(submatch.data(), submatch.size()));
      }
    }
  }

 private:
  // A literal if 'group' is negative, otherwise a reference to a group.
  struct Piece {
    std::string literal;
    int32_t group;
  };

  static bool isDigit(char c) {
    return c >= '0' && c <= '9';
  }

  // Java only allows ASCII letters and digits in group names.
  static bool isGroupNameChar(char c) {
    return isDigit(c) || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
  }

  // Parses the replacement into 'pieces_'. Returns an error message if the
  // replacement is invalid.
  std::optional<std::string> parse(const RE2& re) {
    const std::string_view replacement{replacement_};
    const int32_t numGroups = re.NumberOfCapturingGroups();
    std::string literal;
    size_t i = 0;
    while (i < replacement.size()) {
      const char c = replacement[i++];
      if (c == '\\') {
        if (i == replacement.size()) {
          return "character to be escaped is missing";
        }
        literal.push_back(replacement[i++]);
        continue;
      }
      if (c != '$') {
        literal.push_back(c);
        continue;
      }
      if (i == replacement.size()) {
        return "group index is missing";
      }
      int32_t group;
      if (replacement[i] == '{') {
        const auto nameStart = ++i;
        while (i < replacement.size() && isGroupNameChar(replacement[i])) {
          ++i;
        }
        const std::string name{replacement.substr(nameStart, i - nameStart)};
        if (name.empty()) {
          return "named capturing group has 0 length name";
        }
        if (i == replacement.size() || replacement[i] != '}') {
          return "named capturing group is missing trailing '}'";
        }
        ++i;
        if (isDigit(name[0])) {
          return fmt::format(
              "capturing group name {{{}}} starts with digit character", name);
        }
        const auto it = re.NamedCapturingGroups().find(name);
        if (it == re.NamedCapturingGroups().end()) {
          return fmt::format("no group with name {{{}}}", name);
        }
        group = it->second;
      } else {
        if (!isDigit(replacement[i])) {
          return "illegal group reference";
        }
        group = replacement[i++] - '0';
        // Takes the longest group number that exists.
        while (i < replacement.size() && isDigit(replacement[i])) {
          const int32_t next = group * 10 + (replacement[i] - '0');
          if (next > numGroups) {
            break;
          }
          group = next;
          ++i;
        }
        if (group > numGroups) {
          return fmt::format("no group {}", group);
        }
      }
      if (!literal.empty()) {
        pieces_.push_back({std::move(literal), -1});
        literal.clear();
      }
      pieces_.push_back({"", group});
      maxGroup_ = std::max(maxGroup_, group);
    }
    if (!literal.empty()) {
      pieces_.push_back({std::move(literal), -1});
    }
    return std::nullopt;
  }

  const std::string replacement_;
  std::vector<Piece> pieces_;
  int32_t maxGroup_{0};
  std::optional<std::string> error_;
};

// REGEXP_REPLACE(string, pattern, overwrite) → string
// REGEXP_REPLACE(string, pattern, overwrite, position) → string
//
// If a string has a substring that matches the given pattern, replace
// the match in the string with overwrite and return the string. If
// optional parameter position is provided, only make replacements
// after that position in the string (1 indexed).
//
// If position <= 0, throw error.
// If position > 1 and position > length of string, return string.
template <typename T>
struct RegexpReplaceFunction {
  RegexpReplaceFunction() : cache_(0) {}

  VELOX_DEFINE_FUNCTION_TYPES(T);

  static constexpr bool is_default_ascii_behavior = true;

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& inputTypes,
      const core::QueryConfig& config,
      const arg_type<Varchar>* stringInput,
      const arg_type<Varchar>* pattern,
      const arg_type<Varchar>* replacement) {
    initialize(inputTypes, config, stringInput, pattern, replacement, nullptr);
  }

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Varchar>* /*stringInput*/,
      const arg_type<Varchar>* pattern,
      const arg_type<Varchar>* replacement,
      const arg_type<int32_t>* /*position*/) {
    if (pattern) {
      const auto processedPattern = prepareRegexpReplacePattern(*pattern);
      re_.emplace(processedPattern, RE2::Quiet);
      VELOX_USER_CHECK(
          re_->ok(),
          "Invalid regular expression {}: {}.",
          processedPattern,
          re_->error());

      if (replacement) {
        // Only when both the 'replacement' and 'pattern' are constants can they
        // be processed during initialization; otherwise, each row needs to be
        // processed separately.
        constantReplacement_.emplace(
            re_.value(),
            std::string_view(replacement->data(), replacement->size()));
      }
    }
    cache_.setMaxCompiledRegexes(config.exprMaxCompiledRegexes());
  }

  void call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& stringInput,
      const arg_type<Varchar>& pattern,
      const arg_type<Varchar>& replacement) {
    call(result, stringInput, pattern, replacement, 1);
  }

  void call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& stringInput,
      const arg_type<Varchar>& pattern,
      const arg_type<Varchar>& replacement,
      const arg_type<int32_t>& position) {
    doCall<false>(result, stringInput, pattern, replacement, position);
  }

  void callAscii(
      out_type<Varchar>& result,
      const arg_type<Varchar>& stringInput,
      const arg_type<Varchar>& pattern,
      const arg_type<Varchar>& replacement) {
    callAscii(result, stringInput, pattern, replacement, 1);
  }

  void callAscii(
      out_type<Varchar>& result,
      const arg_type<Varchar>& stringInput,
      const arg_type<Varchar>& pattern,
      const arg_type<Varchar>& replacement,
      const arg_type<int32_t>& position) {
    doCall<true>(result, stringInput, pattern, replacement, position);
  }

 private:
  template <bool isAscii>
  void doCall(
      out_type<Varchar>& result,
      const arg_type<Varchar>& stringInput,
      const arg_type<Varchar>& pattern,
      const arg_type<Varchar>& replacement,
      const arg_type<int32_t>& position) {
    VELOX_USER_CHECK_GE(position, 1, "regexp_replace requires a position >= 1");
    const size_t start = functions::stringImpl::cappedByteLength<isAscii>(
        stringInput, position - 1);
    // Like Spark, returns the input unchanged if position is beyond the last
    // character, unless position is 1.
    if (position > 1 && start == stringInput.size()) {
      result = stringInput;
      return;
    }
    performReplace(result, stringInput, pattern, replacement, start);
  }

  void performReplace(
      out_type<Varchar>& result,
      const arg_type<Varchar>& stringInput,
      const arg_type<Varchar>& pattern,
      const arg_type<Varchar>& replace,
      const arg_type<int32_t>& position) {
    auto& re = ensurePattern(pattern);
    std::optional<JavaReplacement> rowReplacement;
    if (!constantReplacement_.has_value()) {
      rowReplacement.emplace(
          re, std::string_view(replace.data(), replace.size()));
    }
    const auto& replacement = constantReplacement_.has_value()
        ? constantReplacement_.value()
        : rowReplacement.value();
    const int numSubmatches = replacement.numSubmatches();

    // Spark searches from 'position' as if the input started there, so '^'
    // matches at 'position'.
    const re2::StringPiece text(
        stringInput.data() + position, stringInput.size() - position);
    submatches_.resize(numSubmatches);
    result.append(std::string_view(stringInput.data(), position));

    // Unlike RE2::GlobalReplace, java.util.regex.Matcher allows an empty match
    // right after a non-empty match, e.g. regexp_replace('abc', 'b*', '-')
    // returns '-a--c-'. After an empty match, the search resumes one character
    // later.
    size_t searchPosition = 0;
    size_t appendPosition = 0;
    while (searchPosition <= text.size() &&
           re.Match(
               text,
               searchPosition,
               text.size(),
               RE2::UNANCHORED,
               submatches_.data(),
               numSubmatches)) {
      const size_t matchStart = submatches_[0].data() - text.data();
      const size_t matchEnd = matchStart + submatches_[0].size();
      result.append(
          std::string_view(
              text.data() + appendPosition, matchStart - appendPosition));
      replacement.appendTo(result, submatches_.data());
      appendPosition = matchEnd;
      if (matchStart != matchEnd) {
        searchPosition = matchEnd;
      } else if (matchEnd < text.size()) {
        int32_t codePoint;
        const auto charLength = tryGetUtf8CharLength(
            text.data() + matchEnd, text.size() - matchEnd, codePoint);
        // An invalid UTF-8 character has a length of -charLength.
        searchPosition = matchEnd + std::abs(charLength);
      } else {
        break;
      }
    }
    result.append(
        std::string_view(
            text.data() + appendPosition, text.size() - appendPosition));
  }

  RE2& ensurePattern(const arg_type<Varchar>& pattern) {
    if (re_.has_value()) {
      return re_.value();
    }
    auto processedPattern = prepareRegexpReplacePattern(pattern);
    return *cache_.findOrCompile(StringView(processedPattern));
  }

  // Used when pattern is constant.
  std::optional<RE2> re_;

  // Used when replacement is constant.
  std::optional<JavaReplacement> constantReplacement_;

  // Used when pattern is not constant.
  detail::ReCache cache_;

  // Scratch memory for the submatches of a match.
  std::vector<re2::StringPiece> submatches_;
};

// Calls 'function' with a constant group id of 1 appended to the arguments.
// Spark extracts group 1 if the group id is omitted.
class DefaultGroupIdFunction final : public exec::VectorFunction {
 public:
  explicit DefaultGroupIdFunction(
      std::shared_ptr<exec::VectorFunction> function)
      : function_(std::move(function)) {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const final {
    std::vector<VectorPtr> argsWithGroupId{args};
    argsWithGroupId.push_back(
        BaseVector::createConstant(INTEGER(), 1, rows.end(), context.pool()));
    function_->apply(rows, argsWithGroupId, outputType, context, result);
  }

 private:
  const std::shared_ptr<exec::VectorFunction> function_;
};

// Returns 'inputArgs' with an INTEGER group id argument appended.
std::vector<exec::VectorFunctionArg> withGroupIdArg(
    const std::vector<exec::VectorFunctionArg>& inputArgs) {
  auto argsWithGroupId = inputArgs;
  argsWithGroupId.push_back({INTEGER(), nullptr});
  return argsWithGroupId;
}

} // namespace

// These functions delegate to the RE2-based implementations in
// common/RegexFunctions.h, but check to ensure that syntax that has different
// semantics between Spark (which uses java.util.regex) and RE2 throws an
// error.
std::shared_ptr<exec::VectorFunction> makeRLike(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& config) {
  // Return any errors from re2Search() first.
  auto result = makeRe2Search(name, inputArgs, config);
  ensureRegexIsConstant("RLIKE", inputArgs[1].constantValue);
  return result;
}

std::shared_ptr<exec::VectorFunction> makeRegexExtract(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& config) {
  if (inputArgs.size() == 2) {
    return std::make_shared<DefaultGroupIdFunction>(
        makeRegexExtract(name, withGroupIdArg(inputArgs), config));
  }
  auto result = makeRe2Extract(
      name,
      inputArgs,
      config,
      /*emptyNoMatch=*/true,
      /*checkGroupIdOnMatch=*/true);
  ensureRegexIsConstant("REGEXP_EXTRACT", inputArgs[1].constantValue);
  return result;
}

std::shared_ptr<exec::VectorFunction> makeRegexExtractAll(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& config) {
  if (inputArgs.size() == 2) {
    return std::make_shared<DefaultGroupIdFunction>(
        makeRegexExtractAll(name, withGroupIdArg(inputArgs), config));
  }
  return makeRe2ExtractAll(
      name,
      inputArgs,
      config,
      /*emptyNoMatch=*/true,
      /*checkGroupIdOnMatch=*/true,
      /*advanceByCharOnEmptyMatch=*/true);
}

void registerRegexpReplace(const std::string& prefix) {
  registerFunction<RegexpReplaceFunction, Varchar, Varchar, Varchar, Varchar>(
      {prefix + "regexp_replace"});
  registerFunction<
      RegexpReplaceFunction,
      Varchar,
      Varchar,
      Varchar,
      Varchar,
      int32_t>({prefix + "regexp_replace"});
}

template <typename T>
struct RegexpInstrFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void initialize(
      const std::vector<TypePtr>& /*inputTypes*/,
      const core::QueryConfig& config,
      const arg_type<Varchar>* /*stringInput*/,
      const arg_type<Varchar>* pattern) {
    if (pattern) {
      const auto processedPattern = prepareRegexpReplacePattern(*pattern);
      re_.emplace(processedPattern, RE2::Quiet);
      VELOX_USER_CHECK(
          re_->ok(),
          "Invalid regular expression {}: {}.",
          processedPattern,
          re_->error());
    }
    cache_.setMaxCompiledRegexes(config.exprMaxCompiledRegexes());
  }

  FOLLY_ALWAYS_INLINE void call(
      int32_t& result,
      const arg_type<Varchar>& stringInput,
      const arg_type<Varchar>& pattern) {
    const re2::StringPiece input(stringInput.data(), stringInput.size());
    const RE2& re = re_.has_value() ? re_.value() : getOrCompileRegex(pattern);

    re2::StringPiece match;
    if (re.Match(input, 0, input.size(), RE2::UNANCHORED, &match, 1)) {
      const size_t byteOffset =
          static_cast<size_t>(match.data() - input.data());
      if (byteOffset == 0) {
        result = 1;
      } else {
        const int64_t charCount =
            functions::stringCore::lengthUnicode(input.data(), byteOffset);
        VELOX_USER_CHECK_LE(
            charCount,
            static_cast<int64_t>(std::numeric_limits<int32_t>::max()) - 1,
            "regexp_instr: string has too many characters for int32_t result");
        result = static_cast<int32_t>(charCount + 1);
      }
    } else {
      result = 0;
    }
  }

 private:
  const RE2& getOrCompileRegex(const arg_type<Varchar>& pattern) {
    processedPatternBuf_ = prepareRegexpReplacePattern(pattern);
    return *cache_.findOrCompile(StringView(processedPatternBuf_));
  }

  std::optional<RE2> re_;
  detail::ReCache cache_{0};
  std::string processedPatternBuf_;
};

void registerRegexpInstr(const std::string& prefix) {
  registerFunction<RegexpInstrFunction, int32_t, Varchar, Varchar>(
      {prefix + "regexp_instr"});
}

} // namespace facebook::velox::functions::sparksql
