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

#include <memory>

#include "velox/dwio/common/ReaderFactory.h"

namespace facebook::velox::dwio::common {
class InputStreamProvider;
} // namespace facebook::velox::dwio::common

namespace facebook::velox::parquet {

/// Factory for creating Parquet readers.
///
/// Uses a default InputStreamProvider unless a custom provider is supplied.
class ParquetReaderFactory final : public dwio::common::ReaderFactory {
 public:
  explicit ParquetReaderFactory(
      std::shared_ptr<dwio::common::InputStreamProvider> inputStreamProvider =
          nullptr);

  /// Creates a factory with default implementations.
  static std::shared_ptr<ParquetReaderFactory> createDefault();

  /// Creates a factory with custom InputStreamProvider.
  /// Null provider will be replaced with default implementation.
  static std::shared_ptr<ParquetReaderFactory> create(
      std::shared_ptr<dwio::common::InputStreamProvider> inputStreamProvider =
          nullptr);

  std::unique_ptr<dwio::common::Reader> createReader(
      std::unique_ptr<dwio::common::BufferedInput> input,
      const dwio::common::ReaderOptions& options) override;

  const std::shared_ptr<dwio::common::InputStreamProvider>&
  inputStreamProvider() const {
    return inputStreamProvider_;
  }

 private:
  std::shared_ptr<dwio::common::InputStreamProvider> inputStreamProvider_;
};

} // namespace facebook::velox::parquet
