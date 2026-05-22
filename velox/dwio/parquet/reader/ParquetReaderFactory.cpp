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

#include "velox/dwio/parquet/reader/ParquetReaderFactory.h"
#include "velox/dwio/common/BufferedInput.h"
#include "velox/dwio/common/InputStreamProvider.h"
#include "velox/dwio/parquet/reader/ParquetReader.h"

namespace facebook::velox::parquet {

ParquetReaderFactory::ParquetReaderFactory(
    std::shared_ptr<dwio::common::InputStreamProvider> inputStreamProvider)
    : dwio::common::ReaderFactory(dwio::common::FileFormat::PARQUET),
      inputStreamProvider_(
          inputStreamProvider
              ? std::move(inputStreamProvider)
              : dwio::common::InputStreamProvider::createDefault()) {}

std::shared_ptr<ParquetReaderFactory> ParquetReaderFactory::createDefault() {
  return std::make_shared<ParquetReaderFactory>(
      dwio::common::InputStreamProvider::createDefault());
}

std::shared_ptr<ParquetReaderFactory> ParquetReaderFactory::create(
    std::shared_ptr<dwio::common::InputStreamProvider> inputStreamProvider) {
  return std::make_shared<ParquetReaderFactory>(std::move(inputStreamProvider));
}

std::unique_ptr<dwio::common::Reader> ParquetReaderFactory::createReader(
    std::unique_ptr<dwio::common::BufferedInput> input,
    const dwio::common::ReaderOptions& options) {
  // Preserve existing behavior for the default provider. If a custom provider
  // is registered, rebuild the input using provider-created ReadFile so
  // downstream projects (e.g., Gluten) can customize file access.
  if (dynamic_cast<dwio::common::DefaultInputStreamProvider*>(
          inputStreamProvider_.get()) == nullptr) {
    const auto& filePath = input->getReadFile()->getName();
    auto readFile = inputStreamProvider_->createReadFile(filePath);
    input = std::make_unique<dwio::common::BufferedInput>(
        std::move(readFile), options.memoryPool());
  }

  return std::make_unique<ParquetReader>(
      std::move(input), options, create(inputStreamProvider_));
}

} // namespace facebook::velox::parquet
