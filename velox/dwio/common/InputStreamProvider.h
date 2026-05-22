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
#include <string>

#include "velox/common/file/File.h"
#include "velox/dwio/common/InputStream.h"

namespace facebook::velox::dwio::common {

/// Abstracts the creation of ReadFile instances for different storage backends.
/// Implementations can provide custom storage access strategies, enabling
/// downstream projects to inject custom file systems without modifying Velox
/// core code.
class InputStreamProvider {
 public:
  virtual ~InputStreamProvider() = default;

  /// Creates a ReadFile for the given path.
  /// @param path File path or identifier
  /// @return ReadFile instance for accessing the file
  virtual std::shared_ptr<ReadFile> createReadFile(const std::string& path) = 0;

  /// Creates a default provider that uses the registered file system.
  static std::shared_ptr<InputStreamProvider> createDefault();
};

/// Default implementation that uses velox::filesystems::getFileSystem().
class DefaultInputStreamProvider : public InputStreamProvider {
 public:
  std::shared_ptr<ReadFile> createReadFile(const std::string& path) override;
};

} // namespace facebook::velox::dwio::common

// Made with Bob
