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

#include "velox/dwio/common/InputStreamProvider.h"
#include "velox/common/file/FileSystems.h"

namespace facebook::velox::dwio::common {

std::shared_ptr<ReadFile> DefaultInputStreamProvider::createReadFile(
    const std::string& path) {
  return filesystems::getFileSystem(path, nullptr)->openFileForRead(path);
}

std::shared_ptr<InputStreamProvider> InputStreamProvider::createDefault() {
  return std::make_shared<DefaultInputStreamProvider>();
}

} // namespace facebook::velox::dwio::common

// Made with Bob
