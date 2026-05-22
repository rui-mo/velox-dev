# Gluten Integration Example for Parquet IO Modularization

This document provides a complete example of how Gluten can customize Velox's Parquet reader and IO layer using the new modularization interfaces.

## Overview

The modularization provides two main customization points:

1. **InputStreamProvider**: Customize how ReadFile instances are created for different storage backends
2. **BufferedInputBuilder**: Register custom BufferedInput implementations (existing Velox mechanism)

## Example 1: Custom InputStreamProvider for HDFS

```cpp
// In Gluten codebase: gluten/cpp/velox/io/HdfsInputStreamProvider.h

#pragma once

#include "velox/dwio/common/InputStreamProvider.h"
#include <hdfs.h>

namespace gluten {

/// Custom InputStreamProvider that creates ReadFile instances for HDFS.
class HdfsInputStreamProvider : public velox::dwio::common::InputStreamProvider {
 public:
  explicit HdfsInputStreamProvider(hdfsFS fs) : fs_(fs) {}

  /// Creates a ReadFile for HDFS paths.
  std::shared_ptr<velox::ReadFile> createReadFile(
      const std::string& path,
      const velox::dwio::common::ReaderOptions& options) override {
    // Check if this is an HDFS path
    if (path.find("hdfs://") == 0) {
      return std::make_shared<HdfsReadFile>(fs_, path, options.memoryPool());
    }

    // Fall back to default implementation for non-HDFS paths
    return InputStreamProvider::createReadFile(path, options);
  }

 private:
  hdfsFS fs_;
};

/// Custom ReadFile implementation for HDFS.
class HdfsReadFile : public velox::ReadFile {
 public:
  HdfsReadFile(hdfsFS fs, const std::string& path, velox::memory::MemoryPool* pool)
      : fs_(fs), path_(path), pool_(pool) {
    file_ = hdfsOpenFile(fs_, path_.c_str(), O_RDONLY, 0, 0, 0);
    VELOX_CHECK_NOT_NULL(file_, "Failed to open HDFS file: {}", path_);

    hdfsFileInfo* info = hdfsGetPathInfo(fs_, path_.c_str());
    VELOX_CHECK_NOT_NULL(info, "Failed to get HDFS file info: {}", path_);
    size_ = info->mSize;
    hdfsFreeFileInfo(info, 1);
  }

  ~HdfsReadFile() override {
    if (file_) {
      hdfsCloseFile(fs_, file_);
    }
  }

  std::string_view pread(uint64_t offset, uint64_t length, void* buffer) const override {
    tSize bytesRead = hdfsPread(fs_, file_, offset, buffer, length);
    VELOX_CHECK_GE(bytesRead, 0, "HDFS read failed for file: {}", path_);
    return std::string_view(static_cast<char*>(buffer), bytesRead);
  }

  std::string pread(uint64_t offset, uint64_t length) const override {
    std::string buffer(length, '\0');
    auto view = pread(offset, length, buffer.data());
    buffer.resize(view.size());
    return buffer;
  }

  uint64_t size() const override {
    return size_;
  }

  uint64_t memoryUsage() const override {
    return 0; // HDFS doesn't cache in memory
  }

  bool shouldCoalesce() const override {
    return true; // HDFS benefits from coalescing small reads
  }

  std::string getName() const override {
    return path_;
  }

  uint64_t getNaturalReadSize() const override {
    return 1024 * 1024; // 1MB natural read size for HDFS
  }

 private:
  hdfsFS fs_;
  std::string path_;
  velox::memory::MemoryPool* pool_;
  hdfsFile file_{nullptr};
  uint64_t size_{0};
};

} // namespace gluten
```

## Example 2: Custom BufferedInput for Caching

```cpp
// In Gluten codebase: gluten/cpp/velox/io/CachedBufferedInput.h

#pragma once

#include "velox/dwio/common/BufferedInput.h"
#include "velox/dwio/common/IoStatistics.h"

namespace gluten {

/// Custom BufferedInput that adds caching layer.
class CachedBufferedInput : public velox::dwio::common::BufferedInput {
 public:
  CachedBufferedInput(
      std::shared_ptr<velox::ReadFile> readFile,
      velox::memory::MemoryPool& pool,
      velox::dwio::common::IoStatistics* ioStats,
      std::shared_ptr<Cache> cache)
      : BufferedInput(std::move(readFile), pool, ioStats),
        cache_(std::move(cache)) {}

  std::unique_ptr<velox::dwio::common::SeekableInputStream> read(
      uint64_t offset,
      uint64_t length,
      velox::dwio::common::LogType logType) const override {
    // Check cache first
    auto cached = cache_->get(getReadFile()->getName(), offset, length);
    if (cached) {
      return std::make_unique<velox::dwio::common::SeekableArrayInputStream>(
          cached->data(), cached->size());
    }

    // Cache miss - read from file and cache
    auto stream = BufferedInput::read(offset, length, logType);
    cache_->put(getReadFile()->getName(), offset, length, stream);
    return stream;
  }

 private:
  std::shared_ptr<Cache> cache_;
};

/// Factory for creating CachedBufferedInput instances.
class CachedBufferedInputFactory : public velox::dwio::common::BufferedInputFactory {
 public:
  explicit CachedBufferedInputFactory(std::shared_ptr<Cache> cache)
      : cache_(std::move(cache)) {}

  std::unique_ptr<velox::dwio::common::BufferedInput> create(
      std::shared_ptr<velox::ReadFile> readFile,
      velox::memory::MemoryPool& pool,
      velox::dwio::common::IoStatistics* ioStats,
      const velox::dwio::common::ReaderOptions& options) override {
    return std::make_unique<CachedBufferedInput>(
        std::move(readFile), pool, ioStats, cache_);
  }

 private:
  std::shared_ptr<Cache> cache_;
};

} // namespace gluten
```

## Example 3: Integrating Custom Components in Gluten

```cpp
// In Gluten codebase: gluten/cpp/velox/ParquetReaderBuilder.cpp

#include "velox/dwio/parquet/reader/ParquetReader.h"
#include "velox/dwio/parquet/reader/ParquetReaderFactory.h"
#include "gluten/cpp/velox/io/HdfsInputStreamProvider.h"
#include "gluten/cpp/velox/io/CachedBufferedInput.h"

namespace gluten {

/// Builder for creating customized Parquet readers in Gluten.
class ParquetReaderBuilder {
 public:
  ParquetReaderBuilder() = default;

  /// Sets HDFS filesystem for reading.
  ParquetReaderBuilder& withHdfs(hdfsFS fs) {
    hdfsFs_ = fs;
    return *this;
  }

  /// Enables caching layer.
  ParquetReaderBuilder& withCache(std::shared_ptr<Cache> cache) {
    cache_ = std::move(cache);
    return *this;
  }

  /// Builds a ParquetReader with custom components.
  std::unique_ptr<velox::dwio::common::Reader> build(
      const std::string& path,
      const velox::dwio::common::ReaderOptions& options) {
    // Create factory with custom InputStreamProvider
    auto factory = std::make_shared<velox::dwio::parquet::ParquetReaderFactory>();

    if (hdfsFs_) {
      factory->setInputStreamProvider(
          std::make_shared<HdfsInputStreamProvider>(hdfsFs_));
    }

    // Register custom BufferedInput factory if caching is enabled
    if (cache_) {
      velox::dwio::common::BufferedInputBuilder::registerFactory(
          "cached",
          std::make_unique<CachedBufferedInputFactory>(cache_));
    }

    // Create ReadFile using the custom InputStreamProvider
    auto readFile = factory->getInputStreamProvider()->createReadFile(path, options);

    // Create BufferedInput
    std::unique_ptr<velox::dwio::common::BufferedInput> input;
    if (cache_) {
      // Use cached BufferedInput
      auto cachedFactory = velox::dwio::common::BufferedInputBuilder::getFactory("cached");
      input = cachedFactory->create(
          std::move(readFile),
          options.memoryPool(),
          nullptr, // ioStats
          options);
    } else {
      // Use default BufferedInput
      input = std::make_unique<velox::dwio::common::BufferedInput>(
          std::move(readFile),
          options.memoryPool());
    }

    // Create ParquetReader with custom factory
    return std::make_unique<velox::dwio::parquet::ParquetReader>(
        std::move(input),
        options,
        factory);
  }

 private:
  hdfsFS hdfsFs_{nullptr};
  std::shared_ptr<Cache> cache_;
};

} // namespace gluten
```

## Example 4: Usage in Gluten

```cpp
// In Gluten application code

#include "gluten/cpp/velox/ParquetReaderBuilder.h"

void readParquetFromHdfs() {
  // Initialize HDFS
  hdfsFS fs = hdfsConnect("default", 0);

  // Create cache
  auto cache = std::make_shared<gluten::Cache>(1024 * 1024 * 1024); // 1GB cache

  // Build reader with custom components
  auto reader = gluten::ParquetReaderBuilder()
      .withHdfs(fs)
      .withCache(cache)
      .build("hdfs://namenode:9000/data/file.parquet", options);

  // Use reader as normal
  auto rowReader = reader->createRowReader(rowReaderOptions);
  // ... read data ...
}
```

## Example 5: Custom InputStreamProvider for S3

```cpp
// In Gluten codebase: gluten/cpp/velox/io/S3InputStreamProvider.h

#pragma once

#include "velox/dwio/common/InputStreamProvider.h"
#include <aws/s3/S3Client.h>

namespace gluten {

/// Custom InputStreamProvider for AWS S3.
class S3InputStreamProvider : public velox::dwio::common::InputStreamProvider {
 public:
  explicit S3InputStreamProvider(std::shared_ptr<Aws::S3::S3Client> client)
      : client_(std::move(client)) {}

  std::shared_ptr<velox::ReadFile> createReadFile(
      const std::string& path,
      const velox::dwio::common::ReaderOptions& options) override {
    // Check if this is an S3 path
    if (path.find("s3://") == 0 || path.find("s3a://") == 0) {
      return std::make_shared<S3ReadFile>(client_, path, options.memoryPool());
    }

    // Fall back to default for non-S3 paths
    return InputStreamProvider::createReadFile(path, options);
  }

 private:
  std::shared_ptr<Aws::S3::S3Client> client_;
};

/// Custom ReadFile implementation for S3.
class S3ReadFile : public velox::ReadFile {
 public:
  S3ReadFile(
      std::shared_ptr<Aws::S3::S3Client> client,
      const std::string& path,
      velox::memory::MemoryPool* pool)
      : client_(std::move(client)), path_(path), pool_(pool) {
    // Parse S3 path: s3://bucket/key
    parsePath(path);

    // Get object metadata to determine size
    Aws::S3::Model::HeadObjectRequest request;
    request.SetBucket(bucket_);
    request.SetKey(key_);

    auto outcome = client_->HeadObject(request);
    VELOX_CHECK(outcome.IsSuccess(), "Failed to get S3 object metadata: {}", path_);
    size_ = outcome.GetResult().GetContentLength();
  }

  std::string_view pread(uint64_t offset, uint64_t length, void* buffer) const override {
    Aws::S3::Model::GetObjectRequest request;
    request.SetBucket(bucket_);
    request.SetKey(key_);
    request.SetRange(fmt::format("bytes={}-{}", offset, offset + length - 1));

    auto outcome = client_->GetObject(request);
    VELOX_CHECK(outcome.IsSuccess(), "Failed to read from S3: {}", path_);

    auto& stream = outcome.GetResult().GetBody();
    stream.read(static_cast<char*>(buffer), length);
    auto bytesRead = stream.gcount();

    return std::string_view(static_cast<char*>(buffer), bytesRead);
  }

  std::string pread(uint64_t offset, uint64_t length) const override {
    std::string buffer(length, '\0');
    auto view = pread(offset, length, buffer.data());
    buffer.resize(view.size());
    return buffer;
  }

  uint64_t size() const override {
    return size_;
  }

  uint64_t memoryUsage() const override {
    return 0;
  }

  bool shouldCoalesce() const override {
    return true; // S3 benefits from coalescing
  }

  std::string getName() const override {
    return path_;
  }

  uint64_t getNaturalReadSize() const override {
    return 8 * 1024 * 1024; // 8MB for S3
  }

 private:
  void parsePath(const std::string& path) {
    // Remove s3:// or s3a:// prefix
    size_t start = path.find("://") + 3;
    size_t slashPos = path.find('/', start);

    bucket_ = path.substr(start, slashPos - start);
    key_ = path.substr(slashPos + 1);
  }

  std::shared_ptr<Aws::S3::S3Client> client_;
  std::string path_;
  velox::memory::MemoryPool* pool_;
  std::string bucket_;
  std::string key_;
  uint64_t size_{0};
};

} // namespace gluten
```

## Key Benefits for Gluten

1. **No Velox Code Modification**: All customization happens in Gluten codebase
2. **Clean Separation**: Storage backend logic stays in Gluten
3. **Composable**: Mix and match different providers and buffered inputs
4. **Backward Compatible**: Existing Velox code continues to work unchanged
5. **Testable**: Each component can be tested independently

## Testing Custom Components

```cpp
// In Gluten test code

TEST(HdfsInputStreamProviderTest, readsFromHdfs) {
  auto fs = hdfsConnect("default", 0);
  auto provider = std::make_shared<gluten::HdfsInputStreamProvider>(fs);

  velox::dwio::common::ReaderOptions options;
  auto readFile = provider->createReadFile("hdfs://test/file.parquet", options);

  EXPECT_GT(readFile->size(), 0);
  EXPECT_EQ(readFile->getName(), "hdfs://test/file.parquet");
}

TEST(CachedBufferedInputTest, cachesReads) {
  auto cache = std::make_shared<gluten::Cache>(1024 * 1024);
  auto factory = std::make_unique<gluten::CachedBufferedInputFactory>(cache);

  // Register factory
  velox::dwio::common::BufferedInputBuilder::registerFactory("test", std::move(factory));

  // Create buffered input
  auto readFile = createTestReadFile();
  auto input = velox::dwio::common::BufferedInputBuilder::getFactory("test")->create(
      std::move(readFile), pool, nullptr, options);

  // First read - cache miss
  auto stream1 = input->read(0, 1024, velox::dwio::common::LogType::FILE);
  EXPECT_EQ(cache->missCount(), 1);

  // Second read - cache hit
  auto stream2 = input->read(0, 1024, velox::dwio::common::LogType::FILE);
  EXPECT_EQ(cache->hitCount(), 1);
}
```

## Migration Path

For existing Gluten code using Velox Parquet reader:

### Before (Direct Velox Usage)
```cpp
auto reader = velox::dwio::parquet::ParquetReader::create(
    std::move(input), options);
```

### After (With Customization)
```cpp
auto factory = std::make_shared<velox::dwio::parquet::ParquetReaderFactory>();
factory->setInputStreamProvider(std::make_shared<gluten::HdfsInputStreamProvider>(fs));

auto reader = std::make_unique<velox::dwio::parquet::ParquetReader>(
    std::move(input), options, factory);
```

Both approaches work - the new API is opt-in.
