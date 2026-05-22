# Parquet Reader Modularization for Gluten Integration

## Overview

This document describes the modularization of Velox's Parquet reader to enable easy customization in downstream projects like Gluten. The design introduces the `InputStreamProvider` interface for custom storage backends while leveraging Velox's existing `BufferedInputBuilder` mechanism for custom buffering strategies.

## Architecture

### Layered Design

```
┌─────────────────────────────────────────────────────────┐
│              ParquetReader (Public API)                  │
│  - Accepts optional ParquetReaderFactory                │
└────────────────┬────────────────────────────────────────┘
                 │ uses
                 ▼
┌─────────────────────────────────────────────────────────┐
│            ParquetReaderFactory                          │
│  - InputStreamProvider (creates ReadFile)               │
└────────────────┬────────────────────────────────────────┘
                 │ creates
                 ▼
┌─────────────────────────────────────────────────────────┐
│              ReadFile                                    │
│  (Created by InputStreamProvider)                       │
└────────────────┬────────────────────────────────────────┘
                 │ used by
                 ▼
┌─────────────────────────────────────────────────────────┐
│              BufferedInput                               │
│  (Created via BufferedInputBuilder - existing mechanism)│
│  - CachedBufferedInput                                  │
│  - DirectBufferedInput                                  │
│  - Custom implementations (registered via Builder)      │
└─────────────────────────────────────────────────────────┘
```

## Extension Points

### 1. InputStreamProvider (New)

**Purpose**: Abstracts ReadFile creation for different storage backends.

**Interface**: `velox/dwio/common/InputStreamProvider.h`

```cpp
class InputStreamProvider {
 public:
  virtual ~InputStreamProvider() = default;

  /// Creates a ReadFile for the given path.
  virtual std::shared_ptr<ReadFile> createReadFile(
      const std::string& path) = 0;

  static std::shared_ptr<InputStreamProvider> createDefault();
};
```

**When to customize**:
- Integrating with custom storage backends (HDFS, S3, custom file systems)
- Need special file access patterns or caching at the ReadFile level
- Want to inject custom ReadFile implementations

### 2. BufferedInputBuilder (Existing)

**Purpose**: Registers custom BufferedInput implementations.

**Mechanism**: Velox already has a `BufferedInputBuilder` that allows registering custom BufferedInput creators. Gluten should use this existing mechanism instead of a new factory.

**When to customize**:
- Need custom buffering strategies
- Want specialized prefetching logic
- Require custom memory management
- Need to extend CachedBufferedInput or DirectBufferedInput

**How to use**: Register your custom BufferedInput creator with the BufferedInputBuilder (see implementation examples below).

### 3. ParquetReaderFactory (New)

**Purpose**: Holds customizable InputStreamProvider for Parquet reader creation.

**Interface**: `velox/dwio/parquet/reader/ParquetReaderFactory.h`

```cpp
struct ParquetReaderFactory {
  std::shared_ptr<dwio::common::InputStreamProvider> inputStreamProvider;

  static std::shared_ptr<ParquetReaderFactory> createDefault();
  static std::shared_ptr<ParquetReaderFactory> create(
      std::shared_ptr<dwio::common::InputStreamProvider> inputStreamProvider);
};
```

## Implementation Guide for Gluten

### Step 1: Implement Custom InputStreamProvider

```cpp
// GlutenInputStreamProvider.h
#pragma once
#include "velox/dwio/common/InputStreamProvider.h"
#include "your/gluten/storage/Client.h"

namespace gluten {

class GlutenInputStreamProvider
    : public facebook::velox::dwio::common::InputStreamProvider {
 public:
  explicit GlutenInputStreamProvider(
      std::shared_ptr<YourStorageClient> client)
      : client_(std::move(client)) {}

  std::shared_ptr<facebook::velox::ReadFile> createReadFile(
      const std::string& path) override {
    // Create your custom ReadFile implementation
    return std::make_shared<GlutenReadFile>(path, client_);
  }

 private:
  std::shared_ptr<YourStorageClient> client_;
};

} // namespace gluten
```

### Step 2: Implement Custom ReadFile

```cpp
// GlutenReadFile.h
#pragma once
#include "velox/common/file/File.h"

namespace gluten {

class GlutenReadFile : public facebook::velox::ReadFile {
 public:
  GlutenReadFile(std::string path, std::shared_ptr<YourStorageClient> client)
      : path_(std::move(path)), client_(std::move(client)) {}

  std::string_view pread(
      uint64_t offset,
      uint64_t length,
      void* buffer) const override {
    client_->read(path_, offset, length, buffer);
    return std::string_view(static_cast<char*>(buffer), length);
  }

  uint64_t preadv(
      uint64_t offset,
      const std::vector<folly::Range<char*>>& buffers) const override {
    return client_->vectorizedRead(path_, offset, buffers);
  }

  uint64_t size() const override {
    return client_->getFileSize(path_);
  }

  uint64_t memoryUsage() const override { return 0; }

  bool shouldCoalesce() const override { return true; }

  std::string getName() const override { return path_; }

  uint64_t getNaturalReadSize() const override {
    return 8 * 1024 * 1024; // 8MB for remote storage
  }

 private:
  std::string path_;
  std::shared_ptr<YourStorageClient> client_;
};

} // namespace gluten
```

### Step 3: Register Custom BufferedInput (Using Existing Builder)

```cpp
// GlutenBufferedInput.h
#pragma once
#include "velox/dwio/common/CachedBufferedInput.h"
#include "velox/dwio/common/DirectBufferedInput.h"

namespace gluten {

/// Custom BufferedInput extending CachedBufferedInput
class GlutenCachedBufferedInput
    : public facebook::velox::dwio::common::CachedBufferedInput {
 public:
  using CachedBufferedInput::CachedBufferedInput;

  // Override to customize prefetching
  bool shouldPreload(int32_t numPages) override {
    return numPages < 2000; // More aggressive for Gluten
  }

  bool shouldPrefetchStripes() const override {
    return true;
  }

  std::unique_ptr<facebook::velox::dwio::common::BufferedInput> clone()
      const override {
    return std::make_unique<GlutenCachedBufferedInput>(
        input_, fileNum_, cache_, tracker_, groupId_,
        ioStatistics_, ioStats_, executor_, options_);
  }
};

/// Custom BufferedInput extending DirectBufferedInput
class GlutenDirectBufferedInput
    : public facebook::velox::dwio::common::DirectBufferedInput {
 public:
  using DirectBufferedInput::DirectBufferedInput;

  bool shouldPreload(int32_t numPages) override {
    return numPages < 1000;
  }

  std::unique_ptr<facebook::velox::dwio::common::BufferedInput> clone()
      const override {
    return std::make_unique<GlutenDirectBufferedInput>(
        input_, fileNum_, tracker_, groupId_,
        ioStatistics_, ioStats_, executor_, options_);
  }
};

} // namespace gluten

// GlutenBufferedInputRegistration.cpp
#include "GlutenBufferedInput.h"
// Include the BufferedInputBuilder header (find the actual location in Velox)
// #include "velox/dwio/common/BufferedInputBuilder.h"

namespace gluten {

// Register Gluten's custom BufferedInput creators with Velox's builder
void registerGlutenBufferedInput() {
  // Use Velox's existing BufferedInputBuilder to register custom creators
  // The exact API depends on BufferedInputBuilder's implementation
  // Example (adjust based on actual API):
  //
  // BufferedInputBuilder::registerCreator(
  //     "gluten_cached",
  //     [](auto readFile, auto metricsLog, auto fileNum, auto cache, ...) {
  //       return std::make_unique<GlutenCachedBufferedInput>(...);
  //     });
  //
  // BufferedInputBuilder::registerCreator(
  //     "gluten_direct",
  //     [](auto readFile, auto metricsLog, auto fileNum, ...) {
  //       return std::make_unique<GlutenDirectBufferedInput>(...);
  //     });
}

} // namespace gluten
```

### Step 4: Create Parquet Reader with Custom Factory

```cpp
// GlutenParquetReader.cpp
#include "velox/dwio/parquet/reader/ParquetReader.h"
#include "velox/dwio/parquet/reader/ParquetReaderFactory.h"
#include "GlutenInputStreamProvider.h"

namespace gluten {

std::unique_ptr<facebook::velox::dwio::common::Reader>
createGlutenParquetReader(
    const std::string& path,
    std::shared_ptr<YourStorageClient> storageClient,
    facebook::velox::memory::MemoryPool& pool,
    facebook::velox::cache::AsyncDataCache* cache,
    folly::Executor* executor) {

  // Create custom factory with Gluten's InputStreamProvider
  auto factory = facebook::velox::parquet::ParquetReaderFactory::create(
      std::make_shared<GlutenInputStreamProvider>(storageClient));

  // Create ReadFile using custom provider
  auto readFile = factory->inputStreamProvider->createReadFile(path);

  // Setup reader options
  facebook::velox::io::ReaderOptions readerOptions{&pool};
  readerOptions.setLoadQuantum(8 * 1024 * 1024);
  readerOptions.setMaxCoalesceDistance(512 * 1024);

  // Create BufferedInput using Velox's existing builder mechanism
  // The builder will use Gluten's registered custom BufferedInput if configured
  // Otherwise it will use default CachedBufferedInput or DirectBufferedInput
  auto fileNum = cache ? cache->fileIds().getNextId()
                       : facebook::velox::cache::StringIdLease(
                           facebook::velox::cache::fileIds(), path);
  auto groupId = facebook::velox::cache::StringIdLease(
      facebook::velox::cache::fileIds(), "gluten");
  auto tracker = std::make_shared<facebook::velox::cache::ScanTracker>(
      "GlutenScan", nullptr, 100);

  std::unique_ptr<facebook::velox::dwio::common::BufferedInput> bufferedInput;

  if (cache != nullptr) {
    // Use Gluten's custom CachedBufferedInput
    bufferedInput = std::make_unique<GlutenCachedBufferedInput>(
        readFile,
        facebook::velox::dwio::common::MetricsLog::voidLog(),
        fileNum,
        cache,
        tracker,
        groupId,
        std::make_shared<facebook::velox::dwio::common::IoStatistics>(),
        nullptr,
        executor,
        readerOptions);
  } else {
    // Use Gluten's custom DirectBufferedInput
    bufferedInput = std::make_unique<GlutenDirectBufferedInput>(
        readFile,
        facebook::velox::dwio::common::MetricsLog::voidLog(),
        fileNum,
        tracker,
        groupId,
        std::make_shared<facebook::velox::dwio::common::IoStatistics>(),
        nullptr,
        executor,
        readerOptions);
  }

  // Create ParquetReader with custom factory
  facebook::velox::dwio::common::ReaderOptions parquetReaderOptions{&pool};

  return std::make_unique<facebook::velox::parquet::ParquetReader>(
      std::move(bufferedInput), parquetReaderOptions, factory);
}

} // namespace gluten
```

### Step 5: Use in Query Execution

```cpp
void executeGlutenQuery(
    const std::string& filePath,
    std::shared_ptr<YourStorageClient> storageClient,
    facebook::velox::memory::MemoryPool& pool) {

  // Register Gluten's custom BufferedInput (do this once at startup)
  // gluten::registerGlutenBufferedInput();

  // Create reader with Gluten customizations
  auto reader = gluten::createGlutenParquetReader(
      filePath, storageClient, pool, yourCache, yourExecutor);

  // Create row reader with scan spec
  facebook::velox::dwio::common::RowReaderOptions rowReaderOptions;
  auto scanSpec = std::make_shared<facebook::velox::common::ScanSpec>("root");
  scanSpec->addField("column1", 0);
  scanSpec->addField("column2", 1);
  rowReaderOptions.setScanSpec(scanSpec);

  auto rowReader = reader->createRowReader(rowReaderOptions);

  // Read and process data
  facebook::velox::VectorPtr batch;
  while (true) {
    auto rowsRead = rowReader->next(10000, batch);
    if (rowsRead == 0) break;

    // Process batch
    processGlutenBatch(batch, rowsRead);
  }
}
```

## Key Design Decisions

### Why Not BufferedInputFactory?

Velox already has a `BufferedInputBuilder` mechanism that allows registering custom BufferedInput creators. Adding a new factory would be redundant. Instead:

1. **For InputStreamProvider**: New interface needed because there was no existing mechanism to customize ReadFile creation
2. **For BufferedInput**: Use existing `BufferedInputBuilder` registration mechanism
3. **Result**: Clean separation - InputStreamProvider for storage, BufferedInputBuilder for buffering

### Backward Compatibility

All existing code continues to work without changes:

```cpp
// Existing code - still works
auto reader = std::make_unique<ParquetReader>(
    std::move(bufferedInput), options);
```

The new constructor with `ParquetReaderFactory` is optional and only used when customization is needed.

## Benefits

1. **Minimal New Interfaces**: Only adds InputStreamProvider, reuses existing BufferedInputBuilder
2. **No Velox Core Modifications**: All customization through interfaces
3. **Clean Separation**: Gluten code stays in Gluten repository
4. **Easy Maintenance**: Velox updates don't break Gluten
5. **Flexible**: Mix default and custom implementations
6. **Zero Overhead**: Default path has no performance penalty

## Testing

```cpp
TEST(GlutenParquetTest, CustomInputStreamProvider) {
  auto client = std::make_shared<MockStorageClient>();
  auto provider = std::make_shared<GlutenInputStreamProvider>(client);

  auto readFile = provider->createReadFile("test.parquet");
  EXPECT_NE(readFile, nullptr);
  EXPECT_EQ(readFile->getName(), "test.parquet");
}

TEST(GlutenParquetTest, CustomBufferedInput) {
  auto bufferedInput = std::make_unique<GlutenCachedBufferedInput>(...);

  EXPECT_TRUE(bufferedInput->shouldPreload(1000));
  EXPECT_TRUE(bufferedInput->shouldPrefetchStripes());
}

TEST(GlutenParquetTest, EndToEndRead) {
  auto reader = createGlutenParquetReader(...);
  auto rowReader = reader->createRowReader(...);

  facebook::velox::VectorPtr batch;
  auto rowsRead = rowReader->next(1000, batch);

  EXPECT_GT(rowsRead, 0);
  EXPECT_NE(batch, nullptr);
}
```

## Migration Checklist

- [ ] Implement `GlutenInputStreamProvider`
- [ ] Implement `GlutenReadFile`
- [ ] Extend `CachedBufferedInput` or `DirectBufferedInput` for custom buffering
- [ ] Register custom BufferedInput with BufferedInputBuilder (if needed)
- [ ] Create `createGlutenParquetReader()` helper function
- [ ] Add unit tests for custom components
- [ ] Add integration tests
- [ ] Performance benchmark
- [ ] Remove Velox patches from Gluten

## References

- InputStreamProvider: `velox/dwio/common/InputStreamProvider.h`
- ParquetReaderFactory: `velox/dwio/parquet/reader/ParquetReaderFactory.h`
- ParquetReader: `velox/dwio/parquet/reader/ParquetReader.h`
- ReadFile: `velox/common/file/File.h`
- CachedBufferedInput: `velox/dwio/common/CachedBufferedInput.h`
- DirectBufferedInput: `velox/dwio/common/DirectBufferedInput.h`
- BufferedInputBuilder: (Find in Velox codebase - existing mechanism)
