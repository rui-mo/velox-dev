# Parquet Reader Customization Guide for Gluten

## Overview

This guide explains how to customize Velox's Parquet reader for use in Gluten by leveraging existing extension points in the architecture. Velox already provides flexible base classes that can be extended without modifying core code.

## Current Architecture

Velox's Parquet reader has a well-designed layered architecture with clear extension points:

```
┌─────────────────────────────────────────────────────────┐
│                  ParquetReader                           │
│            (Public API, uses ReaderBase)                │
└────────────────┬────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────┐
│                  ReaderBase                              │
│         (Core logic, uses BufferedInput)                │
└────────────────┬────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────┐
│              BufferedInput (abstract)                    │
│  ┌──────────────────┬──────────────────────────┐       │
│  │                  │                           │       │
│  ▼                  ▼                           ▼       │
│  CachedBufferedInput DirectBufferedInput  CustomImpl   │
│  (with cache)       (direct IO)          (your code)   │
└─────────────────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────┐
│            ReadFileInputStream                           │
│              (wraps ReadFile)                           │
└────────────────┬────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────┐
│                 ReadFile                                 │
│          (storage abstraction)                          │
└─────────────────────────────────────────────────────────┘
```

## Extension Points for Gluten

### 1. Custom ReadFile Implementation

**When to use**: Need to integrate with Gluten's storage backend (HDFS, S3, etc.)

**How**: Implement the `ReadFile` interface

```cpp
// GlutenReadFile.h
#pragma once
#include "velox/common/file/File.h"

namespace gluten {

class GlutenReadFile : public facebook::velox::ReadFile {
 public:
  explicit GlutenReadFile(std::string path, YourStorageClient* client)
      : path_(std::move(path)), client_(client) {}

  std::string_view pread(
      uint64_t offset,
      uint64_t length,
      void* buffer) const override {
    // Read from your storage backend
    client_->read(path_, offset, length, buffer);
    return std::string_view(static_cast<char*>(buffer), length);
  }

  uint64_t preadv(
      uint64_t offset,
      const std::vector<folly::Range<char*>>& buffers) const override {
    // Implement vectorized read if your storage supports it
    return client_->vectorizedRead(path_, offset, buffers);
  }

  uint64_t size() const override {
    return client_->getFileSize(path_);
  }

  uint64_t memoryUsage() const override {
    return 0; // No memory held by this object
  }

  bool shouldCoalesce() const override {
    return true; // Enable read coalescing for remote storage
  }

  std::string getName() const override {
    return path_;
  }

  uint64_t getNaturalReadSize() const override {
    return 8 * 1024 * 1024; // 8MB for remote storage
  }

 private:
  std::string path_;
  YourStorageClient* client_;
};

} // namespace gluten
```

### 2. Custom BufferedInput Implementation

**When to use**: Need custom buffering strategy, prefetching, or caching logic

**Option A: Extend CachedBufferedInput** (recommended if you have a cache)

```cpp
// GlutenCachedBufferedInput.h
#pragma once
#include "velox/dwio/common/CachedBufferedInput.h"

namespace gluten {

class GlutenCachedBufferedInput
    : public facebook::velox::dwio::common::CachedBufferedInput {
 public:
  using CachedBufferedInput::CachedBufferedInput;

  // Override to customize prefetching behavior
  bool shouldPreload(int32_t numPages) override {
    // More aggressive prefetching for Gluten
    return numPages < 2000; // Prefetch up to 2000 pages
  }

  // Override to customize when to prefetch stripe metadata
  bool shouldPrefetchStripes() const override {
    return true; // Always prefetch for Gluten
  }

  // Override clone to return your custom type
  std::unique_ptr<facebook::velox::dwio::common::BufferedInput> clone()
      const override {
    return std::make_unique<GlutenCachedBufferedInput>(
        input_, fileNum_, cache_, tracker_, groupId_,
        ioStatistics_, ioStats_, executor_, options_);
  }
};

} // namespace gluten
```

**Option B: Extend DirectBufferedInput** (if no cache needed)

```cpp
// GlutenDirectBufferedInput.h
#pragma once
#include "velox/dwio/common/DirectBufferedInput.h"

namespace gluten {

class GlutenDirectBufferedInput
    : public facebook::velox::dwio::common::DirectBufferedInput {
 public:
  using DirectBufferedInput::DirectBufferedInput;

  // Customize prefetching logic
  bool shouldPreload(int32_t numPages) override {
    // Custom logic for Gluten
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
```

### 3. Custom Column Reader (Advanced)

**When to use**: Need specialized decoding or filtering logic

```cpp
// GlutenStringColumnReader.h
#pragma once
#include "velox/dwio/parquet/reader/StringColumnReader.h"

namespace gluten {

class GlutenStringColumnReader
    : public facebook::velox::parquet::StringColumnReader {
 public:
  using StringColumnReader::StringColumnReader;

  void read(
      facebook::velox::vector_size_t offset,
      facebook::velox::dwio::common::RowSet rows,
      const uint64_t* incomingNulls) override {
    // Custom string reading logic with Gluten-specific optimizations
    // For example: custom dictionary handling, compression, etc.

    // Call base implementation or implement from scratch
    StringColumnReader::read(offset, rows, incomingNulls);
  }
};

} // namespace gluten
```

## Complete Integration Example

### Step 1: Create Gluten-Specific Reader Factory

```cpp
// GlutenParquetReaderFactory.h
#pragma once
#include "velox/dwio/parquet/reader/ParquetReader.h"
#include "GlutenReadFile.h"
#include "GlutenCachedBufferedInput.h"

namespace gluten {

class GlutenParquetReaderFactory {
 public:
  static std::unique_ptr<facebook::velox::dwio::common::Reader>
  createReader(
      const std::string& path,
      YourStorageClient* storageClient,
      facebook::velox::memory::MemoryPool& pool,
      facebook::velox::cache::AsyncDataCache* cache,
      folly::Executor* executor) {

    // Create custom ReadFile
    auto readFile = std::make_shared<GlutenReadFile>(path, storageClient);

    // Create reader options
    facebook::velox::io::ReaderOptions readerOptions{&pool};
    readerOptions.setLoadQuantum(8 * 1024 * 1024); // 8MB
    readerOptions.setMaxCoalesceDistance(512 * 1024); // 512KB

    // Create custom BufferedInput
    std::unique_ptr<facebook::velox::dwio::common::BufferedInput> bufferedInput;

    if (cache != nullptr) {
      // Use cached version
      auto fileNum = cache->fileIds().getNextId();
      auto groupId = cache->fileIds().getNextId();
      auto tracker = std::make_shared<facebook::velox::cache::ScanTracker>(
          "GlutenScan", nullptr, 100);

      bufferedInput = std::make_unique<GlutenCachedBufferedInput>(
          readFile,
          facebook::velox::dwio::common::MetricsLog::voidLog(),
          fileNum,
          cache,
          tracker,
          groupId,
          std::make_shared<facebook::velox::dwio::common::IoStatistics>(),
          nullptr, // ioStats
          executor,
          readerOptions);
    } else {
      // Use direct IO version
      auto fileNum = facebook::velox::cache::StringIdLease(
          facebook::velox::cache::fileIds(), path);
      auto groupId = facebook::velox::cache::StringIdLease(
          facebook::velox::cache::fileIds(), "gluten");
      auto tracker = std::make_shared<facebook::velox::cache::ScanTracker>(
          "GlutenScan", nullptr, 100);

      bufferedInput = std::make_unique<GlutenDirectBufferedInput>(
          readFile,
          facebook::velox::dwio::common::MetricsLog::voidLog(),
          fileNum,
          tracker,
          groupId,
          std::make_shared<facebook::velox::dwio::common::IoStatistics>(),
          nullptr, // ioStats
          executor,
          readerOptions);
    }

    // Create Parquet reader with custom BufferedInput
    facebook::velox::dwio::common::ReaderOptions parquetReaderOptions{&pool};

    return std::make_unique<facebook::velox::parquet::ParquetReader>(
        std::move(bufferedInput), parquetReaderOptions);
  }
};

} // namespace gluten
```

### Step 2: Use in Gluten Query Execution

```cpp
// In your Gluten query execution code
void executeParquetScan(
    const std::string& filePath,
    YourStorageClient* storageClient,
    facebook::velox::memory::MemoryPool& pool) {

  // Create reader using Gluten factory
  auto reader = gluten::GlutenParquetReaderFactory::createReader(
      filePath,
      storageClient,
      pool,
      yourCacheInstance, // or nullptr for no cache
      yourExecutor);

  // Create row reader with scan spec
  facebook::velox::dwio::common::RowReaderOptions rowReaderOptions;

  auto scanSpec = std::make_shared<facebook::velox::common::ScanSpec>("root");
  scanSpec->addField("column1", 0);
  scanSpec->addField("column2", 1);
  // Add filters if needed
  rowReaderOptions.setScanSpec(scanSpec);

  auto rowReader = reader->createRowReader(rowReaderOptions);

  // Read data
  facebook::velox::VectorPtr batch;
  while (true) {
    auto rowsRead = rowReader->next(10000, batch);
    if (rowsRead == 0) {
      break;
    }

    // Process batch in Gluten
    processGlutenBatch(batch, rowsRead);
  }
}
```

## Key Benefits of This Approach

1. **No Velox Core Modifications**: All customization through inheritance
2. **Leverages Existing Infrastructure**: Uses proven CachedBufferedInput/DirectBufferedInput
3. **Clean Separation**: Gluten code stays in Gluten repository
4. **Easy Maintenance**: Velox updates don't break Gluten customizations
5. **Performance**: Zero overhead when using default implementations

## Testing Your Implementation

```cpp
// Test custom ReadFile
TEST(GlutenParquetTest, CustomReadFile) {
  auto client = std::make_shared<MockStorageClient>();
  auto readFile = std::make_shared<GlutenReadFile>("test.parquet", client.get());

  EXPECT_EQ(readFile->size(), expectedSize);
  EXPECT_EQ(readFile->getNaturalReadSize(), 8 * 1024 * 1024);
}

// Test custom BufferedInput
TEST(GlutenParquetTest, CustomBufferedInput) {
  auto bufferedInput = createGlutenBufferedInput(...);

  EXPECT_TRUE(bufferedInput->shouldPreload(1000));
  EXPECT_TRUE(bufferedInput->shouldPrefetchStripes());
}

// Test end-to-end reading
TEST(GlutenParquetTest, EndToEndRead) {
  auto reader = GlutenParquetReaderFactory::createReader(...);
  auto rowReader = reader->createRowReader(...);

  facebook::velox::VectorPtr batch;
  auto rowsRead = rowReader->next(1000, batch);

  EXPECT_GT(rowsRead, 0);
  EXPECT_NE(batch, nullptr);
}
```

## Performance Tuning

### For Remote Storage (HDFS, S3)
- Use `CachedBufferedInput` with AsyncDataCache
- Set larger `loadQuantum` (8-16MB)
- Enable aggressive prefetching
- Use vectorized reads (preadv)

### For Local Storage
- Use `DirectBufferedInput`
- Smaller `loadQuantum` (1-4MB)
- Less aggressive prefetching
- Standard read operations

### Memory Management
- Monitor cache hit rates
- Adjust `maxCoalesceDistance` based on access patterns
- Use `shouldPreload()` to control prefetch memory usage

## Common Patterns

### Pattern 1: Async Prefetching
```cpp
bool GlutenCachedBufferedInput::shouldPreload(int32_t numPages) {
  // Check available memory before prefetching
  auto availableMemory = pool_->availableReservation();
  auto requiredMemory = numPages * pageSize;
  return availableMemory > requiredMemory * 2; // 2x safety margin
}
```

### Pattern 2: Adaptive Coalescing
```cpp
class GlutenReadFile : public ReadFile {
  bool shouldCoalesce() const override {
    // Coalesce more aggressively for remote storage
    return isRemoteStorage_;
  }

  uint64_t getNaturalReadSize() const override {
    return isRemoteStorage_ ? 8 * 1024 * 1024 : 1 * 1024 * 1024;
  }
};
```

### Pattern 3: Custom Metrics Collection
```cpp
class GlutenCachedBufferedInput : public CachedBufferedInput {
  void load(LogType logType) override {
    auto startTime = std::chrono::steady_clock::now();

    CachedBufferedInput::load(logType);

    auto duration = std::chrono::steady_clock::now() - startTime;
    glutenMetrics_->recordLoadTime(duration);
  }
};
```

## Migration Checklist

- [ ] Implement `GlutenReadFile` for your storage backend
- [ ] Choose between `CachedBufferedInput` or `DirectBufferedInput`
- [ ] Extend chosen BufferedInput class with Gluten customizations
- [ ] Create `GlutenParquetReaderFactory` for easy reader creation
- [ ] Add unit tests for custom components
- [ ] Add integration tests for end-to-end reading
- [ ] Performance benchmark against baseline
- [ ] Update Gluten documentation
- [ ] Remove any Velox patches from Gluten codebase

## References

- Velox BufferedInput: `velox/dwio/common/BufferedInput.h`
- CachedBufferedInput: `velox/dwio/common/CachedBufferedInput.h`
- DirectBufferedInput: `velox/dwio/common/DirectBufferedInput.h`
- ReadFile Interface: `velox/common/file/File.h`
- ParquetReader: `velox/dwio/parquet/reader/ParquetReader.h`
