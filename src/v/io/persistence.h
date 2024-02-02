/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include <seastar/core/file.hh>
#include <seastar/core/future.hh>

#include <absl/container/btree_map.h>
#include <absl/container/flat_hash_map.h>

#include <filesystem>

namespace experimental::io {

/**
 * An abstract persistence interface.
 *
 * This interface is used to provide storage access to the io subsystem.
 *
 * Rationale
 * =========
 *
 * Forcing file access to pass through a dedicated interface makes it easy to
 * audit the set of low-level io primitives used in redpanda. Additionally,
 * implementations can be constructed that are useful in testing, such offering
 * functionality like fault injection and fuzzing.
 */
class persistence {
public:
    persistence() = default;
    persistence(const persistence&) = delete;
    persistence(persistence&&) = delete;
    persistence& operator=(const persistence&) = delete;
    persistence& operator=(persistence&&) = delete;
    virtual ~persistence() = default;

    /**
     * An abstract file interface.
     */
    class file {
    public:
        file() = default;
        file(const file&) = delete;
        file(file&&) = delete;
        file& operator=(const file&) = delete;
        file& operator=(file&&) = delete;
        virtual ~file() = default;

        /**
         * Read \p size bytes at \p offset into \p buf.
         */
        virtual seastar::future<size_t>
        dma_read(uint64_t offset, char* buf, size_t) noexcept = 0;

        /**
         * Write \p size bytes from \p buf to \p offset.
         */
        virtual seastar::future<size_t>
        dma_write(uint64_t offset, const char* buf, size_t size) noexcept = 0;

        /**
         * Close the file.
         */
        virtual seastar::future<> close() noexcept = 0;

        /**
         * Required alignment for offsets being read.
         */
        [[nodiscard]] virtual uint64_t disk_read_dma_alignment() const noexcept
          = 0;

        /**
         * Required alignment for offsets being written.
         */
        [[nodiscard]] virtual uint64_t disk_write_dma_alignment() const noexcept
          = 0;

        /**
         * Required alignment for I/O memory.
         */
        [[nodiscard]] virtual uint64_t memory_dma_alignment() const noexcept
          = 0;

        /**
         * Arrange for the next invocation of \ref read to return an exceptional
         * future containing \p eptr.
         */
        void fail_next_read(std::exception_ptr eptr) noexcept;

        /**
         * Arrange for the next invocation of \ref read to return an exceptional
         * future containing \p exception.
         */
        template<typename T>
        void fail_next_read(T&& exception) noexcept {
            fail_next_read(std::make_exception_ptr(std::forward<T>(exception)));
        }

        /**
         * Arrange for the next invocation of \ref write to return an
         * exceptional future containing \p eptr.
         */
        void fail_next_write(std::exception_ptr eptr) noexcept;

        /**
         * Arrange for the next invocation of \ref write to return an
         * exceptional future containing \p exception.
         */
        template<typename T>
        void fail_next_write(T&& exception) noexcept {
            fail_next_write(
              std::make_exception_ptr(std::forward<T>(exception)));
        }

        /**
         * Arrange for the next invocation of \ref close to return an
         * exceptional future containing \p eptr.
         */
        void fail_next_close(std::exception_ptr eptr) noexcept;

        /**
         * Arrange for the next invocation of \ref close to return an
         * exceptional future containing \p exception.
         */
        template<typename T>
        void fail_next_close(T&& exception) noexcept {
            fail_next_close(
              std::make_exception_ptr(std::forward<T>(exception)));
        }

    protected:
        seastar::future<> maybe_fail_read();
        seastar::future<> maybe_fail_write();
        seastar::future<> maybe_fail_close();

    private:
        std::exception_ptr read_ex_;
        std::exception_ptr write_ex_;
        std::exception_ptr close_ex_;
    };

    /**
     * Allocate at least \p size bytes with \p alignment.
     */
    virtual seastar::temporary_buffer<char>
    allocate(uint64_t alignment, size_t size) noexcept = 0;

    /**
     * Create a new file at the specified \p path.
     */
    virtual seastar::future<seastar::shared_ptr<file>>
      create(std::filesystem::path) noexcept = 0;

    /**
     * Open the file at the specified \p path.
     */
    virtual seastar::future<seastar::shared_ptr<file>>
      open(std::filesystem::path) noexcept = 0;

    /**
     * Arrange for the next invocation of \ref create to return an exceptional
     * future containing \p eptr.
     */
    void fail_next_create(std::exception_ptr eptr) noexcept;

    /**
     * Arrange for the next invocation of \ref create to return an exceptional
     * future containing \p exception.
     */
    template<typename T>
    void fail_next_create(T&& exception) noexcept {
        fail_next_create(std::make_exception_ptr(std::forward<T>(exception)));
    }

    /**
     * Arrange for the next invocation of \ref open to return an exceptional
     * future containing \p eptr.
     */
    void fail_next_open(std::exception_ptr eptr) noexcept;

    /**
     * Arrange for the next invocation of \ref open to return an exceptional
     * future containing \p exception.
     */
    template<typename T>
    void fail_next_open(T&& exception) noexcept {
        fail_next_open(std::make_exception_ptr(std::forward<T>(exception)));
    }

protected:
    seastar::future<> maybe_fail_create();
    seastar::future<> maybe_fail_open();

private:
    std::exception_ptr create_ex_;
    std::exception_ptr open_ex_;
};

/**
 * An implementation of \ref persistence that proxies directly to Seastar I/O
 * interfaces.
 */
class disk_persistence final : public persistence {
public:
    /**
     * An implementation of \ref persistence::file that proxies directly to
     * Seastar I/O interfaces.
     */
    class disk_file final : public file {
    public:
        /**
         * Construct instance from Seastar \p file.
         */
        explicit disk_file(seastar::file file);

        seastar::future<size_t>
        dma_read(uint64_t pos, char* buf, size_t len) noexcept override;

        seastar::future<size_t>
        dma_write(uint64_t pos, const char* buf, size_t len) noexcept override;

        seastar::future<> close() noexcept override;

        [[nodiscard]] uint64_t
        disk_read_dma_alignment() const noexcept override;
        [[nodiscard]] uint64_t
        disk_write_dma_alignment() const noexcept override;
        [[nodiscard]] uint64_t memory_dma_alignment() const noexcept override;

    private:
        seastar::file file_;
    };

    seastar::temporary_buffer<char>
    allocate(uint64_t alignment, size_t size) noexcept override;

    seastar::future<seastar::shared_ptr<file>>
    create(std::filesystem::path path) noexcept override;

    seastar::future<seastar::shared_ptr<file>>
    open(std::filesystem::path path) noexcept override;
};

/**
 * An implementation of \ref persistence that stores all data in memory.
 *
 * The in-memory file system is fast, making it possible to run large parameter
 * tests much faster than using the disk-backed file system. However, there are
 * benefits that go beyond speed. The in-memory file system allows us to
 * customize properties like alignment requirements while continuing to have a
 * working storage layer. This is useful because it lets us test scenarios that
 * would be difficult to construct in practice, such as using an OS or hardware
 * with uncommon alignment requirements.
 */
class memory_persistence : public persistence {
    static constexpr uint64_t default_alignment = 4096;

public:
    /**
     * Create a \ref memory_persistence.
     */
    memory_persistence();

    /**
     * Memory persistence parameters.
     */
    struct config {
        /// Required alignment for DMA read operations.
        uint64_t disk_read_dma_alignment;
        /// Required alignment for DMA write operations.
        uint64_t disk_write_dma_alignment;
        /// Required memory alignment for DMA operations.
        uint64_t memory_dma_alignment;
    };

    /**
     * Create a memory_persistence with specific alignment requirements.
     */
    explicit memory_persistence(config config);

    /**
     * An implementation of \ref persistence::file that stores all data in
     * memory.
     */
    class memory_file final : public file {
        static constexpr uint64_t chunk_size = 4096;

    public:
        /**
         * Create an empty file in the underlying \p persistence.
         */
        explicit memory_file(memory_persistence* persistence);

        seastar::future<size_t>
        dma_read(uint64_t pos, char* buf, size_t len) noexcept override;

        seastar::future<size_t>
        dma_write(uint64_t pos, const char* buf, size_t len) noexcept override;

        seastar::future<> close() noexcept override;

        [[nodiscard]] uint64_t
        disk_read_dma_alignment() const noexcept override;
        [[nodiscard]] uint64_t
        disk_write_dma_alignment() const noexcept override;
        [[nodiscard]] uint64_t memory_dma_alignment() const noexcept override;

    private:
        seastar::future<> ensure_capacity(size_t size);
        seastar::future<size_t> read(uint64_t pos, char* buf, size_t len);
        seastar::future<size_t>
        write(uint64_t pos, const char* buf, size_t len);

        size_t size_{0};
        size_t capacity_{0};
        memory_persistence* persistence_;
        absl::btree_map<size_t, seastar::temporary_buffer<char>> data_;
    };

    seastar::temporary_buffer<char>
    allocate(uint64_t alignment, size_t size) noexcept override;

    seastar::future<seastar::shared_ptr<file>>
    create(std::filesystem::path path) noexcept override;

    seastar::future<seastar::shared_ptr<file>>
    open(std::filesystem::path path) noexcept override;

private:
    uint64_t disk_read_dma_alignment_;
    uint64_t disk_write_dma_alignment_;
    uint64_t memory_dma_alignment_;

    struct fs_path_hash {
        std::size_t
        operator()(const std::filesystem::path& path) const noexcept {
            return std::filesystem::hash_value(path);
        }
    };

    absl::flat_hash_map<
      std::filesystem::path,
      seastar::shared_ptr<memory_file>,
      fs_path_hash>
      files_;
};

} // namespace experimental::io
