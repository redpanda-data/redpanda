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

#include "container/intrusive_list_helpers.h"

#include <seastar/core/temporary_buffer.hh>

#include <bitset>
#include <cstdint>

namespace experimental::io {

/**
 * A page represents a contiguous region of data in a file.
 */
class page {
public:
    /**
     * Construct a page with the given \p offset and \p data.
     */
    page(uint64_t offset, seastar::temporary_buffer<char> data);

    page(const page&) = delete;
    page& operator=(const page&) = delete;
    page(page&&) = delete;
    page& operator=(page&&) = delete;
    ~page() = default;

    /**
     * Offset of this page in the underlying file.
     *
     * The offset is fixed for the lifetime of the page.
     */
    [[nodiscard]] uint64_t offset() const noexcept;

    /**
     * Size of the this page.
     *
     * The size is fixed, even if the page data is removed.
     */
    [[nodiscard]] uint64_t size() const noexcept;

    /**
     * Data stored in this page.
     */
    [[nodiscard]] seastar::temporary_buffer<char>& data() noexcept;
    [[nodiscard]] const seastar::temporary_buffer<char>& data() const noexcept;

    /*
     * read,write: page is queued for read or write
     * faulting: page is faulting when a read is occuring in response to a cache miss.
     * dirty: page contains data not persisted to disk
     */
    enum class flags { faulting, dirty, read, write, queued, num_flags };

    /**
     * set a page flag.
     */
    void set_flag(flags) noexcept;

    /**
     * clear a page flag.
     */
    void clear_flag(flags) noexcept;

    /**
     * Return true if the flag is set, and false otherwise.
     */
    [[nodiscard]] bool test_flag(flags) const noexcept;

    /**
     * Intrusive list hook for I/O queue membership.
     */
    // NOLINTNEXTLINE(*-non-private-member-variables-in-classes)
    intrusive_list_hook io_queue_hook;

private:
    static constexpr auto num_page_flags
      = static_cast<std::underlying_type_t<flags>>(flags::num_flags);

    uint64_t offset_;
    uint64_t size_;
    seastar::temporary_buffer<char> data_;
    std::bitset<num_page_flags> flags_;
};

} // namespace experimental::io
