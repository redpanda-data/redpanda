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

#include <seastar/core/future.hh>
#include <seastar/core/iostream.hh>
#include <seastar/core/temporary_buffer.hh>

namespace experimental::io {

class page;
class pager;

/**
 * An implementation of seastar::data_source backed by io::pager. Use this to
 * construct a Seastar input stream over file, providing write-back caching.
 */
class paging_data_source final : public seastar::data_source_impl {
public:
    struct config {
        uint64_t offset;
        uint64_t length;
    };

    /**
     * Construct a paging data source over the data managed by \p pager, limited
     * to the range defined by \p config.
     */
    paging_data_source(pager* pager, config config);

    /**
     * Construct a paging data source backed by a fixed set of pages.
     */
    paging_data_source(
      std::vector<seastar::lw_shared_ptr<page>>, config config);

    /**
     * Read some data.
     */
    seastar::future<seastar::temporary_buffer<char>> get() noexcept override;

private:
    seastar::temporary_buffer<char>
    finalize(seastar::temporary_buffer<char> page);

    pager* pager_;

    /// The next offset from which to read.
    uint64_t offset_;

    /// The amount of data left to read.
    uint64_t remaining_;

    /// Queue of retreived pages. Will be stored in reverse order of page
    /// offset for more efficient removal when consuming pages.
    std::vector<seastar::lw_shared_ptr<page>> pages_;
};

} // namespace experimental::io
