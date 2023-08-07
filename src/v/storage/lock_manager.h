/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

// important to keep dependencies small because this type is used
// in the log readers and throughout the code where segment_set.h is used
// if it becomes large, consider making it a pimpl class
#include "storage/fwd.h"
#include "storage/segment_set.h"

#include <seastar/core/rwlock.hh>

namespace storage {
class lock_manager {
public:
    explicit lock_manager(segment_set& s) noexcept
      : _set(s) {}
    struct lease {
        explicit lease(segment_set s)
          : range(std::move(s)) {}
        ~lease() noexcept = default;
        lease(lease&&) noexcept = default;
        lease& operator=(lease&&) noexcept = default;
        lease(const lease&) = delete;
        lease& operator=(const lease&) = delete;

        segment_set range;
        std::vector<ss::rwlock::holder> locks;

        friend std::ostream& operator<<(std::ostream&, const lease&);
    };

    ss::future<std::unique_ptr<lease>> range_lock(const timequery_config& cfg);
    ss::future<std::unique_ptr<lease>> range_lock(const log_reader_config& cfg);

private:
    segment_set& _set;
};

} // namespace storage
