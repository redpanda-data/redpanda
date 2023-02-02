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

#include "model/fundamental.h"
#include "model/timestamp.h"
#include "seastarx.h"
#include "storage/fwd.h"

#include <seastar/core/io_queue.hh>
#include <seastar/util/bool_class.hh>

namespace storage {

class log_replayer {
public:
    explicit log_replayer(segment& seg) noexcept
      : _seg(&seg) {}

    struct checkpoint {
        std::optional<model::offset> last_offset;
        std::optional<size_t> truncate_file_pos;
        std::optional<model::timestamp> last_max_timestamp;
        explicit operator bool() const {
            return last_offset && truncate_file_pos && last_max_timestamp;
        }
    };

    const checkpoint& last_checkpoint() const { return _ckpt; }

    // Must be called in the context of a ss::thread
    checkpoint recover_in_thread(const ss::io_priority_class&);

private:
    checkpoint _ckpt;
    segment* _seg;

    friend std::ostream& operator<<(std::ostream&, const checkpoint&);
};

} // namespace storage
