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

#include "base/format_to.h"
#include "base/seastarx.h"
#include "model/fundamental.h"
#include "model/timestamp.h"
#include "storage/fwd.h"

#include <seastar/core/io_queue.hh>
#include <seastar/util/bool_class.hh>

#include <string_view>

namespace storage {

/// Where replay stopped reading a segment. `checkpoint::operator bool` says
/// whether the segment held any data, and a full description of an
/// unrecoverable segment needs both.
enum class replay_stop_reason {
    /// Replay read to the end of the file and validated every batch.
    end_of_file,
    /// The bytes where a batch header belongs read back as zeros. Two causes
    /// produce this: a preallocated region that nobody wrote, or corruption
    /// that zeroed the bytes.
    zeroed_batch_header,
    /// A batch header failed the CRC it carries over its own fields.
    header_crc_mismatch,
    /// A batch header was intact but the records it covers failed the batch
    /// CRC.
    record_crc_mismatch,
    /// The file ended before the batch did.
    truncated_batch,
    /// Replay threw, for example on an input or output error.
    threw,
};

std::string_view to_string_view(replay_stop_reason);

class log_replayer {
public:
    explicit log_replayer(segment& seg) noexcept
      : _seg(&seg) {}

    struct checkpoint {
        std::optional<model::offset> last_offset;
        std::optional<size_t> truncate_file_pos;
        std::optional<model::timestamp> last_max_timestamp;

        /// Why replay stopped. `recover_in_thread` sets this before it
        /// returns.
        std::optional<replay_stop_reason> stop_reason;

        explicit operator bool() const {
            return last_offset && truncate_file_pos && last_max_timestamp;
        }

        fmt::iterator format_to(fmt::iterator it) const;
    };

    const checkpoint& last_checkpoint() const { return _ckpt; }

    // Must be called in the context of a ss::thread
    checkpoint recover_in_thread();

private:
    checkpoint _ckpt;
    segment* _seg;
};

} // namespace storage
