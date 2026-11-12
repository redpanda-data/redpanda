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
#include "container/chunked_vector.h"
#include "model/fundamental.h"
#include "model/timestamp.h"
#include "storage/fwd.h"
#include "storage/types.h"

#include <seastar/core/io_queue.hh>
#include <seastar/util/bool_class.hh>

namespace storage {

class log_replayer {
public:
    explicit log_replayer(
      segment& seg, config_batch_term_parser term_parser = {}) noexcept
      : _seg(&seg)
      , _term_parser(std::move(term_parser)) {}

    struct checkpoint {
        std::optional<model::offset> last_offset;
        std::optional<size_t> truncate_file_pos;
        std::optional<model::timestamp> last_max_timestamp;
        // term transitions (term, first offset) recovered from raft
        // configuration batch payloads, in offset order
        chunked_vector<std::pair<model::term_id, model::offset>>
          term_transitions;
        explicit operator bool() const {
            return last_offset && truncate_file_pos && last_max_timestamp;
        }

        fmt::iterator format_to(fmt::iterator it) const;
    };

    // Must be called in the context of a ss::thread
    checkpoint recover_in_thread();

private:
    checkpoint _ckpt;
    segment* _seg;
    config_batch_term_parser _term_parser;
};

} // namespace storage
