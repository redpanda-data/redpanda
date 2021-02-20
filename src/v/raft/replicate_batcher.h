/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "model/record_batch_reader.h"
#include "outcome.h"
#include "raft/types.h"
#include "units.h"
#include "utils/mutex.h"

#include <seastar/core/semaphore.hh>

#include <absl/container/flat_hash_map.h>
namespace raft {
class consensus;

class replicate_batcher {
public:
    struct item {
        ss::promise<result<replicate_result>> _promise;
        size_t record_count;
        std::vector<model::record_batch> data;
        std::optional<model::term_id> expected_term;
        /**
         * Item keeps semaphore units until replicate batcher is done with
         * processing the request.
         */
        ss::semaphore_units<> units;
    };
    using item_ptr = ss::lw_shared_ptr<item>;
    explicit replicate_batcher(consensus* ptr, size_t cache_size);

    replicate_batcher(replicate_batcher&&) noexcept = default;
    replicate_batcher& operator=(replicate_batcher&&) noexcept = delete;
    replicate_batcher(const replicate_batcher&) = delete;
    replicate_batcher& operator=(const replicate_batcher&) = delete;
    ~replicate_batcher() noexcept = default;

    ss::future<result<replicate_result>>
    replicate(std::optional<model::term_id>, model::record_batch_reader&&);

    ss::future<> flush();
    ss::future<> stop();

    // it will lock on behalf of caller to append entries to leader log.
    ss::future<> do_flush(
      std::vector<item_ptr>&&,
      append_entries_request&&,
      ss::semaphore_units<>,
      absl::flat_hash_map<vnode, follower_req_seq>);

private:
    ss::future<item_ptr>
    do_cache(std::optional<model::term_id>, model::record_batch_reader&&);
    ss::future<replicate_batcher::item_ptr> do_cache_with_backpressure(
      std::optional<model::term_id>,
      ss::circular_buffer<model::record_batch>,
      size_t);

    consensus* _ptr;
    ss::semaphore _max_batch_size_sem;

    std::vector<item_ptr> _item_cache;
    mutex _lock;
};

} // namespace raft
