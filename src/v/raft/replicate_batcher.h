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

#include <absl/container/flat_hash_map.h>
namespace raft {
class consensus;

class replicate_batcher {
public:
    struct item {
        ss::promise<result<replicate_result>> _promise;
        replicate_result ret;
        size_t record_count;
    };
    using item_ptr = ss::lw_shared_ptr<item>;
    explicit replicate_batcher(
      consensus* ptr,
      std::chrono::milliseconds debounce_duration,
      size_t cache_size);

    replicate_batcher(replicate_batcher&&) noexcept = default;
    replicate_batcher& operator=(replicate_batcher&&) noexcept = delete;
    replicate_batcher(const replicate_batcher&) = delete;
    replicate_batcher& operator=(const replicate_batcher&) = delete;
    ~replicate_batcher() noexcept = default;

    ss::future<result<replicate_result>>
    replicate(model::record_batch_reader&&);

    ss::future<> flush();
    ss::future<> stop();

    // it will lock on behalf of caller to append entries to leader log.
    ss::future<> do_flush(
      std::vector<item_ptr>&&,
      append_entries_request&&,
      ss::semaphore_units<>,
      absl::flat_hash_map<model::node_id, follower_req_seq>);

private:
    ss::future<item_ptr> do_cache(model::record_batch_reader&&);
    void dispatch_background_flush();

    consensus* _ptr;
    std::chrono::milliseconds _debounce_duration;
    size_t _max_batch_size;
    size_t _pending_bytes{0};
    timer_type _flush_timer;

    std::vector<item_ptr> _item_cache;
    ss::circular_buffer<model::record_batch> _data_cache;
    mutex _lock;
};

} // namespace raft
