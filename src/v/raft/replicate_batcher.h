#pragma once

#include "model/record_batch_reader.h"
#include "outcome.h"
#include "raft/types.h"
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
    // 1MB default size
    static constexpr size_t default_batch_bytes = 1024 * 1024;

    explicit replicate_batcher(
      consensus* ptr, size_t cache_size = default_batch_bytes);

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

    consensus* _ptr;
    size_t _max_batch_size{default_batch_bytes};
    size_t _pending_bytes{0};
    timer_type _flush_timer;

    std::vector<item_ptr> _item_cache;
    ss::circular_buffer<model::record_batch> _data_cache;
    mutex _lock;
};

} // namespace raft
