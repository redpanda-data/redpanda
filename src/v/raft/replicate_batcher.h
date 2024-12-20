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

#include "base/outcome.h"
#include "container/fragmented_vector.h"
#include "model/record_batch_reader.h"
#include "raft/types.h"
#include "ssx/semaphore.h"
#include "utils/mutex.h"

#include <seastar/core/gate.hh>

#include <absl/container/flat_hash_map.h>
namespace raft {
class consensus;

class replicate_batcher {
public:
    class item {
    public:
        item(
          size_t record_count,
          chunked_vector<model::record_batch> batches,
          ssx::semaphore_units u,
          std::optional<model::term_id> expected_term,
          replicate_options opts)
          : _record_count(record_count)
          , _data(std::move(batches))
          , _units(std::move(u))
          , _expected_term(expected_term)
          , _replicate_opts(opts) {
            _timeout_timer.set_callback([this] { expire_with_timeout(); });
            if (_replicate_opts.timeout) {
                _timeout_timer.arm(_replicate_opts.timeout.value());
            }
        };

        item(item&&) noexcept = default;
        item& operator=(item&&) noexcept = delete;

        item operator=(const item&) = delete;
        item(const item&) = delete;

        ~item() = default;

        std::optional<model::term_id> get_expected_term() const {
            return _expected_term;
        }

        size_t get_record_count() const { return _record_count; }
        consistency_level get_consistency_level() const {
            return _replicate_opts.consistency;
        }
        bool force_flush_requested() const {
            return _replicate_opts.force_flush();
        }

        auto release_data() {
            return std::make_tuple(std::move(_data), std::move(_units));
        }

        void set_value(result<replicate_result> r) {
            if (!_ready) {
                _timeout_timer.cancel();
                _ready = true;
                _promise.set_value(r);
            }
        }

        void set_exception(const std::exception_ptr& e) {
            if (!_ready) {
                _timeout_timer.cancel();
                _ready = true;
                _promise.set_exception(e);
            }
        }

        ss::future<result<replicate_result>> get_future() {
            return _promise.get_future();
        }

        bool ready() const { return _ready; }

    private:
        void expire_with_timeout() {
            if (!_ready) {
                _ready = true;
                _data.clear();
                _units.return_all();
                _promise.set_value(errc::timeout);
            }
        }
        size_t _record_count;
        chunked_vector<model::record_batch> _data;
        ssx::semaphore_units _units;
        std::optional<model::term_id> _expected_term;
        replicate_options _replicate_opts;
        /**
         * Item keeps semaphore units until replicate batcher is done with
         * processing the request.
         */

        bool _ready{false};
        ss::timer<> _timeout_timer;
        ss::promise<result<replicate_result>> _promise;
    };
    using item_ptr = ss::lw_shared_ptr<item>;
    explicit replicate_batcher(consensus* ptr, size_t cache_size);

    replicate_batcher(replicate_batcher&&) noexcept = default;
    replicate_batcher& operator=(replicate_batcher&&) noexcept = delete;
    replicate_batcher(const replicate_batcher&) = delete;
    replicate_batcher& operator=(const replicate_batcher&) = delete;
    ~replicate_batcher() noexcept = default;

    replicate_stages replicate(
      std::optional<model::term_id>,
      chunked_vector<model::record_batch>,
      replicate_options);

    ss::future<> flush(ssx::semaphore_units u, const bool transfer_flush);

    ss::future<> stop();

private:
    ss::future<> do_flush(
      std::vector<item_ptr>,
      append_entries_request,
      std::vector<ssx::semaphore_units>,
      absl::flat_hash_map<vnode, follower_req_seq>);

    ss::future<item_ptr> do_cache(
      std::optional<model::term_id>,
      chunked_vector<model::record_batch>,
      replicate_options);

    ss::future<replicate_batcher::item_ptr> do_cache_with_backpressure(
      std::optional<model::term_id>,
      chunked_vector<model::record_batch>,
      size_t,
      replicate_options);

    ss::future<result<replicate_result>> cache_and_wait_for_result(
      ss::promise<> enqueued,
      std::optional<model::term_id> expected_term,
      chunked_vector<model::record_batch> r,
      replicate_options);

    consensus* _ptr;
    ssx::semaphore _max_batch_size_sem;
    size_t _max_batch_size;
    std::vector<item_ptr> _item_cache;
    mutex _lock{"replicate_batcher"};
    ss::gate _bg;
    // If true, a background flush must be pending. Used to coalesce
    // background flush requests, since one flush dequeues all items
    // in the item cache. Without this, a high rate of replication may
    // cause the _item_cache to grow without bound since the rate of
    // flush task execution can be lower than the rate at which new
    // items are added to the cache.
    bool _flush_pending = false;
};

} // namespace raft
