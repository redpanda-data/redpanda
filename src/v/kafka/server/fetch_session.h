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
#include "kafka/protocol/errors.h"
#include "kafka/protocol/fetch.h"
#include "model/fundamental.h"
#include "model/ktp.h"
#include "model/timeout_clock.h"

#include <absl/container/flat_hash_map.h>
#include <boost/iterator/iterator_adaptor.hpp>
#include <boost/iterator/transform_iterator.hpp>
#include <boost/iterator_adaptors.hpp>

namespace kafka {

struct fetch_session_partition {
    model::ktp_with_hash topic_partition;
    int32_t max_bytes;
    model::offset start_offset;
    model::offset fetch_offset;
    model::offset high_watermark;
    model::offset last_stable_offset;
    kafka::leader_epoch current_leader_epoch = invalid_leader_epoch;

    fetch_session_partition(
      const model::topic& tp, const fetch_request::partition& p)
      : topic_partition(tp, p.partition_index)
      , max_bytes(p.max_bytes)
      , fetch_offset(p.fetch_offset)
      , high_watermark(model::offset(-1))
      , last_stable_offset(model::offset(-1))
      , current_leader_epoch(p.current_leader_epoch) {}
};
/**
 * Map of partitions that is kept by fetch session. This map is using intrusive
 * list to allow interation with preserved insertion order. We require such
 * semantics as partitions order in fetch request/response have to be the same.
 *
 * Internally the map is based on absl::flat_hash_map containing entries that
 * are additionally linked by being elements of an intrusive list. The intrusive
 * list provides the insertion order traversal across the partitions.
 */
class fetch_partitions_linked_hash_map {
private:
    struct entry {
        explicit entry(kafka::fetch_session_partition partition)
          : partition(std::move(partition)) {}

        kafka::fetch_session_partition partition;
        intrusive_list_hook _hook;
    };

    struct topic_partition_hash {
        using is_transparent = void;

        size_t operator()(model::topic_partition_view v) const {
            return absl::Hash<model::topic_partition_view>{}(v);
        }

        size_t operator()(const model::topic_partition& v) const {
            return absl::Hash<model::topic_partition>{}(v);
        }
    };

    struct topic_partition_eq {
        using is_transparent = void;

        bool operator()(
          const model::topic_partition& lhs,
          const model::topic_partition& rhs) const {
            return lhs.topic == rhs.topic && lhs.partition == rhs.partition;
        }

        bool operator()(
          const model::topic_partition_view& lhs,
          const model::topic_partition_view& rhs) const {
            return lhs.topic == rhs.topic && lhs.partition == rhs.partition;
        }

        bool operator()(
          const model::topic_partition& lhs,
          const model::topic_partition_view& rhs) const {
            return model::topic_view(lhs.topic) == rhs.topic
                   && lhs.partition == rhs.partition;
        }
        bool operator()(
          const model::topic_partition_view& lhs,
          const model::topic_partition& rhs) const {
            return operator()(rhs, lhs);
        }
    };

    using underlying_t = absl::flat_hash_map<
      model::topic_partition_view,
      std::unique_ptr<entry>,
      topic_partition_hash,
      topic_partition_eq>;

    using io_list_t = intrusive_list<entry, &entry::_hook>;

    static auto make_partition_iterator(io_list_t::const_iterator it) {
        return boost::iterators::make_transform_iterator(
          it, [](const entry& e) -> const kafka::fetch_session_partition& {
              return e.partition;
          });
    }

public:
    using iterator = typename underlying_t::iterator;
    using const_iterator = typename underlying_t::const_iterator;

    fetch_partitions_linked_hash_map() = default;
    ~fetch_partitions_linked_hash_map() {
        vassert(
          insertion_order.size() == partitions.size(),
          "partitions map and insertion order list sizes are not equal.");
    }

    void emplace(kafka::fetch_session_partition v) {
        auto e = std::make_unique<entry>(std::move(v));
        auto [it, success] = partitions.emplace(
          e->partition.topic_partition.as_tp_view(), std::move(e));
        vassert(
          success,
          "Can not insert {} to partitions map as it is already present.",
          it->second->partition.topic_partition);
        insertion_order.push_back(*it->second);
    }

    bool contains(model::topic_partition_view v) {
        return partitions.contains(v);
    }

    void erase(model::topic_partition_view v) { partitions.erase(v); }

    iterator find(model::topic_partition_view v) { return partitions.find(v); }

    const_iterator find(model::topic_partition_view v) const {
        return partitions.find(v);
    }

    bool empty() const { return partitions.empty(); }

    size_t size() const { return partitions.size(); }

    auto cbegin_insertion_order() const {
        return make_partition_iterator(insertion_order.cbegin());
    }

    auto cend_insertion_order() const {
        return make_partition_iterator(insertion_order.cend());
    }

    size_t mem_usage() {
        using debug = absl::container_internal::hashtable_debug_internal::
          HashtableDebugAccess<underlying_t>;
        return debug::AllocatedByteSize(partitions)
               + partitions.size() * sizeof(fetch_session_partition);
    }

    void move_to_end(iterator it) {
        it->second->_hook.unlink();
        insertion_order.push_back(*it->second);
    }

    iterator begin() { return partitions.begin(); }
    iterator end() { return partitions.end(); }

    const_iterator begin() const { return partitions.begin(); }
    const_iterator end() const { return partitions.end(); }

    underlying_t::const_iterator cbegin() { return partitions.cbegin(); }
    underlying_t::const_iterator cend() { return partitions.cend(); }

private:
    underlying_t partitions;
    intrusive_list<entry, &entry::_hook> insertion_order;
};

inline fetch_session_epoch next_epoch(fetch_session_epoch current) {
    if (current < initial_fetch_session_epoch) {
        // The next epoch after FINAL_EPOCH is always FINAL_EPOCH itself.
        return final_fetch_session_epoch;
    }
    return current + fetch_session_epoch(1);
}

class fetch_session {
public:
    explicit fetch_session(fetch_session_id id)
      : _id(id)
      , _created(model::timeout_clock::now())
      , _last_used(_created)
      , _epoch(next_epoch(initial_fetch_session_epoch))
      , _locked(false) {}

    bool empty() const { return _partitions.empty(); }

    fetch_session_id id() const { return _id; }

    fetch_partitions_linked_hash_map& partitions() { return _partitions; }

    const fetch_partitions_linked_hash_map& partitions() const {
        return _partitions;
    }

    fetch_session_epoch epoch() const { return _epoch; }

    void advance_epoch() {
        _epoch = next_epoch(_epoch);
        _last_used = model::timeout_clock::now();
    }

    bool is_locked() const { return _locked; }

    size_t mem_usage() {
        return sizeof(fetch_session) + _partitions.mem_usage();
    }

private:
    friend struct fetch_session_ctx;
    friend class fetch_session_cache;
    fetch_session_id _id;
    fetch_partitions_linked_hash_map _partitions;
    model::timeout_clock::time_point _created;
    model::timeout_clock::time_point _last_used;
    fetch_session_epoch _epoch;
    bool _locked;
};

using fetch_session_ptr = ss::lw_shared_ptr<fetch_session>;

/**
 * Wrapper representing the semantincs of particular fetch request which can
 * either contain an error or be sessionless, full (initiating/removing fetch
 * session) or incremental
 */
struct fetch_session_ctx {
    fetch_session_ctx() = default;

    explicit fetch_session_ctx(error_code err)
      : _error(err) {}

    fetch_session_ctx(fetch_session_ptr ptr, bool is_full)
      : _session(std::move(ptr))
      , _is_full_fetch(is_full) {
        _session->_locked = true;
    }

    fetch_session_ctx(fetch_session_ctx&&) = default;
    fetch_session_ctx& operator=(fetch_session_ctx&&) = default;

    fetch_session_ctx(const fetch_session_ctx&) = delete;
    fetch_session_ctx& operator=(const fetch_session_ctx&) = delete;

    ~fetch_session_ctx() {
        if (_session) {
            _session->_locked = false;
        }
    }

    bool is_sessionless() const { return !static_cast<bool>(_session); }

    bool has_error() const { return _error != error_code::none; }

    bool is_full_fetch() const { return _is_full_fetch; }

    fetch_session_ptr session() { return _session; }

    const fetch_session_ptr session() const { return _session; }

    error_code error() const { return _error; }

private:
    fetch_session_ptr _session = nullptr;
    bool _is_full_fetch = false;
    error_code _error = error_code::none;
};
} // namespace kafka
