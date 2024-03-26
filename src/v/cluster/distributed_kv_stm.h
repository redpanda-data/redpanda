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

#include "cluster/logger.h"
#include "distributed_kv_stm_types.h"
#include "raft/persisted_stm.h"

#include <type_traits>

namespace cluster {

using namespace distributed_kv_stm_types;
/**
 * Implements simple distributed KV store like semantics using the state
 * machine abstraction. Backed by a Kafka topic with one or more partitions.
 *
 * Supports the following operations
 *
 * put(map<key, val>) - bulk/batched put
 * get(key)
 * remove(key)
 * coordinator(key) - only on routing partition (discussed below)
 *
 * Input KV pairs are spread across the partitions of the topic for
 * load balancing, this requires that each key K to be tied to a partition.
 * Such a partition is called coordinator. A coordinator for a key K is the
 * id of the partition responsible for storage and querying KV mapping for K.

 * An easy way to map a key -> coordinator is using a static hash function but
 * that doesn't give us the flexibility to increase the partition to scale
 * the operations. For example, we start with 1 partition, realize it is a
 * bottleneck and want to increase to 3 partitions, the hash function is
 * stale. To have this flexibility of scaling partitions, we use a routing
 * partition that tracks the coordinator mappings. It is partition 0 of the
 * topic that also works as a coordinator along with routing capabilities.
 *
 * Routing partition keeps track of repartitioning operations and remembers
 * the coordinator mappings prior to repartitiong. So any new keys are hashed
 * across a larger set of partitions while still retaining original mappings.
 * Migration of keys across coordinators is not supported and can be added
 * later on.
 *
 * The query pattern from clients is expected to be as follows.
 * - query routing_partition using coordinator(key) to get the coordinator
 *   for key.
 * - call put(map<key, val>)/get(key)/remove(key) on the coordinator.
 *
 * For a reference client implementation, look at transforms_offsets topic
 * that uses this KV store to track consumption offsets for transforms.
 *
 * Currently we require constant memory usage, which is enforced by ensuring the
 * types are trivially copyable.
 */

template<class T>
concept SerdeSerializable = requires(T t, iobuf buf, iobuf_parser parser) {
    serde::write_async(buf, t);
    serde::read_async<T>(parser);
    serde::from_iobuf<T>(std::move(buf));
    serde::to_iobuf(t);
};

template<
  SerdeSerializable Key,
  SerdeSerializable Value,
  size_t MaxMemoryUsage = 1_MiB>
requires std::is_trivially_copyable_v<Key>
         && std::is_trivially_copyable_v<Value>
class distributed_kv_stm final : public raft::persisted_stm<> {
public:
    static constexpr const char* name = "distributed_kv_stm";
    explicit distributed_kv_stm(
      size_t max_partitions, ss::logger& logger, raft::consensus* raft)
      : persisted_stm<>("distributed_kv_stm.snapshot", logger, raft)
      , _default_max_partitions(max_partitions)
      , _is_routing_partition(_raft->ntp().tp.partition == routing_partition){};

    ss::future<> start() override { co_await raft::persisted_stm<>::start(); }
    ss::future<> stop() override { co_await _gate.close(); }

    ss::future<> apply(const model::record_batch& record_batch) override {
        if (record_batch.header().type != model::record_batch_type::raft_data) {
            co_return;
        }
        auto holder = _gate.hold();
        auto units = co_await _snapshot_lock.hold_read_lock();
        co_await record_batch.for_each_record_async(
          [this](model::record r) -> ss::future<> {
              auto key = reflection::from_iobuf<record_key>(r.release_key());
              auto val = reflection::from_iobuf<record_value_wrapper>(
                r.release_value());
              if (key.type == record_type::repartitioning) {
                  do_apply_repartition(std::move(val.actual_value));
              } else if (key.type == record_type::coordinator_assignment) {
                  do_apply_assignment(
                    std::move(key.key_data), std::move(val.actual_value));
              } else if (key.type == record_type::kv_data) {
                  do_apply_kvs(
                    std::move(key.key_data), std::move(val.actual_value));
              } else {
                  vlog(
                    clusterlog.error,
                    "skipped applying unknown record type: {}",
                    static_cast<int8_t>(key.type));
              }
              return ss::now();
          });
    }

    ss::future<> apply_local_snapshot(
      raft::stm_snapshot_header header, iobuf&& bytes) override {
        auto holder = _gate.hold();
        auto units = _snapshot_lock.hold_write_lock();

        iobuf_parser parser(std::move(bytes));
        auto snap = co_await serde::read_async<snapshot>(parser);

        if (_is_routing_partition) {
            _num_partitions = snap.num_partitions;
            _coordinators.clear();
            if (snap.coordinators) {
                _coordinators = std::move(snap.coordinators.value());
            }
        }
        _kvs = std::move(snap.kv_data);
    }

    ss::future<raft::stm_snapshot> take_local_snapshot() override {
        auto holder = _gate.hold();
        auto units = _snapshot_lock.hold_write_lock();
        auto last_applied = last_applied_offset();
        snapshot result;
        if (_is_routing_partition) {
            result.num_partitions = result.num_partitions;
            result.coordinators = _coordinators;
        }
        result.kv_data = _kvs;
        iobuf result_buf;
        co_await serde::write_async(result_buf, std::move(result));
        co_return raft::stm_snapshot::create(
          0, last_applied, std::move(result_buf));
    }

    ss::future<> apply_raft_snapshot(const iobuf&) final {
        // kv expiry is currently explicit using remove(), nothing to
        // do on log truncations.
        co_return;
    }

    // TODO: implement delete retention with incremental raft snapshots.
    ss::future<iobuf> take_snapshot(model::offset) final { co_return iobuf{}; }

    ss::future<result<model::partition_id, cluster::errc>>
    coordinator(Key key) {
        auto holder = _gate.hold();
        if (!_is_routing_partition) {
            co_return errc::invalid_request;
        }
        auto units = co_await _snapshot_lock.hold_read_lock();
        if (!co_await sync(sync_timeout)) {
            co_return errc::not_leader;
        }
        auto it = _coordinators.find(key);
        if (it != _coordinators.end()) {
            const auto& current = it->second;
            if (
              current.status
              == coordinator_assignment_status::migration_in_progress) {
                // keep the client retrying until migration finishes
                co_return errc::not_leader;
            }
            co_return current.partition;
        }
        auto num_partitions = co_await total_partitions();
        if (num_partitions.has_error()) {
            co_return num_partitions.error();
        }

        if (_kvs.size() >= keys_limit) {
            co_return errc::trackable_keys_limit_exceeded;
        }

        // new assignment
        iobuf buf;
        serde::write(buf, key);
        auto bytes = iobuf_to_bytes(buf);
        auto result = model::partition_id{
          murmur2(bytes.c_str(), bytes.length()) % num_partitions.value()};

        auto res = co_await replicate_and_wait(
          make_coordinator_assignment_batch(
            key, result, coordinator_assignment_status::assigned));

        if (res != errc::success) {
            co_return res;
        }
        co_return co_await coordinator(key);
    }

    ss::future<result<std::optional<Value>, cluster::errc>> get(Key key) {
        auto holder = _gate.hold();
        auto units = co_await _snapshot_lock.hold_read_lock();
        if (!co_await sync(sync_timeout)) {
            co_return errc::not_leader;
        }
        auto it = _kvs.find(key);
        if (it == _kvs.end()) {
            co_return std::nullopt;
        }
        co_return it->second;
    }

    ss::future<errc> put(absl::btree_map<Key, Value> kvs) {
        auto holder = _gate.hold();
        auto units = co_await _snapshot_lock.hold_read_lock();
        if (!co_await sync(sync_timeout)) {
            co_return errc::not_leader;
        }
        co_return co_await replicate_and_wait(
          make_kv_data_batch(std::move(kvs)));
    }

    ss::future<errc> remove(Key key) {
        auto holder = _gate.hold();
        auto units = co_await _snapshot_lock.hold_read_lock();
        auto it = _kvs.find(key);
        if (it == _kvs.end()) {
            co_return errc::success;
        }
        co_return co_await replicate_and_wait(
          make_kv_data_batch_remove_key<Key, Value>(key));
    }

    ss::future<result<size_t, cluster::errc>>
    repartition(size_t new_partition_count) {
        auto holder = _gate.hold();
        if (!_is_routing_partition) {
            co_return errc::invalid_request;
        }
        if (!co_await sync(sync_timeout)) {
            co_return errc::not_leader;
        }
        auto units = co_await _snapshot_lock.hold_read_lock();
        auto repartition_units = co_await _repartitioning_lock.get_units();
        if (_num_partitions && _num_partitions.value() == new_partition_count) {
            co_return _num_partitions.value();
        }
        if (_num_partitions && _num_partitions.value() > new_partition_count) {
            // reducing the number of partitions is disallowed without
            // a proper migration protocol.
            co_return errc::invalid_request;
        }
        auto res = co_await replicate_and_wait(
          make_repartitioning_batch(new_partition_count));

        if (res != errc::success) {
            co_return res;
        }
        vassert(
          _num_partitions.has_value(),
          "unexpected state, stm has not applied {}",
          new_partition_count);
        co_return _num_partitions.value();
    }

private:
    static constexpr model::partition_id routing_partition{0};
    static constexpr std::chrono::seconds sync_timeout{5};
    static constexpr size_t data_entry_memory_usage = sizeof(Key)
                                                      + sizeof(Value);
    static constexpr size_t coordinator_entry_memory_usage
      = sizeof(Key) + sizeof(coordinator_assignment_data);
    // a guardrail until an inactivity based expiration is
    // implemented.
    static constexpr size_t keys_limit
      = MaxMemoryUsage
        / (data_entry_memory_usage + coordinator_entry_memory_usage);

    void do_apply_repartition(iobuf buf) {
        auto data = serde::from_iobuf<repartitioning_record_data>(
          std::move(buf));
        // todo: add logging on state changes.
        _num_partitions = data.num_partitions;
    }
    void do_apply_assignment(iobuf key, iobuf value) {
        auto key_data = serde::from_iobuf<coordinator_assignment_key<Key>>(
          std::move(key));
        auto value_data = serde::from_iobuf<coordinator_assignment_data>(
          std::move(value));
        _coordinators[key_data.key] = value_data;
    }
    void do_apply_kvs(iobuf key, iobuf value) {
        auto key_data = serde::from_iobuf<kv_data_key<Key>>(std::move(key));
        auto val_data = serde::from_iobuf<kv_data_value<Value>>(
          std::move(value));
        if (val_data.value) {
            _kvs[key_data.key] = *val_data.value;
            return;
        }
        _kvs.erase(key_data.key);
    }

    ss::future<result<size_t, cluster::errc>> total_partitions() {
        auto holder = _gate.hold();
        if (!_is_routing_partition || !co_await sync(sync_timeout)) {
            co_return errc::not_leader;
        }
        if (likely(_num_partitions)) {
            co_return _num_partitions.value();
        }
        co_return co_await repartition(_default_max_partitions);
    }

    ss::future<errc> replicate_and_wait(simple_batch_builder builder) {
        auto batch = std::move(builder).build();
        auto reader = model::make_memory_record_batch_reader(std::move(batch));

        auto r = co_await _raft->replicate(
          _insync_term,
          std::move(reader),
          raft::replicate_options(raft::consistency_level::quorum_ack));

        if (!r) {
            co_return errc::replication_error;
        }

        if (!co_await wait_no_throw(
              {r.value().last_offset},
              model::timeout_clock::now() + sync_timeout)) {
            co_return errc::timeout;
        }
        co_return errc::success;
    }

    using coordinator_assignment_t
      = absl::btree_map<Key, coordinator_assignment_data>;
    using kv_data_t = absl::btree_map<Key, Value>;

    struct snapshot
      : serde::envelope<snapshot, serde::version<0>, serde::compat_version<0>> {
        std::optional<size_t> num_partitions;
        std::optional<coordinator_assignment_t> coordinators;
        kv_data_t kv_data;

        auto serde_fields() {
            return std::tie(num_partitions, coordinators, kv_data);
        }
    };

    std::optional<size_t> _num_partitions;
    // only populated on the routing_partition.
    coordinator_assignment_t _coordinators;
    kv_data_t _kvs;
    size_t _default_max_partitions;
    const bool _is_routing_partition;
    ss::gate _gate;
    // grabbed in exclusive mode when taking/applying snapshots so
    // there is a consistent state. All readers/updaters grab this
    // in read mode and wait until the snapshot operations finish.
    ss::rwlock _snapshot_lock;
    mutex _repartitioning_lock;
};

} // namespace cluster
