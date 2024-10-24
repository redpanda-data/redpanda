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

#include "cloud_storage/remote_segment.h"
#include "cluster/archival/ntp_archiver_service.h"
#include "cluster/partition.h"
#include "config/configuration.h"
#include "kafka/server/tests/produce_consume_utils.h"
#include "storage/disk_log_impl.h"

namespace tests {

class remote_segment_generator {
public:
    remote_segment_generator(
      kafka::client::transport transport, cluster::partition& partition)
      : _producer(std::move(transport))
      , _partition(partition) {}

    remote_segment_generator& num_segments(size_t n) {
        _num_remote_segs = n;
        return *this;
    }
    remote_segment_generator& additional_local_segments(size_t n) {
        _num_local_segs = n;
        return *this;
    }
    remote_segment_generator& records_per_batch(size_t r) {
        _records_per_batch = r;
        return *this;
    }
    remote_segment_generator& batches_per_segment(size_t b) {
        _batches_per_seg = b;
        return *this;
    }
    remote_segment_generator& batch_time_delta_ms(int ms) {
        _batch_time_delta_ms = ms;
        return *this;
    }
    remote_segment_generator& base_timestamp(model::timestamp ts) {
        _base_timestamp = ts;
        return *this;
    }
    kafka_produce_transport& producer() { return _producer; }

    // Produces records, flushing and rolling the local log, and uploading
    // according to the given parameters. Once the given segments are produced,
    // the partition manifest is uploaded.
    //
    // The produced pattern "key<i>", "val<i>" for Kafka offset i.
    // Expects to be the only one mutating the underlying partition state.
    ss::future<int> produce() {
        co_await _producer.start();
        auto log = _partition.log();
        auto& archiver = _partition.archiver().value().get();

        size_t total_records = 0;
        auto cur_timestamp = _base_timestamp;
        while (_partition.archival_meta_stm()->manifest().size()
               < _num_remote_segs) {
            for (size_t i = 0; i < _batches_per_seg; i++) {
                std::vector<kv_t> records = kv_t::sequence(
                  total_records, _records_per_batch);
                total_records += _records_per_batch;
                co_await _producer.produce_to_partition(
                  _partition.ntp().tp.topic,
                  _partition.ntp().tp.partition,
                  std::move(records),
                  cur_timestamp);
                if (cur_timestamp.has_value()) {
                    cur_timestamp = model::timestamp(
                      (*cur_timestamp)() + _batch_time_delta_ms);
                }
            }
            co_await log->flush();
            co_await log->force_roll(ss::default_priority_class());
            if (
              config::shard_local_cfg()
                .cloud_storage_disable_upload_loop_for_tests.value()
              && !co_await archiver.sync_for_tests()) {
                co_return -1;
            }
            if (
              (co_await archiver.upload_next_candidates())
                .non_compacted_upload_result.num_failed
              > 0) {
                co_return -1;
            }
        }
        // Upload and flush the manifest to unpin the max collectable offset.
        if (
          config::shard_local_cfg()
            .cloud_storage_disable_upload_loop_for_tests.value()
          && !co_await archiver.sync_for_tests()) {
            co_return -1;
        }
        auto manifest_res = co_await archiver.upload_manifest("test");
        if (manifest_res != cloud_storage::upload_result::success) {
            co_return -1;
        }
        co_await archiver.flush_manifest_clean_offset();
        for (int i = 0; i < _num_local_segs; i++) {
            for (size_t i = 0; i < _batches_per_seg; i++) {
                std::vector<kv_t> records = kv_t::sequence(
                  total_records, _records_per_batch);
                total_records += _records_per_batch;
                co_await _producer.produce_to_partition(
                  _partition.ntp().tp.topic,
                  _partition.ntp().tp.partition,
                  std::move(records),
                  cur_timestamp);
                if (cur_timestamp.has_value()) {
                    cur_timestamp = model::timestamp(
                      (*cur_timestamp)() + _batch_time_delta_ms);
                }
            }
            co_await log->flush();
            co_await log->force_roll(ss::default_priority_class());
        }
        co_return total_records;
    }

private:
    kafka_produce_transport _producer;
    cluster::partition& _partition;
    // Number of remote segments to create.
    size_t _num_remote_segs{1};
    // Number of additional local segments to create.
    size_t _num_local_segs{0};
    size_t _records_per_batch{1};
    size_t _batches_per_seg{1};
    std::optional<model::timestamp> _base_timestamp{std::nullopt};
    int _batch_time_delta_ms{1};
};

} // namespace tests
