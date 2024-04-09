/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "record_batcher.h"

#include "absl/algorithm/container.h"
#include "logger.h"
#include "model/record.h"
#include "storage/record_batch_builder.h"
#include "utils/human.h"

namespace transform::logging {

namespace detail {
class batcher_impl {
public:
    batcher_impl() = delete;
    explicit batcher_impl(size_t batch_max_bytes)
      : _batch_max_bytes(batch_max_bytes) {}
    ~batcher_impl() = default;
    batcher_impl(const batcher_impl&) = delete;
    batcher_impl& operator=(const batcher_impl&) = delete;
    batcher_impl(batcher_impl&&) = delete;
    batcher_impl& operator=(batcher_impl&&) = delete;

    model::record_batch make_batch_of_one(iobuf k, iobuf v) {
        return std::move(bb_init().add_raw_kv(std::move(k), std::move(v)))
          .build();
    }

    void append(iobuf k, iobuf v) {
        auto record = model::record(
          /*attributes*/ {},
          /*ts_delta*/ 0,
          /*offset_delta*/ std::numeric_limits<int32_t>::max(),
          std::move(k),
          std::move(v),
          /*headers*/ {});
        size_t record_size = record.size_bytes();
        if (record_size > max_records_bytes()) {
            vlog(
              tlg_log.info,
              "Dropped record: size exceeds configured batch max "
              "size: {} > {}",
              human::bytes{static_cast<double>(record_size)},
              human::bytes{static_cast<double>(max_records_bytes())});
            return;
        } else if (record_size >= curr_batch_headroom_bytes()) {
            roll_batch();
        }
        _curr_batch_size += record_size;
        _builder.add_raw_kv(record.release_key(), record.release_value());
    }

    size_t total_size_bytes() {
        return absl::c_accumulate(
          _record_batches, 0, [](size_t acc, const model::record_batch& b) {
              return acc + b.size_bytes();
          });
    }

    ss::chunked_fifo<model::record_batch> finish() {
        if (!_builder.empty()) {
            roll_batch();
        }
        return std::exchange(_record_batches, {});
    }

private:
    size_t curr_batch_headroom_bytes() const {
        return max_records_bytes() - curr_records_bytes();
    }
    size_t batch_size_bytes() const {
        return curr_records_bytes() + model::packed_record_batch_header_size;
    }
    size_t curr_records_bytes() const { return _curr_batch_size; }
    size_t max_records_bytes() const {
        return _batch_max_bytes - model::packed_record_batch_header_size;
    }

    void roll_batch() {
        auto batch = std::exchange(_builder, bb_init()).build();
        auto diff = static_cast<int64_t>(batch_size_bytes())
                    - static_cast<int64_t>(batch.size_bytes());
        if (diff < 0) {
            vlog(
              tlg_log.debug,
              "Underestimaged batch size {} - {} = {}",
              human::bytes{static_cast<double>(batch_size_bytes())},
              human::bytes{static_cast<double>(batch.size_bytes())},
              diff);
        } else {
            vlog(
              tlg_log.trace,
              "Building record batch. Actual size: {} (estimated: {}, err:{})",
              human::bytes{static_cast<double>(batch.size_bytes())},
              human::bytes{static_cast<double>(batch_size_bytes())},
              diff);
        }
        // NOTE(oren): We could drop a batch here if it's too large to save
        // bandwidth, but this should happen very rarely if ever (estimates are
        // deliberately conservative). The most important thing is that we roll
        // the batch builder to make room for subsequent records.
        _curr_batch_size = 0;
        _record_batches.push_back(std::move(batch));
    }

    static storage::record_batch_builder bb_init() {
        return storage::record_batch_builder{
          model::record_batch_type::raft_data, model::offset{0}};
    }

    size_t _batch_max_bytes;
    storage::record_batch_builder _builder{bb_init()};
    ss::chunked_fifo<model::record_batch> _record_batches;
    size_t _curr_batch_size{0};
};

} // namespace detail

record_batcher::record_batcher(size_t max_batch_size)
  : _impl(std::make_unique<detail::batcher_impl>(max_batch_size)) {}

record_batcher::~record_batcher() = default;

void record_batcher::append(iobuf k, iobuf v) {
    _impl->append(std::move(k), std::move(v));
}
size_t record_batcher::total_size_bytes() { return _impl->total_size_bytes(); }

ss::chunked_fifo<model::record_batch> record_batcher::finish() {
    return _impl->finish();
}

} // namespace transform::logging
