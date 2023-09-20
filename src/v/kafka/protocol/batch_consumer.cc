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

#include "kafka/protocol/batch_consumer.h"
#include "kafka/server/logger.h"

namespace kafka {

ss::future<ss::stop_iteration> kafka_batch_serializer::operator()(model::record_batch&& batch) {
    vlog(
      klog.trace,
      "SVETA(1): tx?:{} control?: base:{} last:{} records:{}",
      batch.header().attrs.is_transactional(),
      batch.header().attrs.is_control(),
      batch.base_offset(),
      batch.last_offset(),
      batch.record_count());
    
    if (unlikely(record_count_ == 0)) {
        _base_offset = batch.base_offset();
    }
    if (unlikely(
          !_first_tx_batch_offset
          && batch.header().attrs.is_transactional())) {
        _first_tx_batch_offset = batch.base_offset();
    }
    _last_offset = batch.last_offset();
    record_count_ += batch.record_count();
    write_batch(std::move(batch));
    return ss::make_ready_future<ss::stop_iteration>(
      ss::stop_iteration::no);
}

kafka_batch_serializer::result kafka_batch_serializer::end_of_stream() {
    return result{
      .data = std::move(_buf),
      .record_count = record_count_,
      .base_offset = _base_offset,
      .last_offset = _last_offset,
      .first_tx_batch_offset = _first_tx_batch_offset,
    };
}

} // namespace kafka
