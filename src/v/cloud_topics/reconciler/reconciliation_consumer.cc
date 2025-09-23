/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/reconciler/reconciliation_consumer.h"

#include "model/timestamp.h"

namespace cloud_topics::reconciler {

reconciliation_consumer::reconciliation_consumer(
  l1::object_builder* builder, model::topic_id_partition tidp)
  : _builder(builder)
  , _tidp(tidp)
  , _metadata{
      .base_offset = kafka::offset::min(),
      .last_offset = kafka::offset::min(),
      .base_timestamp = model::timestamp::max(),
      .last_timestamp = model::timestamp::min()} {}

ss::future<ss::stop_iteration>
reconciliation_consumer::operator()(model::record_batch batch) {
    if (_metadata.base_offset == kafka::offset::min()) {
        _metadata.base_offset = model::offset_cast(batch.base_offset());
        co_await _builder->start_partition(_tidp);
    }

    // NOTE: Only data batches here. It's safe to use timestamps without
    //       checking the batch type.
    _metadata.base_timestamp = std::min(
      batch.header().first_timestamp, _metadata.base_timestamp);
    _metadata.last_timestamp = std::max(
      batch.header().max_timestamp, _metadata.last_timestamp);
    _metadata.last_offset = model::offset_cast(batch.last_offset());

    if (!_metadata.terms.contains(batch.term())) {
        _metadata.terms.insert(
          std::make_pair(
            batch.term(), model::offset_cast(batch.base_offset())));
    }

    co_await _builder->add_batch(std::move(batch));

    co_return ss::stop_iteration::no;
}

std::optional<partition_metadata> reconciliation_consumer::end_of_stream() {
    if (_metadata.base_offset != kafka::offset::min()) {
        return _metadata;
    }
    return std::nullopt;
}

} // namespace cloud_topics::reconciler
