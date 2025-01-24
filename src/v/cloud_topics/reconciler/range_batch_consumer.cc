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
#include "cloud_topics/reconciler/range_batch_consumer.h"

#include "model/timestamp.h"

namespace experimental::cloud_topics::reconciler {

ss::future<ss::stop_iteration>
range_batch_consumer::operator()(model::record_batch batch) {
    if (!_base_offset.has_value()) {
        _base_offset = model::offset_cast(batch.base_offset());
    }
    // NOTE: we only have data batches here so it's safe
    // to use timestamps without checking the batch type
    if (_range.info.base_timestamp == model::timestamp{}) {
        _range.info.base_timestamp = batch.header().first_timestamp;
    } else {
        _range.info.base_timestamp = std::min(
          batch.header().first_timestamp, _range.info.base_timestamp);
    }
    _range.info.last_timestamp = std::max(
      batch.header().max_timestamp, _range.info.last_timestamp);
    _range.info.last_offset = model::offset_cast(batch.last_offset());

    bool add_term = false;
    if (_range.info.terms.empty()) {
        // Always add term of the first batch in the sequence. We don't really
        // know if it's the first batch in the term so some filtering will be
        // needed later.
        add_term = true;
    } else {
        auto term = _range.info.terms.rbegin()->first;
        if (term != batch.term()) {
            add_term = true;
        }
    }
    if (add_term) {
        _range.info.terms.insert(std::make_pair(
          batch.term(), model::offset_cast(batch.base_offset())));
    }

    auto data = serde::to_iobuf(std::move(batch));
    _range.data.append(std::move(data));

    co_return ss::stop_iteration::no;
}

std::optional<range> range_batch_consumer::end_of_stream() {
    if (_base_offset.has_value()) {
        _range.info.base_offset = _base_offset.value();
        return std::move(_range);
    }
    return std::nullopt;
}

} // namespace experimental::cloud_topics::reconciler
