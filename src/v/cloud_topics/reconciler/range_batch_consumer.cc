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

namespace experimental::cloud_topics::reconciler {

ss::future<ss::stop_iteration>
range_batch_consumer::operator()(model::record_batch batch) {
    if (!_base_offset.has_value()) {
        _base_offset = batch.base_offset();
    }
    _range.info.last_offset = batch.last_offset();

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
