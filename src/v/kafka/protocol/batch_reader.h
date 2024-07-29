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

#include "bytes/iobuf.h"
#include "kafka/protocol/kafka_batch_adapter.h"
#include "model/fundamental.h"

#include <optional>

namespace kafka {

///\brief batch_reader owns an iobuf that's a concatenation of
/// model::record_batch on the wire
///
/// The array bound is not serialized, c.f. array<kafka::thing>
class batch_reader final : public model::record_batch_reader::impl {
    using storage_t = model::record_batch_reader::storage_t;

public:
    batch_reader() = default;

    explicit batch_reader(iobuf buf)
      : _buf(std::move(buf)) {}

    bool empty() const { return _buf.empty(); }

    size_t size_bytes() const { return _buf.size_bytes(); }

    // Obtain the offset of the last record in the last batch
    //
    // Complexity: O(n) record_batches
    model::offset last_offset() const;

    // Consume a model::record_batch as a kafka_batch_adaptor
    kafka_batch_adapter consume_batch();

    // Implements model::record_batch_reader::impl
    bool is_end_of_stream() const final {
        return _do_load_slice_failed || empty();
    }

    // Implements model::record_batch_reader::impl
    ss::future<storage_t> do_load_slice(model::timeout_clock::time_point) final;

    // Implements model::record_batch_reader::impl
    // NOTE: this stream is intentially devoid of user data.
    void print(std::ostream& os) final { os << "{kafka::consumer_records}"; }

    // Release any remaining iobuf that hasn't been consumed
    iobuf release() && { return std::move(_buf); }

    friend std::ostream&
    operator<<(std::ostream& os, const batch_reader& reader) {
        // NOTE: this stream is intentially devoid of user data.
        fmt::print(os, "{{size {}}}", reader.size_bytes());
        return os;
    }

private:
    iobuf _buf;
    bool _do_load_slice_failed{false};
};

} // namespace kafka
