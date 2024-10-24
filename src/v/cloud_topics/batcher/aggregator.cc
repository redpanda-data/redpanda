/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/batcher/aggregator.h"

#include "cloud_topics/batcher/serializer.h"
#include "cloud_topics/batcher/write_request.h"
#include "cloud_topics/dl_placeholder.h"
#include "storage/record_batch_builder.h"

#include <seastar/core/future.hh>
#include <seastar/util/defer.hh>

namespace experimental::cloud_topics::details {

template<class Clock>
aggregator<Clock>::aggregator(object_id id)
  : _id(id) {}

template<class Clock>
aggregator<Clock>::~aggregator() {
    ack_error(errc::timeout);
    if (!_staging.empty()) {
        for (auto& [key, list] : _staging) {
            std::ignore = key;
            for (auto& req : list) {
                req.set_value(errc::timeout);
            }
        }
    }
}

template<class Clock>
struct prepared_placeholder_batches {
    const object_id id;
    chunked_vector<std::unique_ptr<batches_for_req<Clock>>> placeholders;
    uint64_t size_bytes{0};
};

namespace {
/// Convert multiple chunk elements into placeholder batches
///
/// Byte offsets in the chunk are zero based. Because we're
/// concatenating multiple chunks the offset has to be corrected.
/// This is done using the `base_byte_offset` parameter.
template<class Clock>
void make_dl_placeholder_batches(
  prepared_placeholder_batches<Clock>& ctx,
  write_request<Clock>& req,
  const serialized_chunk& chunk) {
    auto result = std::make_unique<batches_for_req<Clock>>();
    for (const auto& b : chunk.batches) {
        dl_placeholder placeholder{
          .id = ctx.id,
          .offset = first_byte_offset_t(ctx.size_bytes),
          .size_bytes = byte_range_size_t(b.size_bytes),
        };

        storage::record_batch_builder builder(
          model::record_batch_type::dl_placeholder, b.base);

        // TX data (producer id, control flag) are not copied from 'src' yet.

        // Put the payload
        builder.add_raw_kv(
          serde::to_iobuf(dl_placeholder_record_key::payload),
          serde::to_iobuf(placeholder));

        // TODO: fix this
        for (int i = 1; i < b.num_records - 1; i++) {
            iobuf empty;
            builder.add_raw_kv(
              serde::to_iobuf(dl_placeholder_record_key::empty),
              std::move(empty));
        }
        result->placeholders.push_back(std::move(builder).build());
        ctx.size_bytes += b.size_bytes;
    }
    result->ref = req.weak_from_this();
    ctx.placeholders.push_back(std::move(result));
}
} // namespace

template<class Clock>
chunked_vector<std::unique_ptr<batches_for_req<Clock>>>
aggregator<Clock>::get_placeholders() {
    prepared_placeholder_batches<Clock> ctx{
      .id = _id,
    };
    for (auto& [key, list] : _staging) {
        for (auto& req : list) {
            vassert(
              !req.data_chunk.payload.empty(),
              "Empty write request for ntp: {}",
              key);
            make_dl_placeholder_batches(ctx, req, req.data_chunk);
        }
    }
    return std::move(ctx.placeholders);
}

template<class Clock>
iobuf aggregator<Clock>::get_stream() {
    iobuf concat;
    for (auto& p : _aggregated) {
        if (p->ref != nullptr) {
            concat.append(std::move(p->ref->data_chunk.payload));
        }
    }
    return concat;
}

template<class Clock>
object_id aggregator<Clock>::get_object_id() const noexcept {
    return _id;
}

template<class Clock>
iobuf aggregator<Clock>::prepare() {
    // Move data from staging to aggregated
    _aggregated = get_placeholders();
    _staging.clear();
    // Produce input stream
    return get_stream();
}

template<class Clock>
void aggregator<Clock>::ack() {
    if (_aggregated.empty()) {
        return;
    }
    auto d = ss::defer([this] { _aggregated.clear(); });
    for (auto& p : _aggregated) {
        if (p->ref != nullptr) {
            try {
                p->ref->set_value(std::move(p->placeholders));
            } catch (const ss::broken_promise&) {
            }
        }
    }
}

template<class Clock>
void aggregator<Clock>::ack_error(errc e) {
    if (_aggregated.empty()) {
        return;
    }
    auto d = ss::defer([this] { _aggregated.clear(); });
    for (auto& p : _aggregated) {
        if (p->ref != nullptr) {
            try {
                p->ref->set_value(e);
            } catch (const ss::broken_promise&) {
            }
        }
    }
}

template<class Clock>
void aggregator<Clock>::add(write_request<Clock>& req) {
    auto it = _staging.find(req.ntp);
    if (it == _staging.end()) {
        it = _staging.emplace_hint(it, req.ntp, write_request_list<Clock>());
    }
    req._hook.unlink();
    it->second.push_back(req);
    _size_bytes += req.size_bytes();
}

template<class Clock>
size_t aggregator<Clock>::size_bytes() const noexcept {
    return _size_bytes;
}

template class aggregator<ss::lowres_clock>;
template class aggregator<ss::manual_clock>;
} // namespace experimental::cloud_topics::details
