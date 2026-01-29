/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_zero/batcher/aggregator.h"

#include "cloud_topics/level_zero/pipeline/serializer.h"
#include "cloud_topics/level_zero/pipeline/write_request.h"
#include "serde/rw/rw.h"

#include <seastar/core/smp.hh>
#include <seastar/util/defer.hh>

namespace cloud_topics::l0 {

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
struct prepared_extents {
    object_id id;
    chunked_vector<std::unique_ptr<extents_for_req<Clock>>> placeholders;
    uint64_t size_bytes{0};
};

namespace {
/// Convert multiple chunk elements into placeholder batches
///
/// Byte offsets in the chunk are zero based. Because we're
/// concatenating multiple chunks the offset has to be corrected.
/// This is done using the `base_byte_offset` parameter.
template<class Clock>
void make_ctp_placeholders(
  prepared_extents<Clock>& ctx,
  l0::write_request<Clock>& req,
  const l0::serialized_chunk& chunk) {
    auto result = std::make_unique<extents_for_req<Clock>>();
    for (const auto& b : chunk.extents) {
        extent_meta placeholder{
          .id = ctx.id,
          .first_byte_offset = first_byte_offset_t(ctx.size_bytes),
          .byte_range_size = byte_range_size_t(b.byte_range_size),
          .base_offset = b.base_offset,
          .last_offset = b.last_offset,
        };

        result->extents.emplace_back(placeholder);
        ctx.size_bytes += b.byte_range_size();
    }
    result->ref = req.weak_from_this();
    ctx.placeholders.push_back(std::move(result));
}
} // namespace

template<class Clock>
chunked_vector<std::unique_ptr<extents_for_req<Clock>>>
aggregator<Clock>::get_extents(object_id id) {
    prepared_extents<Clock> ctx{
      .id = id,
    };
    for (auto& [key, list] : _staging) {
        for (auto& req : list) {
            vassert(
              !req.data_chunk.payload.empty(),
              "Empty write request for topic_id_partition: {}",
              key);
            make_ctp_placeholders(ctx, req, req.data_chunk);
        }
    }
    return std::move(ctx.placeholders);
}

template<class Clock>
iobuf aggregator<Clock>::get_stream(l0::footer& footer_out) {
    iobuf concat;
    size_t current_offset = 0;
    std::optional<model::topic_id_partition> current_tp;
    size_t tp_start_offset = 0;

    for (auto& p : _aggregated) {
        if (p->ref != nullptr) {
            model::topic_id_partition req_tp(
              p->ref->topic_id, p->ref->ntp.tp.partition);

            // Detect when we switch to a new partition
            if (!current_tp || req_tp != *current_tp) {
                // Record the previous partition's info (if any)
                if (current_tp) {
                    footer_out.partitions[*current_tp] = {
                      .file_position = tp_start_offset,
                      .length = current_offset - tp_start_offset,
                    };
                }
                current_tp = req_tp;
                tp_start_offset = current_offset;
            }

            auto& payload = p->ref->data_chunk.payload;
            current_offset += payload.size_bytes();
            concat.append(std::move(payload));
        }
    }

    // Record the final partition's info
    if (current_tp) {
        footer_out.partitions[*current_tp] = {
          .file_position = tp_start_offset,
          .length = current_offset - tp_start_offset,
        };
    }

    // Serialize the footer and append it to the payload
    iobuf footer_buf = serde::to_iobuf(footer_out.copy());
    auto footer_size = static_cast<uint32_t>(footer_buf.size_bytes());
    concat.append(std::move(footer_buf));

    // Append the footer size (4 bytes, little-endian) for tail-reading
    auto footer_size_le = ss::cpu_to_le(footer_size);
    concat.append(
      reinterpret_cast<const char*>(&footer_size_le), sizeof(footer_size_le));

    return concat;
}

template<class Clock>
typename aggregator<Clock>::L0_object aggregator<Clock>::prepare(object_id id) {
    // Move data from staging to aggregated
    _aggregated = get_extents(id);
    _staging.clear();
    // Produce input stream with footer
    l0::footer index;
    auto payload = get_stream(index);
    return L0_object{id, std::move(payload), std::move(index)};
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
                p->ref->set_value(std::move(p->extents));
            } catch (const ss::broken_promise& e) {
                std::ignore = e;
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
            } catch (const ss::broken_promise& e) {
                std::ignore = e;
            }
        }
    }
}

template<class Clock>
void aggregator<Clock>::add(l0::write_request<Clock>& req) {
    model::topic_id_partition tp(req.topic_id, req.ntp.tp.partition);
    auto it = _staging.find(tp);
    if (it == _staging.end()) {
        it = _staging.emplace_hint(it, tp, l0::write_request_list<Clock>());
    }
    req._hook.unlink();
    it->second.push_back(req);
    _size_bytes += req.size_bytes();
    _highest_topic_start_epoch = std::max(
      _highest_topic_start_epoch, req.topic_start_epoch);
}

template<class Clock>
size_t aggregator<Clock>::size_bytes() const noexcept {
    return _size_bytes;
}

template class aggregator<ss::lowres_clock>;
template class aggregator<ss::manual_clock>;
} // namespace cloud_topics::l0
