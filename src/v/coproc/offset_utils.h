/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "bytes/bytes.h"
#include "coproc/ntp_context.h"
#include "model/fundamental.h"
#include "reflection/adl.h"

namespace coproc {

using map_type = ntp_context::offsets_map;

inline bytes ntp_to_bytes(model::ntp ntp) {
    iobuf buf;
    reflection::serialize(buf, std::move(ntp));
    return iobuf_to_bytes(buf);
}

inline iobuf offsets_map_to_iobuf(const map_type& offsets) {
    iobuf r;
    reflection::serialize(r, offsets.size());
    for (const auto& e : offsets) {
        reflection::serialize(
          r, e.first, e.second.last_read, e.second.last_acked);
    }
    return r;
}

inline map_type offsets_map_from_iobuf(iobuf&& buf) {
    map_type offsets;
    iobuf_parser p(std::move(buf));
    auto size = reflection::adl<map_type::size_type>{}.from(p);
    for (auto i = 0; i < size; ++i) {
        auto id = reflection::adl<script_id>{}.from(p);
        auto last_read = reflection::adl<model::offset>{}.from(p);
        auto last_acked = reflection::adl<model::offset>{}.from(p);
        offsets.emplace(
          id,
          ntp_context::offset_pair{
            .last_read = last_read, .last_acked = last_acked});
    }
    return offsets;
}

} // namespace coproc
