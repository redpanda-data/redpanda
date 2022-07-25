// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "index_state_compat.h"

#include "compat/json_helpers.h"
#include "compat/verify.h"
#include "json/writer.h"
#include "random/generators.h"
#include "serde/serde.h"
#include "storage/index_state.h"

#include <ostream>
#include <string_view>

void index_state_compat::random_init() {
    bitflags = random_generators::get_int<uint32_t>();
    base_offset = model::offset(random_generators::get_int<int64_t>());
    max_offset = model::offset(random_generators::get_int<int64_t>());
    base_timestamp = model::timestamp(random_generators::get_int<int64_t>());
    max_timestamp = model::timestamp(random_generators::get_int<int64_t>());

    const auto n = random_generators::get_int(1, 1000);
    for (auto i = 0; i < n; ++i) {
        add_entry(
          random_generators::get_int<uint32_t>(),
          random_generators::get_int<uint32_t>(),
          random_generators::get_int<uint64_t>());
    }
}

void index_state_compat::json_init(rapidjson::Value& w) {
    verify(w.IsObject(), "expected JSON object", w.GetType());
    json::read_member(w, "bitflags", bitflags);
    json::read_member(w, "base_offset", base_offset);
    json::read_member(w, "max_offset", max_offset);
    json::read_member(w, "base_timestamp", base_timestamp);
    json::read_member(w, "max_timestamp", max_timestamp);
    json::read_member(w, "relative_offset_index", relative_offset_index);
    json::read_member(w, "relative_time_index", relative_time_index);
    json::read_member(w, "position_index", position_index);
}

iobuf index_state_compat::to_json() const {
    auto sb = json::StringBuffer{};
    auto w = json::Writer<json::StringBuffer>{sb};

    w.StartObject();
    json::write_member(w, "bitflags", bitflags);
    json::write_member(w, "base_offset", base_offset);
    json::write_member(w, "max_offset", max_offset);
    json::write_member(w, "base_timestamp", base_timestamp.value());
    json::write_member(w, "max_timestamp", max_timestamp.value());
    json::write_member(w, "relative_offset_index", relative_offset_index);
    json::write_member(w, "relative_time_index", relative_time_index);
    json::write_member(w, "position_index", position_index);
    w.EndObject();

    iobuf s;
    s.append(sb.GetString(), sb.GetLength());
    return s;
}

iobuf index_state_compat::to_binary() { return serde::to_iobuf(copy()); }

bool index_state_compat::check_compatibility(iobuf&& binary) const {
    verify_equal(
      *static_cast<storage::index_state const*>(this),
      serde::from_iobuf<storage::index_state>(std::move(binary)));
    return true;
}
