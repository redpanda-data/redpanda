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
#pragma once

#include "container/fragmented_vector.h"
#include "json/json.h"

namespace json {

template<typename Buffer, typename T, size_t max_fragment_size>
void rjson_serialize(
  json::Writer<Buffer>& w, const fragmented_vector<T, max_fragment_size>& v) {
    w.StartArray();
    for (const auto& e : v) {
        rjson_serialize(w, e);
    }
    w.EndArray();
}

} // namespace json
