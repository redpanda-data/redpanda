/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "compat/json.h"
#include "model/record.h"

namespace json {

inline void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const model::producer_identity& v) {
    w.StartObject();
    w.Key("id");
    rjson_serialize(w, v.id);
    w.Key("epoch");
    rjson_serialize(w, v.epoch);
    w.EndObject();
}

inline void read_value(json::Value const& rd, model::producer_identity& obj) {
    read_member(rd, "id", obj.id);
    read_member(rd, "epoch", obj.epoch);
}

} // namespace json
