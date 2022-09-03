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
#include "storage/types.h"

namespace json {

inline void read_value(json::Value const& rd, storage::disk& obj) {
    read_member(rd, "path", obj.path);
    read_member(rd, "free", obj.free);
    read_member(rd, "total", obj.total);
}

inline void
rjson_serialize(json::Writer<json::StringBuffer>& w, const storage::disk& d) {
    w.StartObject();
    write_member(w, "path", d.path);
    write_member(w, "free", d.free);
    write_member(w, "total", d.total);
    w.EndObject();
}

} // namespace json
