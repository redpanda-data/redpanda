/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "container/json.h"
#include "pandaproxy/json/rjson_util.h"
#include "pandaproxy/schema_registry/types.h"

namespace json {

template<typename Buffer>
void rjson_serialize(
  json::iobuf_writer<Buffer>& w,
  const pandaproxy::schema_registry::schema_definition::raw_string& def) {
    w.String(def());
}

template<typename Writer>
void rjson_serialize(
  Writer& w, const pandaproxy::schema_registry::schema_reference& ref) {
    w.StartObject();
    w.Key("name");
    ::json::rjson_serialize(w, ref.name);
    w.Key("subject");
    w.String(ref.sub.to_string());
    w.Key("version");
    ::json::rjson_serialize(w, ref.version);
    w.EndObject();
}

template<typename Writer>
void rjson_serialize(
  Writer& w,
  const std::optional<pandaproxy::schema_registry::schema_metadata>& meta) {
    if (meta.has_value()) {
        w.Key("metadata");
        w.StartObject();
        if (meta->properties.has_value()) {
            w.Key("properties");
            w.StartObject();
            for (const auto& [k, v] : meta->properties.value()) {
                w.Key(k);
                ::json::rjson_serialize(w, v);
            }
            w.EndObject();
        }
        w.EndObject();
    }
}

} // namespace json
