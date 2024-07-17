/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "json/iobuf_writer.h"
#include "pandaproxy/json/rjson_util.h"
#include "pandaproxy/schema_registry/types.h"

namespace pandaproxy::schema_registry {

struct post_subject_versions_version_response {
    canonical_schema schema;
    schema_id id;
    schema_version version;
};

template<typename Buffer>
void rjson_serialize(
  ::json::iobuf_writer<Buffer>& w,
  const post_subject_versions_version_response& res) {
    w.StartObject();
    w.Key("subject");
    ::json::rjson_serialize(w, res.schema.sub());
    w.Key("version");
    ::json::rjson_serialize(w, res.version);
    w.Key("id");
    ::json::rjson_serialize(w, res.id);
    auto type = res.schema.type();
    if (type != schema_type::avro) {
        w.Key("schemaType");
        ::json::rjson_serialize(w, to_string_view(type));
    }
    if (!res.schema.def().refs().empty()) {
        w.Key("references");
        w.StartArray();
        for (const auto& ref : res.schema.def().refs()) {
            w.StartObject();
            w.Key("name");
            ::json::rjson_serialize(w, ref.name);
            w.Key("subject");
            ::json::rjson_serialize(w, ref.sub);
            w.Key("version");
            ::json::rjson_serialize(w, ref.version);
            w.EndObject();
        }
        w.EndArray();
    }
    w.Key("schema");
    ::json::rjson_serialize(w, res.schema.def().raw());
    w.EndObject();
}

} // namespace pandaproxy::schema_registry
