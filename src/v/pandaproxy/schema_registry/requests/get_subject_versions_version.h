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
#include "pandaproxy/schema_registry/rjson.h"

namespace pandaproxy::schema_registry {

struct get_subject_versions_version_response {
    stored_schema stored_schema;
};

template<typename Buffer>
void rjson_serialize(
  ::json::iobuf_writer<Buffer>& w,
  const get_subject_versions_version_response& res) {
    w.StartObject();
    w.Key("subject");
    w.String(res.stored_schema.schema.sub().to_string());
    w.Key("version");
    ::json::rjson_serialize(w, res.stored_schema.version);
    w.Key("id");
    ::json::rjson_serialize(w, res.stored_schema.id);
    auto type = res.stored_schema.schema.type();
    w.Key("schemaType");
    ::json::rjson_serialize(w, to_string_view(type));
    if (!res.stored_schema.schema.def().refs().empty()) {
        w.Key("references");
        ::json::rjson_serialize(w, res.stored_schema.schema.def().refs());
    }
    ::json::rjson_serialize(w, res.stored_schema.schema.def().meta());
    w.Key("schema");
    ::json::rjson_serialize(w, res.stored_schema.schema.def().raw());
    w.Key("deleted");
    ::json::rjson_serialize(w, bool(res.stored_schema.deleted));
    w.EndObject();
}

} // namespace pandaproxy::schema_registry
