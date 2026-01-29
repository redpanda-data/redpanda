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

struct get_schemas_ids_id_response {
    schema_definition definition;
};

template<typename Buffer>
void rjson_serialize(
  ::json::iobuf_writer<Buffer>& w, const get_schemas_ids_id_response& res) {
    w.StartObject();
    if (res.definition.type() != schema_type::avro) {
        w.Key("schemaType");
        ::json::rjson_serialize(w, to_string_view(res.definition.type()));
    }
    w.Key("schema");
    ::json::rjson_serialize(w, res.definition.raw());
    if (!res.definition.refs().empty()) {
        w.Key("references");
        ::json::rjson_serialize(w, res.definition.refs());
    }
    ::json::rjson_serialize(w, res.definition.meta());
    w.EndObject();
}

} // namespace pandaproxy::schema_registry
