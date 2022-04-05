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

#include "pandaproxy/json/rjson_util.h"
#include "pandaproxy/schema_registry/types.h"

namespace pandaproxy::schema_registry {

struct get_schemas_ids_id_versions_response {
    std::vector<subject_version> subject_versions;
};

inline void rjson_serialize(
  ::json::Writer<::json::StringBuffer>& w,
  const get_schemas_ids_id_versions_response& res) {
    w.StartArray();
    for (const auto& sv : res.subject_versions) {
        w.StartObject();
        w.Key("subject");
        ::json::rjson_serialize(w, sv.sub);
        w.Key("version");
        ::json::rjson_serialize(w, sv.version);
        w.EndObject();
    }
    w.EndArray();
}

} // namespace pandaproxy::schema_registry
