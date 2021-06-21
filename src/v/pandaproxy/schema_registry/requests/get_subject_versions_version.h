/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "outcome.h"
#include "pandaproxy/json/iobuf.h"
#include "pandaproxy/json/rjson_parse.h"
#include "pandaproxy/json/rjson_util.h"
#include "pandaproxy/schema_registry/error.h"
#include "pandaproxy/schema_registry/types.h"
#include "seastarx.h"
#include "utils/string_switch.h"

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <atomic>
#include <cstdint>

namespace pandaproxy::schema_registry {

struct post_subject_versions_version_response {
    subject sub;
    schema_id id;
    schema_version version;
    schema_definition definition;
};

inline void rjson_serialize(
  rapidjson::Writer<rapidjson::StringBuffer>& w,
  const post_subject_versions_version_response& res) {
    w.StartObject();
    w.Key("name");
    ::json::rjson_serialize(w, res.sub);
    w.Key("id");
    ::json::rjson_serialize(w, res.id);
    w.Key("version");
    ::json::rjson_serialize(w, res.version);
    w.Key("schema");
    ::json::rjson_serialize(w, res.definition);
    w.EndObject();
}

} // namespace pandaproxy::schema_registry
