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

struct post_compatibility_res {
    bool is_compat{false};
};

inline void rjson_serialize(
  ::json::Writer<::json::StringBuffer>& w,
  const schema_registry::post_compatibility_res& res) {
    w.StartObject();
    w.Key("is_compatible");
    ::json::rjson_serialize(w, res.is_compat);
    w.EndObject();
}

} // namespace pandaproxy::schema_registry
