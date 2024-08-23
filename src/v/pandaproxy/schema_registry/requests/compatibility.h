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
    std::vector<ss::sstring> messages;

    // `is_verbose` is not rendered into the response but `messages` are
    // conditionally rendered based on `is_verbose`
    verbose is_verbose;
};

template<typename Buffer>
void rjson_serialize(
  ::json::Writer<Buffer>& w,
  const schema_registry::post_compatibility_res& res) {
    w.StartObject();
    w.Key("is_compatible");
    ::json::rjson_serialize(w, res.is_compat);
    if (res.is_verbose) {
        w.Key("messages");
        ::json::rjson_serialize(w, res.messages);
    }
    w.EndObject();
}

} // namespace pandaproxy::schema_registry
