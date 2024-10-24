/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/seastarx.h"
#include "json/json.h"
#include "json/writer.h"

#include <seastar/core/sstring.hh>
#include <seastar/http/reply.hh>

namespace pandaproxy::json {

struct error_body {
    std::error_condition ec;
    ss::sstring message;
};

template<typename Buffer>
void rjson_serialize(::json::Writer<Buffer>& w, const error_body& v) {
    w.StartObject();
    w.Key("error_code");
    ::json::rjson_serialize(w, v.ec.value());
    w.Key("message");
    ::json::rjson_serialize(w, v.message);
    w.EndObject();
}

} // namespace pandaproxy::json
