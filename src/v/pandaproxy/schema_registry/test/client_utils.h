// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "http/client.h"
#include "pandaproxy/json/exceptions.h"
#include "pandaproxy/json/types.h"
#include "pandaproxy/schema_registry/types.h"
#include "pandaproxy/test/utils.h"

#include <absl/algorithm/container.h>
#include <boost/beast/http/status.hpp>
#include <boost/beast/http/verb.hpp>

#include <iterator>

namespace pp = pandaproxy;
namespace ppj = pp::json;
namespace pps = pp::schema_registry;

inline iobuf make_body(const ss::sstring& body) {
    iobuf buf;
    buf.append(body.data(), body.size());
    return buf;
}

inline auto post_schema(
  http::client& client, const pps::subject& sub, const ss::sstring& payload) {
    return http_request(
      client,
      fmt::format("/subjects/{}/versions", sub()),
      make_body(payload),
      boost::beast::http::verb::post,
      ppj::serialization_format::schema_registry_v1_json,
      ppj::serialization_format::schema_registry_v1_json);
}
