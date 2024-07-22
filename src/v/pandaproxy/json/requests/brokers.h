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

#include "json/json.h"
#include "json/writer.h"
#include "model/fundamental.h"

namespace pandaproxy::json {

struct get_brokers_res {
    std::vector<model::node_id> ids;
};

template<typename Buffer>
void rjson_serialize(
  ::json::Writer<Buffer>& w, const get_brokers_res& brokers) {
    w.StartObject();
    w.Key("brokers");
    ::json::rjson_serialize(w, brokers.ids);
    w.EndObject();
}

} // namespace pandaproxy::json
