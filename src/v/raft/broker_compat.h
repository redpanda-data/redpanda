/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "model/adl_serde.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "reflection/adl.h"
#include "utils/unresolved_address.h"

namespace model::internal {
/*
 * Old version for use in backwards compatibility serialization /
 * deserialization helpers.
 */
struct broker_v0 {
    model::node_id id;
    net::unresolved_address kafka_address;
    net::unresolved_address rpc_address;
    std::optional<rack_id> rack;
    model::broker_properties properties;

    model::broker to_v3() const {
        return model::broker(id, kafka_address, rpc_address, rack, properties);
    }
};

} // namespace model::internal

namespace reflection {

template<>
struct adl<model::internal::broker_v0> {
    void to(iobuf& out, model::internal::broker_v0&& r) {
        adl<model::node_id>{}.to(out, r.id);
        adl<net::unresolved_address>{}.to(out, r.kafka_address);
        adl<net::unresolved_address>{}.to(out, r.rpc_address);
        adl<std::optional<model::rack_id>>{}.to(out, r.rack);
        adl<model::broker_properties>{}.to(out, r.properties);
    }

    model::internal::broker_v0 from(iobuf_parser& in) {
        auto id = adl<model::node_id>{}.from(in);
        auto kafka_adrs = adl<net::unresolved_address>{}.from(in);
        auto rpc_adrs = adl<net::unresolved_address>{}.from(in);
        auto rack = adl<std::optional<model::rack_id>>{}.from(in);
        auto etc_props = adl<model::broker_properties>{}.from(in);
        return model::internal::broker_v0{
          id, kafka_adrs, rpc_adrs, rack, etc_props};
    }
};

} // namespace reflection
