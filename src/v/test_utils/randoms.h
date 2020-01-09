#pragma once
#include "model/metadata.h"
#include "random/generators.h"
#include "utils/unresolved_address.h"

#include <seastar/net/inet_address.hh>
#include <seastar/net/ip.hh>

#include <bits/stdint-uintn.h>

namespace tests {

inline unresolved_address random_net_address() {
    return unresolved_address(
      random_generators::gen_alphanum_string(8),
      random_generators::get_int(1025, 65535));
}

inline model::broker
random_broker(int32_t id_low_bound, int32_t id_upper_bound) {
    return model::broker(
      model::node_id(
        random_generators::get_int(id_low_bound, id_upper_bound)), // id
      random_net_address(), // kafka api address
      random_net_address(), // rpc address
      std::nullopt,
      model::broker_properties{
        .cores = random_generators::get_int<uint32_t>(96)});
}

} // namespace tests