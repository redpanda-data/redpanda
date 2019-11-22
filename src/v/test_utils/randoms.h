#pragma once
#include "model/metadata.h"
#include "random/generators.h"

#include <seastar/net/inet_address.hh>
#include <seastar/net/ip.hh>
#include <bits/stdint-uintn.h>

namespace tests {

inline socket_address random_socket_address() {
    
    return socket_address(
      net::ipv4_address(random_generators::get_int<uint32_t>()),
      random_generators::get_int(1025, 65535));
}

inline model::broker
random_broker(int32_t id_low_bound, int32_t id_upper_bound) {
    return model::broker(
      model::node_id(
        random_generators::get_int(id_low_bound, id_upper_bound)), // id
      random_socket_address(), // kafka api address
      random_socket_address(), // rpc address
      std::nullopt,
      model::broker_properties{
        .cores = random_generators::get_int<uint32_t>(96)});
}

} // namespace tests