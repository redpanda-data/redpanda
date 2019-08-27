#pragma once
#include <fmt/format.h>

smf::rpc_server_args rpc_config(const config::configuration& c) {
    auto args = smf::rpc_server_args();
    std::ostringstream o;
    o << c.rpc_server().addr();
    args.ip = o.str();
    args.rpc_port = c.rpc_server().port();
    return args;
}
