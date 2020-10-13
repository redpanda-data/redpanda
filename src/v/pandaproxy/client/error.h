#pragma once

#include "kafka/errors.h"
#include "model/fundamental.h"
#include "model/metadata.h"

#include <fmt/core.h>

#include <exception>

namespace pandaproxy::client {

struct broker_error final : std::exception {
    broker_error(model::node_id n_id, kafka::error_code err)
      : std::exception{}
      , msg{fmt::format("{{ node: {} }}, {}", n_id, err)}
      , node_id{n_id}
      , error{err} {}
    const char* what() const noexcept final { return msg.c_str(); }
    std::string msg;
    model::node_id node_id;
    kafka::error_code error;
};

} // namespace pandaproxy::client
