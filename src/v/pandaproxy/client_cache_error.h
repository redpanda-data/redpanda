/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include <stdexcept>
#include <string>

namespace pandaproxy {

struct client_cache_error : std::exception {
    client_cache_error(std::string_view msg)
      : std::exception{}
      , msg{msg} {}
    const char* what() const noexcept final { return msg.c_str(); }
    std::string msg;
};

} // namespace pandaproxy
