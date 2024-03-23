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

#include "net/unresolved_address.h"
#include "random/generators.h"

namespace tests {

inline net::unresolved_address random_net_address() {
    return net::unresolved_address(
      random_generators::gen_alphanum_string(
        random_generators::get_int(1, 100)),
      random_generators::get_int(1025, 65535));
}

} // namespace tests
