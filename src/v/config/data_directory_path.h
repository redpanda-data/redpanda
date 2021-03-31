/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "seastarx.h"

#include <seastar/core/sstring.hh>

#include <cstdlib>
#include <filesystem>

namespace config {

struct data_directory_path {
    std::filesystem::path path;
    ss::sstring as_sstring() const { return path.string(); }

    friend bool
    operator==(const data_directory_path&, const data_directory_path&)
      = default;
};

} // namespace config
