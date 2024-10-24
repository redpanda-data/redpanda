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

#include "base/seastarx.h"

#include <seastar/core/sstring.hh>

namespace config {
struct validation_error {
    validation_error(ss::sstring name, ss::sstring error_msg)
      : _name(std::move(name))
      , _error_msg(std::move(error_msg)) {}

    ss::sstring name() { return _name; }

    ss::sstring error_message() { return _error_msg; }

private:
    ss::sstring _name;
    ss::sstring _error_msg;
};
}; // namespace config
