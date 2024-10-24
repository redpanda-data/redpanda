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

#include <exception>

namespace rpc {
class request_timeout_exception final : std::exception {
public:
    explicit request_timeout_exception(ss::sstring what)
      : what_(std::move(what)) {}

    const char* what() const noexcept final { return what_.c_str(); }

private:
    ss::sstring what_;
};
} // namespace rpc
