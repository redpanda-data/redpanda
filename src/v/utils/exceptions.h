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

#include "base/seastarx.h"

#include <seastar/core/sstring.hh>

#include <stdexcept>

/// Some objects reference state that changes comparatively rarely (e.g.
/// topic_table state) across yield points and expect these references to remain
/// valid. In case these references are invalidated by a concurrent fiber, this
/// exception is thrown. This is a signal for the caller to restart the
/// computation with up-to-date state.
class concurrent_modification_error : public std::exception {
public:
    explicit concurrent_modification_error(ss::sstring s)
      : _msg(std::move(s)) {}

    const char* what() const noexcept override { return _msg.c_str(); }

private:
    ss::sstring _msg;
};
