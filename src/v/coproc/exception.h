/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "coproc/types.h"

#include <seastar/core/sstring.hh>

#include <exception>
#include <seastarx.h>

namespace coproc {
/// Root exception type in v/coproc
class exception : public std::exception {
public:
    explicit exception(ss::sstring msg) noexcept
      : _msg(std::move(msg)) {}

    const char* what() const noexcept override { return _msg.c_str(); }

private:
    ss::sstring _msg;
};

/// Not necessarily a fatal error, explicity handle these events as theres no
/// way to ensure handles to partitions aren't in use before
/// partition::shutdown() is called
class partition_shutdown_exception final : public exception {
public:
    partition_shutdown_exception(model::ntp ntp, ss::sstring msg) noexcept
      : exception(std::move(msg))
      , _ntp(std::move(ntp)) {}

    const model::ntp& ntp() const { return _ntp; }

private:
    model::ntp _ntp;
};

/// Root exception type for classes of exceptions that are only thrown by
/// actions interpreted by coprocessors themselves
class script_exception : public exception {
public:
    script_exception(script_id id, ss::sstring msg) noexcept
      : exception(std::move(msg))
      , _id(id) {}

    script_id get_id() const { return _id; }

private:
    script_id _id;
};

/// brief Thrown when the wasm engine returns a malformed response
class engine_protocol_failure : public script_exception {
    using script_exception::script_exception;
};

/// \brief Thrown when a coprocessor running within nodejs fails for whatever
/// reason
class script_failed_exception final : public script_exception {
    using script_exception::script_exception;
};

/// \brief Thrown when a coprocessor performs an action that is explicity
/// disallowed such as producing onto a normal topic
class script_illegal_action_exception final : public script_exception {
    using script_exception::script_exception;
};

} // namespace coproc
