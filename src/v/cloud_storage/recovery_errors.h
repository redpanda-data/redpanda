/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "seastarx.h"

#include <seastar/core/sstring.hh>

#include <system_error>

namespace cloud_storage {

enum class recovery_error_code {
    success,
    invalid_client_config,
    error_listing_items,
    recovery_already_running,
    error_downloading_manifest,
    error_creating_topics,
    unknown_error,
};

class recovery_error_category : public std::error_category {
public:
    const char* name() const noexcept final;
    std::string message(int c) const final;
};

const std::error_category& error_category() noexcept;

std::error_code make_error_code(recovery_error_code e) noexcept;

/// \brief Provides a wrapper for an error encountered during recovery attached
/// with errors from other subsystems. Since automated recovery is an operation
/// spread across different domains such as making RPC calls, rest API calls,
/// creating topics and changing topic settings, different types of errors can
/// be encountered during the process. The context message contains a
/// description of the original errors and the error code denotes the stage of
/// recovery the error occured in.
struct recovery_error_ctx {
    recovery_error_code error_code;
    ss::sstring context;
    static recovery_error_ctx make(
      ss::sstring context,
      recovery_error_code error_code = recovery_error_code::unknown_error);
};

recovery_error_ctx make_error_ctx(
  ss::sstring context,
  recovery_error_code error_code = recovery_error_code::unknown_error);

std::error_code make_error_code(const recovery_error_ctx& r) noexcept;

void outcome_throw_as_system_error_with_payload(recovery_error_ctx r);

} // namespace cloud_storage

namespace std {
template<>
struct is_error_code_enum<cloud_storage::recovery_error_code> : true_type {};
} // namespace std
