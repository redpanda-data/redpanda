/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include <stdexcept>
namespace datalake {

enum class coordinator_errc : int16_t {
    ok,
    coordinator_topic_not_exists,
    not_leader,
    timeout,
    fenced,
    stale,
    concurrent_requests,
};

// TODO: Make an std::error_category instance for this
enum class arrow_converter_status {
    ok,

    // User errors
    parse_error,

    // System Errors
    internal_error,
};

class initialization_error : public std::runtime_error {
public:
    explicit initialization_error(const std::string& what_arg)
      : std::runtime_error(what_arg) {}
};

} // namespace datalake
