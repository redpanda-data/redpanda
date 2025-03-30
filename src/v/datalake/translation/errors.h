/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include <stdexcept>

namespace datalake::translation {

class translator_out_of_memory_error final : public std::runtime_error {
public:
    explicit translator_out_of_memory_error()
      : std::runtime_error("translator_out_of_memory") {}
};

class translator_shutdown_error final : public std::runtime_error {
public:
    explicit translator_shutdown_error()
      : std::runtime_error("translator_shutdown") {}
};

class translator_time_quota_exceeded_error final : public std::runtime_error {
public:
    explicit translator_time_quota_exceeded_error()
      : std::runtime_error("translator_time_quota_exceeded") {}
};

class translator_out_of_disk_error final : public std::runtime_error {
public:
    explicit translator_out_of_disk_error()
      : std::runtime_error("translator_out_of_disk") {}
};
} // namespace datalake::translation
