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

class initialization_error : public std::runtime_error {
public:
    explicit initialization_error(const std::string& what_arg)
      : std::runtime_error(what_arg) {}
};

} // namespace datalake
