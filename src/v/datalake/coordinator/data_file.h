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

#include "base/seastarx.h"
#include "serde/envelope.h"

#include <seastar/core/sstring.hh>

#include <fmt/core.h>

namespace datalake::coordinator {

// Represents a file that exists in object storage.
struct data_file
  : serde::envelope<data_file, serde::version<0>, serde::compat_version<0>> {
    auto serde_fields() {
        return std::tie(remote_path, row_count, file_size_bytes, hour);
    }
    ss::sstring remote_path = "";
    size_t row_count = 0;
    size_t file_size_bytes = 0;
    int hour = 0;
    // TODO: add kafka schema id
};

std::ostream& operator<<(std::ostream& o, const data_file& f);

} // namespace datalake::coordinator
