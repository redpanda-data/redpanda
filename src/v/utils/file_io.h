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
#include "bytes/iobuf.h"

#include <seastar/core/seastar.hh>
#include <seastar/core/temporary_buffer.hh>

#include <filesystem>

/// \brief Read an entire file into a ss::temporary_buffer
ss::future<ss::temporary_buffer<char>>
read_fully_tmpbuf(const std::filesystem::path&);

/// \brief Read an entire file into an iobuf
ss::future<iobuf> read_fully(const std::filesystem::path&);

/// \brief Read an entire file into a ss:sstring
ss::future<ss::sstring> read_fully_to_string(const std::filesystem::path&);

/// \brief Write an entire buffer into the file at location 'path'
ss::future<> write_fully(const std::filesystem::path&, iobuf buf);
