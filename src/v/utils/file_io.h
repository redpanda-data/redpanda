/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "bytes/iobuf.h"
#include "seastarx.h"

#include <seastar/core/seastar.hh>
#include <seastar/core/temporary_buffer.hh>

#include <filesystem>

/// \brief Read an entire file into a ss::temporary_buffer
ss::future<ss::temporary_buffer<char>> read_fully_tmpbuf(ss::sstring);

/// \brief Read an entire file into an iobuf
ss::future<iobuf> read_fully(ss::sstring name);

/// \brief Write an entire buffer into the file at location 'path'
ss::future<> write_fully(const std::filesystem::path&, iobuf buf);
