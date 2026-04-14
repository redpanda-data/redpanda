/*
 * Copyright 2026 Redpanda Data, Inc.
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

#include <seastar/core/future.hh>

#include <cstdint>

namespace serde::parquet {

/// \brief Abstract interface for single-file random-access reads.
///
/// The file identity and size are bound at construction time.
class file_io {
public:
    virtual ~file_io() noexcept = default;
    virtual ss::future<iobuf> read(int64_t offset, int64_t length) = 0;
    virtual int64_t size() const = 0;
};

/// \brief In-memory implementation backed by an iobuf.
class iobuf_file_io final : public file_io {
public:
    explicit iobuf_file_io(iobuf data);
    ss::future<iobuf> read(int64_t offset, int64_t length) override;
    int64_t size() const override;

private:
    iobuf data_;
};

} // namespace serde::parquet
