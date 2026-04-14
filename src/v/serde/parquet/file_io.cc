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

#include "serde/parquet/file_io.h"

#include <seastar/core/coroutine.hh>

namespace serde::parquet {

iobuf_file_io::iobuf_file_io(iobuf data)
  : data_(std::move(data)) {}

ss::future<iobuf> iobuf_file_io::read(int64_t offset, int64_t length) {
    co_return data_.share(
      static_cast<size_t>(offset), static_cast<size_t>(length));
}

int64_t iobuf_file_io::size() const {
    return static_cast<int64_t>(data_.size_bytes());
}

} // namespace serde::parquet
