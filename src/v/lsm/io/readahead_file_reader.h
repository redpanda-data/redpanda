/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "lsm/io/persistence.h"

namespace lsm::io {

/// A non-owning wrapper around random_access_file_reader that issues larger
/// reads than requested and buffers the extra bytes. Subsequent sequential
/// reads are served from the buffer without disk I/O.
///
/// All reads to the underlying reader are aligned to ioarray::max_chunk_size
/// boundaries so that the buffer can be extended via ioarray::concat without
/// copying, eliminating read amplification on partial buffer hits.
///
/// The caller must ensure the wrapped reader outlives this object.
class readahead_file_reader final : public random_access_file_reader {
public:
    readahead_file_reader(
      random_access_file_reader* inner,
      size_t file_size,
      size_t readahead_size);

    ss::future<ioarray> read(size_t offset, size_t n) override;
    ss::future<> close() override;
    fmt::iterator format_to(fmt::iterator it) const override;

private:
    random_access_file_reader* _inner;
    size_t _file_size;
    size_t _readahead;
    ioarray _buf;
    size_t _file_off{0};
};

} // namespace lsm::io
