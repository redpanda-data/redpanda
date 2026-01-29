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

#include <seastar/core/file.hh>
#include <seastar/core/iostream.hh>

namespace lsm::io {

// A disk based file reader
class disk_file_reader : public random_access_file_reader {
public:
    disk_file_reader(std::filesystem::path path, ss::file file);

    ss::future<ioarray> read(size_t offset, size_t n) override;
    ss::future<> close() override;
    fmt::iterator format_to(fmt::iterator it) const override;

    std::filesystem::path path() const { return _path; }

private:
    std::filesystem::path _path;

    ss::file _file;
};

// A disk based file writer
class disk_seq_file_writer : public sequential_file_writer {
public:
    disk_seq_file_writer(
      std::filesystem::path path, ss::output_stream<char> stream);

    ss::future<> append(iobuf buf) override;
    ss::future<> close() override;
    fmt::iterator format_to(fmt::iterator it) const override;

    std::filesystem::path path() const { return _path; }

private:
    std::filesystem::path _path;
    ss::output_stream<char> _stream;
};

} // namespace lsm::io
