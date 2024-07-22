/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cloud_storage/inventory/tests/common.h"

#include "bytes/iostream.h"
#include "compression/internal/gzip_compressor.h"

#include <charconv>

namespace cloud_storage::inventory {
ss::input_stream<char>
make_report_stream(ss::sstring s, is_gzip_compressed compress) {
    iobuf b;
    b.append(s.data(), s.size());
    if (compress) {
        return make_iobuf_input_stream(
          compression::internal::gzip_compressor::compress(b));
    }
    return make_iobuf_input_stream(std::move(b));
}

ss::input_stream<char>
make_report_stream(std::vector<ss::sstring> rows, is_gzip_compressed compress) {
    return make_report_stream(
      fmt::format("{}", fmt::join(rows, "\n")), compress);
}

std::vector<std::filesystem::path>
collect_hash_files(const std::filesystem::path& p) {
    std::vector<std::filesystem::path> hash_files;
    auto hash_files_view
      = std::filesystem::directory_iterator{p}
        | std::views::filter(is_hash_file)
        | std::views::transform(&std::filesystem::directory_entry::path)
        | std::views::transform(&std::filesystem::path::filename);
    std::ranges::copy(hash_files_view, std::back_inserter(hash_files));
    return hash_files;
}

bool is_hash_file(const std::filesystem::directory_entry& de) {
    const auto& p_str = de.path().filename().string();
    long v{};
    return de.is_regular_file()
           && std::from_chars(p_str.data(), p_str.data() + p_str.size(), v).ec
                != std::errc::invalid_argument;
}
} // namespace cloud_storage::inventory
