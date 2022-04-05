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

#include <system_error>

namespace storage {

enum class parser_errc {
    none = 0,
    end_of_stream,
    header_only_crc_missmatch,
    input_stream_not_enough_bytes,
    fallocated_file_read_zero_bytes_for_header,
    not_enough_bytes_in_parser_for_one_record,
};
struct parser_errc_category final : public std::error_category {
    const char* name() const noexcept final { return "storage::parser_errc"; }

    std::string message(int c) const final {
        switch (static_cast<parser_errc>(c)) {
        case parser_errc::none:
            return "storage::parser_errc::success";
        case parser_errc::end_of_stream:
            return "parser_errc::end_of_stream";
        case parser_errc::header_only_crc_missmatch:
            return "parser_errc::header_only_crc_missmatch";
        case parser_errc::input_stream_not_enough_bytes:
            return "parser_errc::input_stream_not_enough_bytes";
        case parser_errc::fallocated_file_read_zero_bytes_for_header:
            return "parser_errc::fallocated_file_read_zero_bytes_for_header";
        case parser_errc::not_enough_bytes_in_parser_for_one_record:
            return "parser_errc::not_enough_bytes_in_parser_for_one_record";
        default:
            return "storage::parser_errc::unknown";
        }
    }
};
inline const std::error_category& error_category() noexcept {
    static parser_errc_category e;
    return e;
}
inline std::error_code make_error_code(parser_errc e) noexcept {
    return std::error_code(static_cast<int>(e), error_category());
}
} // namespace storage
namespace std {
template<>
struct is_error_code_enum<storage::parser_errc> : true_type {};
} // namespace std
