// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "net/transport.h"

#include <seastar/core/future.hh>
#include <seastar/core/temporary_buffer.hh>

#include <gtest/gtest.h>

#include <array>
#include <span>
#include <string>
#include <string_view>
#include <variant>

using net::detail::connect_response_parser;

namespace {

ss::temporary_buffer<char> as_buf(std::string_view s) {
    return ss::temporary_buffer<char>::copy_of(s);
}

/// The post-run state of the parser plus what the input_stream
/// consumer would observe (leftover bytes from stop_consuming, or
/// nullopt if the parser asked for more data).
struct outcome {
    std::optional<std::string> leftover;
    bool saw_status = false;
    bool saw_terminator = false;
    bool limit_exceeded = false;
    std::string status_line;

    bool operator==(const outcome&) const = default;
};

outcome run_with_chunks(std::span<const std::string_view> chunks) {
    connect_response_parser p;
    std::optional<std::string> stop_leftover;
    size_t fed = 0;
    for (auto s : chunks) {
        auto result = p(as_buf(s)).get();
        std::visit(
          [&](auto& consumed) {
              using T = std::decay_t<decltype(consumed)>;
              if constexpr (std::is_same_v<T, ss::stop_consuming<char>>) {
                  stop_leftover = std::string(
                    consumed.get_buffer().get(), consumed.get_buffer().size());
              }
          },
          result.get());
        ++fed;
        if (stop_leftover.has_value()) {
            break;
        }
    }
    // From the input_stream's perspective, the bytes queued ahead of
    // the next read are the parser's stop_consuming leftover followed
    // by any unfed chunks - inputs split exactly at the header
    // terminator return an empty parser-leftover but leave the
    // following chunk queued.
    std::optional<std::string> leftover;
    if (stop_leftover.has_value()) {
        leftover = std::move(*stop_leftover);
        for (size_t k = fed; k < chunks.size(); ++k) {
            leftover->append(chunks[k]);
        }
    }
    return {
      .leftover = std::move(leftover),
      .saw_status = p.saw_status,
      .saw_terminator = p.saw_terminator,
      .limit_exceeded = p.limit_exceeded,
      .status_line = std::string(p.status_line),
    };
}

outcome run_single(std::string_view input) {
    std::array<std::string_view, 1> chunks{input};
    return run_with_chunks(chunks);
}

/// Runs `input` as one chunk, then under every 2-chunk split, and
/// asserts each variant yields the same outcome. Exhaustively
/// exercises the parser's across-buffer state machine (CR/LF
/// straddling, status-line continuation) without hand-crafting
/// chunk vectors. O(N^2) in input length; oversized-limit tests use
/// run_single() instead.
outcome run_at_all_splits(std::string_view input) {
    auto base = run_single(input);
    for (size_t k = 1; k < input.size(); ++k) {
        std::array<std::string_view, 2> chunks{
          input.substr(0, k), input.substr(k)};
        auto split_outcome = run_with_chunks(chunks);
        EXPECT_EQ(split_outcome, base) << "outcome differs when input is split "
                                          "at byte "
                                       << k;
    }
    return base;
}

} // namespace

TEST(connect_response_parser, minimal_response) {
    auto out = run_at_all_splits("HTTP/1.1 200 OK\r\n\r\n");
    ASSERT_TRUE(out.leftover.has_value());
    EXPECT_EQ(*out.leftover, "");
    EXPECT_TRUE(out.saw_status);
    EXPECT_TRUE(out.saw_terminator);
    EXPECT_FALSE(out.limit_exceeded);
    EXPECT_EQ(out.status_line, "HTTP/1.1 200 OK");
}

TEST(connect_response_parser, status_followed_by_headers) {
    auto out = run_at_all_splits(
      "HTTP/1.1 200 Connection established\r\n"
      "Proxy-Agent: mitmproxy/9.0\r\n"
      "X-Trace-Id: abc123\r\n"
      "\r\n");
    ASSERT_TRUE(out.leftover.has_value());
    EXPECT_EQ(*out.leftover, "");
    EXPECT_TRUE(out.saw_status);
    EXPECT_TRUE(out.saw_terminator);
    EXPECT_EQ(out.status_line, "HTTP/1.1 200 Connection established");
}

TEST(connect_response_parser, leftover_bytes_returned) {
    // The proxy may piggy-back forwarded bytes (e.g., the start of the
    // origin's TLS ServerHello) in the same TCP packet as the CONNECT
    // response. Those bytes must be returned to the input stream so the
    // subsequent TLS handshake reads them.
    auto out = run_at_all_splits("HTTP/1.1 200 OK\r\n\r\nLEFTOVER_BYTES");
    ASSERT_TRUE(out.leftover.has_value());
    EXPECT_EQ(*out.leftover, "LEFTOVER_BYTES");
    EXPECT_TRUE(out.saw_terminator);
}

TEST(connect_response_parser, mid_response_eof_leaves_terminator_unset) {
    // Proxy hangs up after sending the status line but before the
    // header terminator. send_connect_and_read_response uses the
    // saw_terminator flag to detect this.
    auto out = run_at_all_splits("HTTP/1.1 200 OK\r\n");
    EXPECT_FALSE(out.leftover.has_value());
    EXPECT_TRUE(out.saw_status);
    EXPECT_FALSE(out.saw_terminator);
    EXPECT_EQ(out.status_line, "HTTP/1.1 200 OK");
}

TEST(connect_response_parser, empty_buffer_signals_eof) {
    // ss::input_stream signals EOF by passing an empty buffer.
    connect_response_parser p;
    auto result = p(ss::temporary_buffer<char>{}).get();
    bool stopped = std::holds_alternative<ss::stop_consuming<char>>(
      result.get());
    EXPECT_TRUE(stopped);
    EXPECT_FALSE(p.saw_status);
    EXPECT_FALSE(p.saw_terminator);
}

TEST(connect_response_parser, oversized_single_line_trips_limit) {
    // A single header line longer than max_line_bytes must trip
    // limit_exceeded. Skip the split sweep: splits inside the
    // homogeneous 'a' run are equivalent.
    ss::sstring input;
    input.append("HTTP/1.1 200 OK\r\n", 17);
    input.append("X-Garbage: ", 11);
    input.append(
      ss::sstring(connect_response_parser::max_line_bytes + 16, 'a').c_str(),
      connect_response_parser::max_line_bytes + 16);
    input.append("\r\n", 2);
    auto out = run_single(std::string_view(input.data(), input.size()));
    ASSERT_TRUE(out.leftover.has_value());
    EXPECT_TRUE(out.limit_exceeded);
    EXPECT_FALSE(out.saw_terminator);
}

TEST(connect_response_parser, oversized_total_bytes_trips_limit) {
    // Many small headers summing past max_total_bytes must trip the
    // total-bytes limit even if no single line exceeds the per-line
    // limit. As above, skip the split sweep on a multi-kB input.
    ss::sstring input;
    input.append("HTTP/1.1 200 OK\r\n", 17);
    while (input.size() < connect_response_parser::max_total_bytes + 256) {
        input.append("X-H: v\r\n", 8);
    }
    auto out = run_single(std::string_view(input.data(), input.size()));
    ASSERT_TRUE(out.leftover.has_value());
    EXPECT_TRUE(out.limit_exceeded);
    EXPECT_FALSE(out.saw_terminator);
}
