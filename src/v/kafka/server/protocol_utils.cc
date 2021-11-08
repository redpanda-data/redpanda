// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/protocol_utils.h"

#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"

#include <seastar/core/temporary_buffer.hh>

#include <stdexcept>
#include <vector>

namespace kafka {
// TODO: move to iobuf_parser
ss::future<std::optional<request_header>>
parse_header(ss::input_stream<char>& src) {
    constexpr int16_t no_client_id = -1;
    return src.read_exactly(sizeof(raw_request_header))
      .then([&src](ss::temporary_buffer<char> buf) {
          if (src.eof()) {
              return ss::make_ready_future<std::optional<request_header>>();
          }
          auto client_id_size = be_to_cpu(
            reinterpret_cast<const raw_request_header*>(buf.get())
              ->client_id_size);
          auto* raw_header = reinterpret_cast<const raw_request_header*>(
            buf.get());
          auto header = request_header{
            .key = api_key(ss::net::ntoh(raw_header->api_key)),
            .version = api_version(ss::net::ntoh(raw_header->api_version)),
            .correlation = correlation_id(
              ss::net::ntoh(raw_header->correlation))};

          if (client_id_size == 0) {
              header.client_id = std::string_view();
              return ss::make_ready_future<std::optional<request_header>>(
                std::move(header));
          }
          if (client_id_size == no_client_id) {
              return ss::make_ready_future<std::optional<request_header>>(
                std::move(header));
          }
          if (unlikely(client_id_size < 0)) {
              // header parsing error, force connection shutdown
              throw std::runtime_error(
                fmt::format("Invalid client_id size {}", client_id_size));
          }
          return src.read_exactly(client_id_size)
            .then([&src, header = std::move(header)](
                    ss::temporary_buffer<char> buf) mutable {
                if (src.eof()) {
                    throw std::runtime_error(
                      fmt::format("Unexpected EOF for client ID"));
                }
                header.client_id_buffer = std::move(buf);
                header.client_id = std::string_view(
                  header.client_id_buffer.get(),
                  header.client_id_buffer.size());
                validate_utf8(*header.client_id);
                return ss::make_ready_future<std::optional<request_header>>(
                  std::move(header));
            });
      });
}

size_t parse_size_buffer(ss::temporary_buffer<char>& buf) {
    auto* raw = reinterpret_cast<const int32_t*>(buf.get());
    int32_t size = ss::be_to_cpu(*raw);
    if (size < 0) {
        throw std::runtime_error("kafka::parse_size_buffer is negative");
    }
    return size_t(size);
}

ss::future<std::optional<size_t>> parse_size(ss::input_stream<char>& src) {
    return src.read_exactly(sizeof(int32_t))
      .then([](ss::temporary_buffer<char> buf) -> std::optional<size_t> {
          if (!buf) {
              return std::nullopt;
          }
          return parse_size_buffer(buf);
      });
}

ss::scattered_message<char> response_as_scattered(response_ptr response) {
    auto correlation = response->correlation();
    auto header = ss::temporary_buffer<char>(sizeof(raw_response_header));
    // NOLINTNEXTLINE
    auto* raw_header = reinterpret_cast<raw_response_header*>(
      header.get_write());
    auto size = int32_t(sizeof(correlation) + response->buf().size_bytes());
    raw_header->size = ss::cpu_to_be(size);
    raw_header->correlation = ss::cpu_to_be(correlation());
    auto& buf = response->buf();
    buf.prepend(std::move(header));
    ss::scattered_message<char> msg;
    auto in = iobuf::iterator_consumer(buf.cbegin(), buf.cend());
    int32_t chunk_no = 0;
    in.consume(
      buf.size_bytes(), [&msg, &chunk_no, &buf](const char* src, size_t sz) {
          ++chunk_no;
          vassert(
            chunk_no <= std::numeric_limits<int16_t>::max(),
            "Invalid construction of scattered_message. max count:{}. Usually "
            "a bug with small append() to iobuf. {}",
            chunk_no,
            buf);
          msg.append_static(src, sz);
          return ss::stop_iteration::no;
      });
    // MUST be the foreign ptr not the iobuf
    msg.on_delete([response = std::move(response)] {});
    return msg;
}

} // namespace kafka
