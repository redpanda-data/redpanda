// Copyright 2020 Redpanda Data, Inc.
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
#include "kafka/server/flex_versions.h"

#include <seastar/core/temporary_buffer.hh>

#include <stdexcept>
#include <vector>

namespace kafka {

ss::future<std::optional<request_header>>
parse_header(ss::input_stream<char>& src) {
    constexpr int16_t no_client_id = -1;

    auto buf = co_await src.read_exactly(request_header_size);

    if (src.eof()) {
        co_return std::nullopt;
    }

    iobuf data;
    data.append(std::move(buf));
    request_reader reader(std::move(data));

    request_header header;
    header.key = api_key(reader.read_int16());
    header.version = api_version(reader.read_int16());
    header.correlation = correlation_id(reader.read_int32());

    // There is a contradiction here with the proposed flexible header
    // introduced in KIP-482. The KIP details how client_id will be a compact
    // string, however this is not the case
    auto client_id_size = reader.read_int16();
    if (client_id_size == 0) {
        header.client_id = std::string_view();
        co_return header;
    }

    if (client_id_size == no_client_id) {
        // header.client_id is left as a std::nullopt
        co_return header;
    }

    if (unlikely(client_id_size < 0)) {
        // header parsing error, force connection shutdown
        throw std::runtime_error(
          fmt::format("Invalid client_id size {}", client_id_size));
    }

    buf = co_await src.read_exactly(client_id_size);

    if (src.eof()) {
        throw std::runtime_error(fmt::format("Unexpected EOF for client ID"));
    }
    header.client_id_buffer = std::move(buf);
    header.client_id = std::string_view(
      header.client_id_buffer.get(), header.client_id_buffer.size());
    validate_utf8(*header.client_id);

    /// Conditionally handle v1 (flex) header
    if (!flex_versions::is_api_in_schema(header.key)) {
        /// User provided unsupported an invalid key that does not map
        /// to any known kafka requests, code will throw when it eventually
        /// reaches the request router
    } else if (flex_versions::is_flexible_request(header.key, header.version)) {
        auto [tags, bytes_read] = co_await parse_tags(src);
        header.tags = std::move(tags);
        header.tags_size_bytes = bytes_read;
    }
    co_return header;
}

ss::future<std::pair<std::optional<tagged_fields>, size_t>>
parse_tags(ss::input_stream<char>& src) {
    size_t total_bytes_read = 0;
    auto read_unsigned_vint =
      [](size_t& total_bytes_read, ss::input_stream<char>& src) {
          return unsigned_vint::stream_deserialize(src).then(
            [&total_bytes_read](std::pair<uint32_t, size_t> pair) {
                auto& [n, bytes_read] = pair;
                total_bytes_read += bytes_read;
                return n;
            });
      };

    auto num_tags = co_await read_unsigned_vint(total_bytes_read, src);
    if (num_tags == 0) {
        /// In the likely event that no tags are parsed as headers, return
        /// nullopt instead of an empty vector to reduce the memory overhead
        /// (that will never be used) per request
        co_return std::make_pair(std::nullopt, total_bytes_read);
    }

    tagged_fields tags;
    while (num_tags-- > 0) {
        auto tag_id = co_await read_unsigned_vint(total_bytes_read, src);
        auto next_len = co_await read_unsigned_vint(total_bytes_read, src);
        auto buf = co_await src.read_exactly(next_len);
        iobuf data;
        data.append(std::move(buf));
        total_bytes_read += next_len;
        tags.emplace_back(tag_id, std::move(data));
    }
    co_return std::make_pair(std::move(tags), total_bytes_read);
}

size_t parse_size_buffer(ss::temporary_buffer<char> buf) {
    iobuf data;
    data.append(std::move(buf));
    request_reader reader(std::move(data));
    auto size = reader.read_int32();
    if (size < 0) {
        throw std::runtime_error("kafka::parse_size_buffer is negative");
    }
    return size_t(size);
}

ss::future<std::optional<size_t>> parse_size(ss::input_stream<char>& src) {
    auto buf = co_await src.read_exactly(sizeof(int32_t));
    if (!buf) {
        co_return std::nullopt;
    }
    co_return parse_size_buffer(std::move(buf));
}

ss::scattered_message<char> response_as_scattered(response_ptr response) {
    /*
     * response header:
     *   - int32_t: size (correlation + response size)
     *   - int32_t: correlation
     */
    ss::temporary_buffer<char> b;
    const auto size = static_cast<int32_t>(
      sizeof(response->correlation()) + response->buf().size_bytes());
    iobuf header;
    response_writer writer(header);
    writer.write(size);
    writer.write(response->correlation());

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
