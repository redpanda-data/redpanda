// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "base/seastarx.h"

#include <seastar/core/future.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/net/api.hh>
#include <seastar/net/socket_defs.hh>

#include <array>
#include <cstddef>
#include <cstring>
#include <optional>
#include <span>
#include <string>
#include <string_view>

namespace net::dns_test {

enum class server_mode {
    // Answer NXDOMAIN to every query.
    nxdomain,
    // Answer A queries with a single 127.0.0.1 record; answer anything else
    // with an empty NOERROR response (NODATA).
    answer_a,
    // Receive queries and never reply.
    blackhole,
};

// Minimal DNS wire format handling. A query datagram is:
//   [id:2][flags:2][qdcount:2][ancount:2][nscount:2][arcount:2]
//   [qname: length-prefixed labels, 0-terminated][qtype:2][qclass:2]
//   [optional additional records, e.g. EDNS OPT]
// The reply echoes the header id and question verbatim and patches the
// header (dropping any OPT record from the additional section).
inline std::optional<ss::temporary_buffer<char>>
make_reply(std::string_view request, server_mode mode) {
    constexpr size_t header_size = 12;
    if (request.size() < header_size) {
        return std::nullopt;
    }
    auto pos = header_size;
    while (pos < request.size()) {
        auto label_len = static_cast<uint8_t>(request[pos]);
        if (label_len == 0) {
            pos += 1;
            break;
        }
        if ((label_len & 0xc0) != 0) {
            // Compression pointers never appear in queries.
            return std::nullopt;
        }
        pos += 1 + label_len;
    }
    pos += 4; // qtype + qclass
    if (pos > request.size()) {
        return std::nullopt;
    }
    const bool is_a_query = static_cast<uint8_t>(request[pos - 4]) == 0
                            && static_cast<uint8_t>(request[pos - 3]) == 1;

    const bool answer = mode == server_mode::answer_a && is_a_query;
    // name (pointer to the question) + type + class + ttl + rdlength + rdata
    constexpr size_t a_record_size = 2 + 2 + 2 + 4 + 2 + 4;
    auto reply = ss::temporary_buffer<char>(pos + (answer ? a_record_size : 0));
    std::memcpy(reply.get_write(), request.data(), pos);
    auto* b = reinterpret_cast<unsigned char*>(reply.get_write());
    b[2] = 0x81; // QR=1, opcode=QUERY, RD=1
    // RA=1, rcode: NXDOMAIN in nxdomain mode, NOERROR otherwise
    b[3] = mode == server_mode::nxdomain ? 0x83 : 0x80;
    b[4] = 0;
    b[5] = 1;                 // qdcount=1
    std::memset(b + 6, 0, 6); // ancount/nscount/arcount=0 (drops any OPT)
    if (answer) {
        b[7] = 1; // ancount=1
        const std::array<unsigned char, a_record_size> a_record{
          0xc0,
          0x0c, // name: pointer to the question qname
          0x00,
          0x01, // type A
          0x00,
          0x01, // class IN
          0x00,
          0x00,
          0x00,
          0x3c, // ttl 60s
          0x00,
          0x04, // rdlength
          127,
          0,
          0,
          1}; // rdata
        std::memcpy(b + pos, a_record.data(), a_record.size());
    }
    return reply;
}

class mock_dns_server {
public:
    explicit mock_dns_server(server_mode mode = server_mode::nxdomain)
      : _mode(mode) {}

    ss::future<> start() {
        _chan = ss::engine().net().make_bound_datagram_channel(
          ss::socket_address(ss::ipv4_addr("127.0.0.1", 0)));
        _loop = run();
        co_return;
    }

    ss::future<> stop() {
        _stopping = true;
        _chan.shutdown_input();
        _chan.shutdown_output();
        co_await std::move(_loop);
    }

    void set_mode(server_mode mode) { _mode = mode; }

    uint16_t port() const { return _chan.local_address().port(); }
    uint64_t queries_received() const { return _queries_received; }

private:
    ss::future<> run() {
        while (!_stopping) {
            try {
                auto datagram = co_await _chan.receive();
                std::string request;
                for (auto& buf : datagram.get_buffers()) {
                    request.append(buf.get(), buf.size());
                }
                ++_queries_received;
                if (_mode == server_mode::blackhole) {
                    continue;
                }
                auto reply = make_reply(request, _mode);
                if (!reply) {
                    continue;
                }
                std::array<ss::temporary_buffer<char>, 1> bufs{
                  std::move(*reply)};
                co_await _chan.send(datagram.get_src(), std::span(bufs));
            } catch (...) {
                // receive()/send() throw on shutdown_input/output.
                co_return;
            }
        }
    }

    ss::net::datagram_channel _chan;
    ss::future<> _loop = ss::make_ready_future<>();
    server_mode _mode;
    bool _stopping = false;
    uint64_t _queries_received = 0;
};

} // namespace net::dns_test
