// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/details/io_byte_iterator.h"
#include "bytes/details/io_fragment.h"
#include "bytes/iobuf.h"
#include "bytes/iobuf_parser.h"
#include "http/chunk_encoding.h"
#include "seastarx.h"

#include <seastar/core/temporary_buffer.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/unit_test.hpp>

#include <optional>
#include <random>
#include <sstream>

#define REQUIRE_CRLF(exp)                                                      \
    BOOST_REQUIRE_EQUAL((exp)[0], '\r');                                       \
    BOOST_REQUIRE_EQUAL((exp)[1], '\n');

class chunk_decoder {
public:
    explicit chunk_decoder(const iobuf& body) {
        for (const auto& frag : body) {
            _body.write(frag.get(), frag.size());
        }
        _body.seekg(0, std::ios::beg);
    }

    /// Get next chunk
    std::optional<iobuf> get_next() {
        // Read the chunk header
        size_t nbytes = 0;
        _body >> std::hex >> nbytes;
        std::array<char, 2> delim{};
        if (nbytes != 0) {
            // Read \r\n
            _body.read(delim.data(), delim.size());
            REQUIRE_CRLF(delim);
            iobuf chunk;
            auto placeholder = chunk.reserve(nbytes);
            _body.read(placeholder.mutable_index(), nbytes);
            // Read trailing /r/n
            _body.read(delim.data(), delim.size());
            REQUIRE_CRLF(delim);
            return std::move(chunk);
        }
        // Final header
        _body.read(delim.data(), delim.size());
        REQUIRE_CRLF(delim);
        _body.read(delim.data(), delim.size());
        REQUIRE_CRLF(delim);
        return std::nullopt;
    }

private:
    std::stringstream _body;
};

/// Generates test data fragmented in a different ways
class fragmented_test_data_generator {
public:
    explicit fragmented_test_data_generator()
      : _dist('A', 'Z') {}

    /// Generate fragmented iobuf using 'fragmentation_hint' as a reference
    iobuf generate_fragmented(const std::vector<size_t>& fragmentation_hint) {
        iobuf result;
        for (size_t fragment_size : fragmentation_hint) {
            ss::temporary_buffer<char> buf(fragment_size);
            std::generate_n(buf.get_write(), fragment_size, std::ref(*this));
            auto fragm = new details::io_fragment(
              std::move(buf), details::io_fragment::full{});
            // Make sure that fragmentation is not reduced by memory opt. in
            // iobuf
            result.append_take_ownership(fragm);
        }
        return result;
    }

    /// Generate random character
    char operator()() { return _dist(_rnddev); }

private:
    std::random_device _rnddev;
    std::uniform_int_distribution<int8_t> _dist;
};

struct test_case {
    std::vector<size_t> fragment_sizes;
    size_t encoder_chunk_size;
};

static const std::vector<test_case> test_cases = {
  {{0x100, 1337, 1234, 42}, 0x100}};

void test_chunked_encoding_fragmentation(test_case inp_case) {
    const auto& [fragment_sizes, chunk_size] = inp_case;
    fragmented_test_data_generator gen;
    http::chunked_encoder encoder(false, chunk_size);
    iobuf inp = gen.generate_fragmented(fragment_sizes);
    iobuf expected = inp.copy();
    iobuf out;
    out.append(encoder.encode(std::move(inp)));
    out.append(encoder.encode_eof());

    iobuf actual;
    chunk_decoder decoder(out);
    while (auto it = decoder.get_next()) {
        actual.append(std::move(*it));
    }

    BOOST_REQUIRE(expected == actual);
}

SEASTAR_THREAD_TEST_CASE(test_chunked_encoding_fragment_per_chunk) {
    test_chunked_encoding_fragmentation({{0x100, 0x100, 0x100}, 0x100});
}

SEASTAR_THREAD_TEST_CASE(test_chunked_encoding_fragments_smaller_than_chunk) {
    test_chunked_encoding_fragmentation({{0x100, 0x100, 0x100}, 0x1000});
}

SEASTAR_THREAD_TEST_CASE(test_chunked_encoding_fragments_larger_than_chunk) {
    test_chunked_encoding_fragmentation({{0x1000, 0x1000, 0x1000}, 0x100});
}

SEASTAR_THREAD_TEST_CASE(test_chunked_encoding_var_fragments_small_chunk) {
    test_chunked_encoding_fragmentation(
      {{0x10, 0x20, 0x30, 0x100, 0x200, 0x10}, 0x100});
}

SEASTAR_THREAD_TEST_CASE(test_chunked_encoding_empty_fragm) {
    test_chunked_encoding_fragmentation({{0x40, 0x0, 0x20}, 0x100});
}

SEASTAR_THREAD_TEST_CASE(test_chunked_bypass) {
    fragmented_test_data_generator gen;
    http::chunked_encoder encoder(true, 1000);
    iobuf inp = gen.generate_fragmented({100, 50, 10, 50, 100});
    iobuf expected = inp.copy();
    iobuf actual;
    actual.append(encoder.encode(std::move(inp)));
    actual.append(encoder.encode_eof());
    BOOST_REQUIRE(expected == actual);
}