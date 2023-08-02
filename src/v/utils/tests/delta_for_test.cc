// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/iobuf.h"
#include "delta_for_characterization_data.h"
#include "random/generators.h"
#include "utils/delta_for.h"
#include "version.h"
#include "vlog.h"

#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

#include <ranges>
#include <stdexcept>

template<class TVal, class DeltaT>
std::vector<TVal> populate_encoder(
  deltafor_encoder<TVal, DeltaT>& c,
  uint64_t initial_value,
  const std::vector<std::pair<TVal, TVal>>& deltas) {
    std::vector<TVal> result;
    auto p = initial_value + deltas.front().first;
    for (auto [min_delta, max_delta] : deltas) {
        std::array<TVal, details::FOR_buffer_depth> buf = {};
        for (int x = 0; x < details::FOR_buffer_depth; x++) {
            result.push_back(p);
            buf.at(x) = p;
            p += random_generators::get_int(min_delta, max_delta);
            if (p < buf.at(x)) {
                throw std::out_of_range("delta can't be represented");
            }
        }
        c.add(buf);
    }
    return result;
}

template<class TVal, class DeltaT>
void roundtrip_test(
  const std::vector<std::pair<TVal, TVal>>& deltas, DeltaT delta) {
    static constexpr TVal initial_value = 0;
    deltafor_encoder<TVal, DeltaT> enc(initial_value, delta);
    auto expected = populate_encoder(enc, initial_value, deltas);

    deltafor_decoder<TVal, DeltaT> dec(
      initial_value, enc.get_row_count(), enc.copy(), delta);

    std::vector<TVal> actual;
    std::array<TVal, details::FOR_buffer_depth> buf{};
    int cnt = 0;
    while (dec.read(buf)) {
        cnt++;
        std::copy(buf.begin(), buf.end(), std::back_inserter(actual));
        buf = {};
    }
    BOOST_REQUIRE_EQUAL(cnt, deltas.size());
    BOOST_REQUIRE(expected == actual);
}

BOOST_AUTO_TEST_CASE(roundtrip_test_1) {
    std::vector<std::pair<int64_t, int64_t>> deltas = {
      std::make_pair(0LL, 0LL),
      std::make_pair(0LL, 10LL),
      std::make_pair(0LL, 100LL),
      std::make_pair(0LL, 1000LL),
      std::make_pair(0LL, 10000LL),
      std::make_pair(0LL, 100000LL),
      std::make_pair(0LL, 1000000LL),
      std::make_pair(0LL, 10000000LL),
      std::make_pair(0LL, 100000000LL),
      std::make_pair(0LL, 1000000000LL),
      std::make_pair(0LL, 10000000000LL),
      std::make_pair(0LL, 100000000000LL),
      std::make_pair(0LL, 1000000000000LL),
      std::make_pair(0LL, 10000000000000LL),
      std::make_pair(0LL, 100000000000000LL),
      std::make_pair(0LL, 1000000000000000LL),
      std::make_pair(0LL, 10000000000000000LL),
      std::make_pair(0LL, 100000000000000000LL),
      std::make_pair(0LL, 1000000000000000000LL),
    };
    roundtrip_test<int64_t>(deltas, details::delta_xor());
}

BOOST_AUTO_TEST_CASE(roundtrip_test_2) {
    std::vector<std::pair<uint64_t, uint64_t>> deltas = {
      std::make_pair(0ULL, 0ULL),
      std::make_pair(0ULL, 10ULL),
      std::make_pair(0ULL, 100ULL),
      std::make_pair(0ULL, 1000ULL),
      std::make_pair(0ULL, 10000ULL),
      std::make_pair(0ULL, 100000ULL),
      std::make_pair(0ULL, 1000000ULL),
      std::make_pair(0ULL, 10000000ULL),
      std::make_pair(0ULL, 100000000ULL),
      std::make_pair(0ULL, 1000000000ULL),
      std::make_pair(0ULL, 10000000000ULL),
      std::make_pair(0ULL, 100000000000ULL),
      std::make_pair(0ULL, 1000000000000ULL),
      std::make_pair(0ULL, 10000000000000ULL),
      std::make_pair(0ULL, 100000000000000ULL),
      std::make_pair(0ULL, 1000000000000000ULL),
      std::make_pair(0ULL, 10000000000000000ULL),
      std::make_pair(0ULL, 100000000000000000ULL),
      std::make_pair(0ULL, 1000000000000000000ULL),
    };
    roundtrip_test<uint64_t>(deltas, details::delta_xor());
}

BOOST_AUTO_TEST_CASE(roundtrip_test_3) {
    std::vector<std::pair<int64_t, int64_t>> deltas = {
      std::make_pair(10LL, 10LL),
      std::make_pair(10LL, 100LL),
      std::make_pair(10LL, 1000LL),
      std::make_pair(10LL, 10000LL),
      std::make_pair(10LL, 100000LL),
      std::make_pair(10LL, 1000000LL),
      std::make_pair(10LL, 10000000LL),
      std::make_pair(10LL, 100000000LL),
      std::make_pair(10LL, 1000000000LL),
      std::make_pair(10LL, 10000000000LL),
      std::make_pair(10LL, 100000000000LL),
      std::make_pair(10LL, 1000000000000LL),
      std::make_pair(10LL, 10000000000000LL),
      std::make_pair(10LL, 100000000000000LL),
      std::make_pair(10LL, 1000000000000000LL),
      std::make_pair(10LL, 10000000000000000LL),
      std::make_pair(10LL, 100000000000000000LL),
    };
    roundtrip_test<int64_t>(deltas, details::delta_delta<int64_t>(10));
}

BOOST_AUTO_TEST_CASE(roundtrip_test_4) {
    std::vector<std::pair<uint64_t, uint64_t>> deltas = {
      std::make_pair(10ULL, 10ULL),
      std::make_pair(10ULL, 100ULL),
      std::make_pair(10ULL, 1000ULL),
      std::make_pair(10ULL, 10000ULL),
      std::make_pair(10ULL, 100000ULL),
      std::make_pair(10ULL, 1000000ULL),
      std::make_pair(10ULL, 10000000ULL),
      std::make_pair(10ULL, 100000000ULL),
      std::make_pair(10ULL, 1000000000ULL),
      std::make_pair(10ULL, 10000000000ULL),
      std::make_pair(10ULL, 100000000000ULL),
      std::make_pair(10ULL, 1000000000000ULL),
      std::make_pair(10ULL, 10000000000000ULL),
      std::make_pair(10ULL, 100000000000000ULL),
      std::make_pair(10ULL, 1000000000000000ULL),
      std::make_pair(10ULL, 10000000000000000ULL),
      std::make_pair(10ULL, 100000000000000000ULL),
    };
    roundtrip_test<uint64_t>(deltas, details::delta_delta<uint64_t>(10));
}

template<class TVal>
void test_random_walk_roundtrip(int test_size, int max_delta) {
    static constexpr TVal initial_value = 0;
    deltafor_encoder<TVal> enc(initial_value);
    std::vector<std::pair<TVal, TVal>> deltas;
    deltas.reserve(test_size);
    for (int i = 0; i < test_size; i++) {
        deltas.push_back(
          std::make_pair(0, random_generators::get_int(max_delta)));
    }
    auto expected = populate_encoder(enc, initial_value, deltas);

    deltafor_decoder<TVal> dec(initial_value, enc.get_row_count(), enc.copy());

    std::vector<TVal> actual;
    std::array<TVal, details::FOR_buffer_depth> buf{};
    int cnt = 0;
    while (dec.read(buf)) {
        cnt++;
        std::copy(buf.begin(), buf.end(), std::back_inserter(actual));
        buf = {};
    }
    BOOST_REQUIRE_EQUAL(cnt, deltas.size());
    BOOST_REQUIRE(expected == actual);
}

BOOST_AUTO_TEST_CASE(random_walk_test_1) {
    static constexpr int test_size = 100000;
    static constexpr int max_delta = 100;
    test_random_walk_roundtrip<uint64_t>(test_size, max_delta);
}

BOOST_AUTO_TEST_CASE(random_walk_test_2) {
    static constexpr int test_size = 100000;
    static constexpr int max_delta = 1000;
    test_random_walk_roundtrip<uint64_t>(test_size, max_delta);
}

BOOST_AUTO_TEST_CASE(random_walk_test_3) {
    static constexpr int test_size = 100000;
    static constexpr int max_delta = 10000;
    test_random_walk_roundtrip<uint64_t>(test_size, max_delta);
}

BOOST_AUTO_TEST_CASE(random_walk_test_4) {
    static constexpr int test_size = 100000;
    static constexpr int max_delta = 100000;
    test_random_walk_roundtrip<uint64_t>(test_size, max_delta);
}

BOOST_AUTO_TEST_CASE(random_walk_test_5) {
    static constexpr int test_size = 100000;
    static constexpr int max_delta = 100;
    test_random_walk_roundtrip<int64_t>(test_size, max_delta);
}

BOOST_AUTO_TEST_CASE(random_walk_test_6) {
    static constexpr int test_size = 100000;
    static constexpr int max_delta = 1000;
    test_random_walk_roundtrip<int64_t>(test_size, max_delta);
}

BOOST_AUTO_TEST_CASE(random_walk_test_7) {
    static constexpr int test_size = 100000;
    static constexpr int max_delta = 10000;
    test_random_walk_roundtrip<int64_t>(test_size, max_delta);
}

BOOST_AUTO_TEST_CASE(random_walk_test_8) {
    static constexpr int test_size = 100000;
    static constexpr int max_delta = 100000;
    test_random_walk_roundtrip<int64_t>(test_size, max_delta);
}

BOOST_AUTO_TEST_CASE(test_compression_ratio) {
    const int num_rows = 100000;
    const int num_elements = num_rows * 16;
    static constexpr uint64_t initial_value = 0;
    static constexpr uint64_t min_step = 10000;
    deltafor_encoder<uint64_t> enc_xor(initial_value);
    deltafor_encoder<uint64_t, details::delta_delta<uint64_t>> enc_delta(
      initial_value, details::delta_delta(min_step));
    std::vector<std::pair<uint64_t, uint64_t>> deltas;
    deltas.reserve(num_rows);
    for (int i = 0; i < num_rows; i++) {
        deltas.emplace_back(std::make_pair(min_step, min_step + 100));
    }
    populate_encoder(enc_xor, initial_value, deltas);
    populate_encoder(enc_delta, initial_value, deltas);
    BOOST_REQUIRE(
      enc_xor.share().size_bytes() > enc_delta.share().size_bytes());
    auto num_bytes_per_val = num_elements;
    BOOST_REQUIRE(enc_delta.share().size_bytes() < num_bytes_per_val);
}

template<class TVal, class DeltaT>
std::vector<std::array<TVal, details::FOR_buffer_depth>> populate_encoder(
  deltafor_encoder<TVal, DeltaT>& c,
  std::vector<deltafor_stream_pos_t<TVal>>& pos,
  uint64_t initial_value,
  const std::vector<std::pair<TVal, TVal>>& deltas) {
    std::vector<std::array<TVal, details::FOR_buffer_depth>> result;
    auto p = initial_value + deltas.front().first;
    for (auto [min_delta, max_delta] : deltas) {
        std::array<TVal, details::FOR_buffer_depth> buf = {};
        for (int x = 0; x < details::FOR_buffer_depth; x++) {
            buf.at(x) = p;
            p += random_generators::get_int(min_delta, max_delta);
            if (p < buf.at(x)) {
                throw std::out_of_range("delta can't be represented");
            }
        }
        result.push_back(buf);
        pos.push_back(c.get_position());
        c.add(buf);
    }
    return result;
}

template<class TVal, class DeltaT>
void skip_test(const std::vector<std::pair<TVal, TVal>>& deltas, DeltaT delta) {
    static constexpr TVal initial_value = 0;
    deltafor_encoder<TVal, DeltaT> enc(initial_value, delta);
    std::vector<deltafor_stream_pos_t<TVal>> positions;
    auto expected = populate_encoder(enc, positions, initial_value, deltas);

    BOOST_REQUIRE_EQUAL(positions.size(), expected.size());

    for (auto i = 0; i < expected.size(); i++) {
        auto row = expected.at(i);
        auto pos = positions.at(i);
        deltafor_decoder<TVal, DeltaT> dec(
          initial_value, enc.get_row_count(), enc.copy(), delta);
        dec.skip(pos);
        std::array<TVal, details::FOR_buffer_depth> buf{};
        auto success = dec.read(buf);
        BOOST_REQUIRE(success);
        BOOST_REQUIRE(row == buf);

        // maybe read another item
        if (i != expected.size() - 1) {
            row = expected.at(i + 1);
            buf = {};
            success = dec.read(buf);
            BOOST_REQUIRE(success);
            BOOST_REQUIRE(row == buf);
        }
    }
}

BOOST_AUTO_TEST_CASE(skip_test_1) {
    std::vector<std::pair<int64_t, int64_t>> deltas = {
      std::make_pair(0LL, 0LL),
      std::make_pair(0LL, 10LL),
      std::make_pair(0LL, 100LL),
      std::make_pair(0LL, 1000LL),
      std::make_pair(0LL, 10000LL),
      std::make_pair(0LL, 100000LL),
      std::make_pair(0LL, 1000000LL),
      std::make_pair(0LL, 10000000LL),
      std::make_pair(0LL, 100000000LL),
      std::make_pair(0LL, 1000000000LL),
      std::make_pair(0LL, 10000000000LL),
      std::make_pair(0LL, 100000000000LL),
      std::make_pair(0LL, 1000000000000LL),
      std::make_pair(0LL, 10000000000000LL),
      std::make_pair(0LL, 100000000000000LL),
      std::make_pair(0LL, 1000000000000000LL),
      std::make_pair(0LL, 10000000000000000LL),
      std::make_pair(0LL, 100000000000000000LL),
      std::make_pair(0LL, 1000000000000000000LL),
    };
    skip_test<int64_t>(deltas, details::delta_xor());
}

BOOST_AUTO_TEST_CASE(skip_test_2) {
    std::vector<std::pair<uint64_t, uint64_t>> deltas = {
      std::make_pair(0ULL, 0ULL),
      std::make_pair(0ULL, 10ULL),
      std::make_pair(0ULL, 100ULL),
      std::make_pair(0ULL, 1000ULL),
      std::make_pair(0ULL, 10000ULL),
      std::make_pair(0ULL, 100000ULL),
      std::make_pair(0ULL, 1000000ULL),
      std::make_pair(0ULL, 10000000ULL),
      std::make_pair(0ULL, 100000000ULL),
      std::make_pair(0ULL, 1000000000ULL),
      std::make_pair(0ULL, 10000000000ULL),
      std::make_pair(0ULL, 100000000000ULL),
      std::make_pair(0ULL, 1000000000000ULL),
      std::make_pair(0ULL, 10000000000000ULL),
      std::make_pair(0ULL, 100000000000000ULL),
      std::make_pair(0ULL, 1000000000000000ULL),
      std::make_pair(0ULL, 10000000000000000ULL),
      std::make_pair(0ULL, 100000000000000000ULL),
      std::make_pair(0ULL, 1000000000000000000ULL),
    };
    skip_test<uint64_t>(deltas, details::delta_xor());
}

BOOST_AUTO_TEST_CASE(skip_test_3) {
    std::vector<std::pair<int64_t, int64_t>> deltas = {
      std::make_pair(10LL, 10LL),
      std::make_pair(10LL, 100LL),
      std::make_pair(10LL, 1000LL),
      std::make_pair(10LL, 10000LL),
      std::make_pair(10LL, 100000LL),
      std::make_pair(10LL, 1000000LL),
      std::make_pair(10LL, 10000000LL),
      std::make_pair(10LL, 100000000LL),
      std::make_pair(10LL, 1000000000LL),
      std::make_pair(10LL, 10000000000LL),
      std::make_pair(10LL, 100000000000LL),
      std::make_pair(10LL, 1000000000000LL),
      std::make_pair(10LL, 10000000000000LL),
      std::make_pair(10LL, 100000000000000LL),
      std::make_pair(10LL, 1000000000000000LL),
      std::make_pair(10LL, 10000000000000000LL),
      std::make_pair(10LL, 100000000000000000LL),
    };
    skip_test<int64_t>(deltas, details::delta_delta<int64_t>(10));
}

BOOST_AUTO_TEST_CASE(skip_test_4) {
    std::vector<std::pair<uint64_t, uint64_t>> deltas = {
      std::make_pair(10ULL, 10ULL),
      std::make_pair(10ULL, 100ULL),
      std::make_pair(10ULL, 1000ULL),
      std::make_pair(10ULL, 10000ULL),
      std::make_pair(10ULL, 100000ULL),
      std::make_pair(10ULL, 1000000ULL),
      std::make_pair(10ULL, 10000000ULL),
      std::make_pair(10ULL, 100000000ULL),
      std::make_pair(10ULL, 1000000000ULL),
      std::make_pair(10ULL, 10000000000ULL),
      std::make_pair(10ULL, 100000000000ULL),
      std::make_pair(10ULL, 1000000000000ULL),
      std::make_pair(10ULL, 10000000000000ULL),
      std::make_pair(10ULL, 100000000000000ULL),
      std::make_pair(10ULL, 1000000000000000ULL),
      std::make_pair(10ULL, 10000000000000000ULL),
      std::make_pair(10ULL, 100000000000000000ULL),
    };
    skip_test<uint64_t>(deltas, details::delta_delta<uint64_t>(10));
}

BOOST_AUTO_TEST_CASE(
  test_deltafor_generate_characterization_data, *boost::unit_test::disabled()) {
    /** this test is disabled because it generates a bunch of data to be used
     * for characterization of deltafor, and prints it to stdout ready to paste
     * in a file and use it for unit tests*/

    struct datum {
        std::array<int64_t, details::FOR_buffer_depth> ref;
        std::vector<uint8_t> bytes;
    };

    auto to_save = std::vector<datum>{};

    // generate a datum for each N_BITS [0, 64], with the bounds being the edge
    // cases
    for (auto i = 0u; i <= sizeof(int64_t) * 8; ++i) {
        auto dfor = deltafor_datapoint::dfor_enc{};
        auto buffer = std::array<int64_t, details::FOR_buffer_depth>{};
        if (i > 0) {
            // random bits
            std::ranges::generate(buffer, [] {
                return random_generators::get_int(
                  std::numeric_limits<int64_t>::min(),
                  std::numeric_limits<int64_t>::max());
            });

            // clear out upper bits, and set an odd number of msb to 1 to
            // ensure that delta_xor produces N_BITS=i
            auto keep_mask = ~(std::numeric_limits<uint64_t>::max() << (i - 1));
            for (auto& e : buffer) {
                e &= keep_mask;
            }
            buffer[2] |= uint64_t{1} << (i - 1);
            buffer[5] |= uint64_t{1} << (i - 1);
            buffer[11] |= uint64_t{1} << (i - 1);
        }
        {
            auto tmp = std::array<int64_t, details::FOR_buffer_depth>{};
            auto xor_reducer = details::delta_xor{};
            // require that generated data will in fact be encoded in i bits,
            // to fully test the code paths of deltafor
            auto row_bitwidth
              = xor_reducer.encode<int64_t, details::FOR_buffer_depth>(
                0, buffer, tmp);
            BOOST_REQUIRE_EQUAL(row_bitwidth, i);
        }
        dfor.add(buffer);
        auto serialized = iobuf_parser{serde::to_iobuf(std::move(dfor))};
        auto tmp = std::vector<uint8_t>(serialized.bytes_left(), 0);
        serialized.consume_to(serialized.bytes_left(), tmp.begin());
        to_save.emplace_back(buffer, std::move(tmp));
    }

    auto deltafor_datapoint_printer = [](auto& p) {
        return fmt::format(
          "{{ (int64_t[]) {{ {} }},\n(uint8_t[]) {{ {} }} }}",
          fmt::join(p.ref, ","),
          fmt::join(p.bytes, ","));
    };
    fmt::print(
      R"cpp(

#include "delta_for_characterization_data.h"

#include <cstdint>

// data produced from {}

constexpr auto characterization_data = std::to_array<deltafor_datapoint>({{
{}
}});

auto get_characterization_data() -> std::span<const deltafor_datapoint> {{
    return characterization_data;
}}
)cpp",
      redpanda_version(),
      fmt::join(
        to_save | std::views::transform(deltafor_datapoint_printer), ",\n"));
}

BOOST_AUTO_TEST_CASE(test_deltafor_characterization) {
    using dfor_enc = deltafor_datapoint::dfor_enc;
    using dfor_dec = deltafor_datapoint::dfor_dec;

    for (auto [source_data, serialized_data] : get_characterization_data()) {
        {
            // check serializing source_data generates the same binary format
            auto generated_serialization = [&] {
                auto dfor = dfor_enc{};
                auto tmp = std::array<int64_t, details::FOR_buffer_depth>{};
                std::ranges::copy(source_data, tmp.begin());
                dfor.add(tmp);
                auto stream = iobuf_parser{serde::to_iobuf(std::move(dfor))};
                auto res = std::vector<uint8_t>(stream.bytes_left(), 0);
                stream.consume_to(stream.bytes_left(), res.begin());
                return res;
            }();

            BOOST_CHECK(
              std::ranges::equal(generated_serialization, serialized_data));
        }

        {
            // check deserializing the binary format generates the same
            // source_data
            auto deserialized_data = [&] {
                auto buf = iobuf{};
                buf.append(serialized_data.data(), serialized_data.size());

                auto enc = serde::from_iobuf<dfor_enc>(std::move(buf));
                auto dec = dfor_dec{
                  enc.get_initial_value(), enc.get_row_count(), enc.share()};

                std::array<int64_t, details::FOR_buffer_depth> result{};
                BOOST_CHECK(dec.read(result));
                return result;
            }();
            BOOST_CHECK(std::ranges::equal(deserialized_data, source_data));
        }
    }
}
