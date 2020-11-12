// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "random/generators.h"
#include "test_utils/fixture.h"
#include "utils/state_crc_file.h"
#include "utils/state_crc_file_errc.h"

#include <seastar/core/file.hh>
#include <seastar/core/fstream.hh>
#include <seastar/testing/thread_test_case.hh>

struct state_crc_file_fixture {
    // test local state
    struct local_state {
        ss::sstring str_value;
        int64_t int_value;
        std::vector<uint32_t> vector;
        friend std::ostream& operator<<(std::ostream& o, const local_state& s) {
            fmt::print(
              o,
              "{{str: {}, int: {}, vec: {}}}",
              s.str_value,
              s.int_value,
              s.vector);
            return o;
        }

        friend bool operator==(const local_state& lhs, const local_state& rhs) {
            return lhs.int_value == rhs.int_value
                   && lhs.str_value == rhs.str_value
                   && lhs.vector == rhs.vector;
        }

        uint32_t crc() const {
            crc32 crc;
            crc.extend(str_value.c_str(), str_value.size());
            crc.extend(int_value);
            for (const auto i : vector) {
                crc.extend(i);
            }
            return crc.value();
        }
    };

    std::vector<uint32_t> random_vector() {
        auto size = random_generators::get_int(0, 20);
        std::vector<uint32_t> ret;

        std::generate_n(std::back_inserter(ret), size, [] {
            return random_generators::get_int(0, 1000);
        });
        return ret;
    }

    local_state make_random_state() {
        return local_state{
          .str_value = random_generators::gen_alphanum_string(10),
          .int_value = random_generators::get_int(0, 10000000),
          .vector = random_vector()};
    }

    ss::sstring test_file_name = fmt::format(
      "./local_state_{}.yml", random_generators::gen_alphanum_string(4));

    ss::future<> disturb_content(ss::sstring file) {
        return ss::open_file_dma(file, ss::open_flags::rw)
          .then([](ss::file fd) {
              return fd.size()
                .then([fd](size_t sz) mutable {
                    return fd.dma_read_bulk<char>(0, sz);
                })
                .then([fd](ss::temporary_buffer<char> buf) {
                    // distrub single byte
                    buf.get_write()[20] = buf.get_write()[20] + 1;
                    return ss::make_file_output_stream(std::move(fd))
                      .then([buf = std::move(buf)](
                              ss::output_stream<char> out) mutable {
                          return ss::do_with(
                            std::move(out),
                            std::move(buf),
                            [](
                              ss::output_stream<char>& os,
                              ss::temporary_buffer<char>& buf) {
                                return os.write(buf.get(), buf.size())
                                  .then([&os] { return os.close(); });
                            });
                      });
                });
          });
    }
};

namespace YAML {
template<>
struct convert<state_crc_file_fixture::local_state> {
    static Node encode(const state_crc_file_fixture::local_state& state) {
        Node node;
        node["str_value"] = std::string(state.str_value);
        node["int_value"] = state.int_value;
        node["vector"] = state.vector;
        return node;
    }
    static bool
    decode(const Node& node, state_crc_file_fixture::local_state& state) {
        state.str_value = node["str_value"].as<std::string>();
        state.int_value = node["int_value"].as<int64_t>();
        state.vector = node["vector"].as<std::vector<uint32_t>>();
        return true;
    }
};
}; // namespace YAML

FIXTURE_TEST(local_state_round_trip_test, state_crc_file_fixture) {
    auto state = make_random_state();
    auto yaml_file = utils::state_crc_file(test_file_name);
    // persist state
    yaml_file.persist(state).get0();

    auto read = yaml_file.read<local_state>().get0();

    BOOST_REQUIRE(read.has_value());
    BOOST_REQUIRE_EQUAL(read.value(), state);
}

FIXTURE_TEST(local_state_missing_test, state_crc_file_fixture) {
    auto yaml_file = utils::state_crc_file(test_file_name);

    auto read = yaml_file.read<local_state>().get0();

    BOOST_REQUIRE(read.has_error());
    BOOST_REQUIRE_EQUAL(
      read.error(), utils::state_crc_file_errc::file_not_found);
}

FIXTURE_TEST(local_state_checksum_mismatch, state_crc_file_fixture) {
    auto state = make_random_state();
    auto yaml_file = utils::state_crc_file(test_file_name);
    yaml_file.persist(state).get0();
    disturb_content(test_file_name).get0();
    auto read = yaml_file.read<local_state>().get0();

    BOOST_REQUIRE(read.has_error());
    BOOST_REQUIRE_EQUAL(read.error(), utils::state_crc_file_errc::crc_mismatch);
}
