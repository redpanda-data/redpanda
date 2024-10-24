// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/seastarx.h"
#include "ssx/sformat.h"

#include <seastar/core/reactor.hh>
#include <seastar/core/sharded.hh>
#include <seastar/testing/perf_tests.hh>

#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>

const auto identifier = fmt::format(
  fmt::runtime("{}"), boost::uuids::random_generator()());

struct fmt_format {
    template<typename... Args>
    auto operator()(Args... args) {
        return fmt::format(std::forward<Args>(args)...);
    }
};

struct ssx_sformat {
    template<typename... Args>
    auto operator()(Args... args) {
        return ssx::sformat(std::forward<Args>(args)...);
    }
};

template<typename Ret, typename Val, typename Fun>
void run_test(Fun fun, size_t data_size) {
    std::vector<Val> vec;
    vec.reserve(data_size);
    perf_tests::start_measuring_time();
    for (int i = 0; i < data_size; ++i) {
        vec.emplace_back(fun(fmt::runtime("{}"), identifier));
    }
    perf_tests::do_not_optimize(
      Ret{fun(fmt::runtime("{}"), fmt::join(vec, ","))});
    perf_tests::stop_measuring_time();
}

// std::string thoughout
PERF_TEST(std_std_fmt_1K, join) {
    run_test<std::string, std::string>(fmt_format{}, 1 << 10);
}

// ss::sstring, but fmt::format uses std::string
PERF_TEST(ss_ss_fmt_1K, join) {
    run_test<ss::sstring, ss::sstring>(fmt_format{}, 1 << 10);
}

// ss::sstring thoughout
PERF_TEST(ss_ss_ssx_1K, join) {
    run_test<ss::sstring, ss::sstring>(ssx_sformat{}, 1 << 10);
}
