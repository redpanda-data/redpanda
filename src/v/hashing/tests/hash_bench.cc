// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "hashing/crc32c.h"
#include "hashing/xx.h"
#include "model/fundamental.h"
#include "model/ktp.h"
#include "model/namespace.h"
#include "random/generators.h"

#include <seastar/core/reactor.hh>
#include <seastar/core/sstring.hh>
#include <seastar/testing/perf_tests.hh>

#include <absl/hash/hash.h>
#include <absl/strings/string_view.h>
#include <boost/crc.hpp>

static constexpr size_t step_bytes = 57;

static constexpr size_t inner_iters = 1000;

template<typename F>
static size_t header_body(F n) {
    auto buffer = random_generators::gen_alphanum_string(step_bytes);
    perf_tests::start_measuring_time();
    for (auto i = inner_iters; i--;) {
        auto s = n(buffer);
        perf_tests::do_not_optimize(s);
    }
    perf_tests::stop_measuring_time();
    return inner_iters * step_bytes;
}

PERF_TEST(header_hash, abseil_string_view) {
    return header_body([](ss::sstring& buffer) {
        return absl::HashOf(absl::string_view(buffer.data(), buffer.size()));
    });
}

PERF_TEST(header_hash, abseil_std_string_view) {
    return header_body([](ss::sstring& buffer) {
        return absl::HashOf(std::string_view(buffer.data(), buffer.size()));
    });
}

// wrap an sstring so that it can define AbslHashValue
struct abseil_wrapper_cc {
    ss::sstring& _s;

    template<typename H>
    friend H AbslHashValue(H h, const abseil_wrapper_cc& s) {
        return H::combine_contiguous(std::move(h), s._s.data(), s._s.size());
    }
};

PERF_TEST(header_hash, abseil_combine_contiguous) {
    return header_body([](ss::sstring& buffer) {
        return absl::HashOf(abseil_wrapper_cc{buffer});
    });
}

// wrap an sstring so that it can define AbslHashValue
struct abseil_wrapper_string_view {
    ss::sstring& _s;

    template<typename H>
    friend H AbslHashValue(H h, const abseil_wrapper_string_view& s) {
        return H::combine(
          std::move(h), absl::string_view(s._s.data(), s._s.size()));
    }
};

PERF_TEST(header_hash, abseil_wrap_string_view) {
    return header_body([](ss::sstring& buffer) {
        return absl::HashOf(abseil_wrapper_string_view{buffer});
    });
}

template<typename T>
static auto std_hash_of(const T& t) {
    return std::hash<T>{}(t);
}

PERF_TEST(header_hash, std_hash_string_view) {
    return header_body([](ss::sstring& buffer) {
        return std_hash_of(std::string_view(buffer.data(), buffer.size()));
    });
}

PERF_TEST(header_hash, sstring) {
    return header_body([](ss::sstring& buffer) { return std_hash_of(buffer); });
}

PERF_TEST(header_hash, boost_crc16_fn) {
    return header_body([](auto& buffer) {
        boost::crc_16_type crc;
        crc.process_bytes(buffer.data(), buffer.size());
        return crc.checksum();
    });
}

PERF_TEST(header_hash, boost_ccitt16_fn) {
    return header_body([](auto& buffer) {
        boost::crc_ccitt_type crc;
        crc.process_bytes(buffer.data(), buffer.size());
        return crc.checksum();
    });
}

PERF_TEST(header_hash, crc32_fn) {
    return header_body([](auto& buffer) {
        crc::crc32c crc;
        crc.extend(buffer.data(), buffer.size());
        return crc.value();
    });
}

PERF_TEST(header_hash, xx32_fn) {
    return header_body(
      [](auto& buffer) { return xxhash_32(buffer.data(), buffer.size()); });
}

using model::ktp;
using model::ktp_with_hash;
using model::ntp;

namespace {

inline size_t boost_hash(const ntp& ntp);

constexpr size_t topic_name_length = 30;

template<typename T>
T make_ntp(const ss::sstring& t, int32_t p) {
    return T{t, p};
}

template<>
ntp make_ntp<ntp>(const ss::sstring& t, int32_t p) {
    return ntp{model::kafka_namespace, t, p};
}

template<typename T>
static T random_ntp() {
    auto t = random_generators::gen_alphanum_string(topic_name_length);
    int32_t p = random_generators::get_int(1000);
    return make_ntp<T>(t, p);
}

template<typename NTP = model::ntp, typename F>
static size_t ntp_body(F f) {
    auto ntp = random_ntp<NTP>();
    perf_tests::do_not_optimize(ntp);
    perf_tests::start_measuring_time();
    for (auto i = inner_iters; i--;) {
        auto s = f(ntp);
        perf_tests::do_not_optimize(s);
    }
    perf_tests::stop_measuring_time();
    return inner_iters;
}

inline size_t boost_hash(const ntp& ntp) {
    size_t h = 0;
    boost::hash_combine(h, std::hash<ss::sstring>()(ntp.ns));
    boost::hash_combine(h, std::hash<ss::sstring>()(ntp.tp.topic));
    boost::hash_combine(h, std::hash<int32_t>()(ntp.tp.partition));
    return h;
}

size_t absl_hash_of(const ntp& ntp) {
    return absl::HashOf(ntp.ns, ntp.tp.topic, ntp.tp.partition);
}

template<typename T>
struct old_ntp_hash {};

template<>
struct old_ntp_hash<model::ntp> {
    size_t operator()(const model::ntp& ntp) const {
        size_t h = 0;
        boost::hash_combine(h, std::hash<ss::sstring>()(ntp.ns));
        boost::hash_combine(h, std::hash<ss::sstring>()(ntp.tp.topic));
        boost::hash_combine(
          h, std::hash<model::partition_id>()(ntp.tp.partition));
        return h;
        // return details::ntp_hash(ntp.ns, ntp.tp.topic, ntp.tp.partition);
    }
};

} // namespace

size_t get_ktp_hash(const ktp& k) { return std::hash<ktp>{}(k); }
size_t get_kaf_hash0(const model::ktp&) {
    return std::hash<std::string_view>{}("kafka");
}
size_t get_kaf_hash1() {
    return std::hash<std::string_view>{}(model::kafka_ns_view);
}
size_t get_kaf_hash2() {
    return std::hash<ss::sstring>{}(ss::sstring{model::kafka_ns_view});
}

bool ntp_equals(const ntp& l, const ktp& r) { return l == r; }

PERF_TEST(ntp_hash, ntp_std_hash) {
    return ntp_body([](const ntp& v) { return std::hash<ntp>{}(v); });
}

PERF_TEST(ntp_hash, ntp_absl_hash) {
    return ntp_body<ntp>([](const ntp& v) { return absl::Hash<ntp>{}(v); });
}

PERF_TEST(ntp_hash, ntp_any_hash_eq) {
    return ntp_body<ntp>([](const ntp& v) { return model::ktp_hash_eq{}(v); });
}

PERF_TEST(ntp_hash, ktp_std_hash) {
    return ntp_body<ktp>([](const ktp& v) { return std::hash<ktp>{}(v); });
}

PERF_TEST(ntp_hash, ktp_absl_hash) {
    return ntp_body<ktp>([](const ktp& v) { return absl::Hash<ktp>{}(v); });
}

PERF_TEST(ntp_hash, ktp_any_hash_eq) {
    return ntp_body<ktp>([](const ktp& v) { return model::ktp_hash_eq{}(v); });
}

PERF_TEST(ntp_hash, ktp_with_hash_std_hash) {
    return ntp_body<ktp_with_hash>(
      [](const ktp_with_hash& v) { return std::hash<ktp_with_hash>{}(v); });
}

PERF_TEST(ntp_hash, ntp_old_hash) {
    return ntp_body([](const ntp& v) { return old_ntp_hash<ntp>{}(v); });
}

PERF_TEST(ntp_hash, boost_hash) {
    return ntp_body([](const ntp& v) { return boost_hash(v); });
}

PERF_TEST(ntp_hash, absl_hash_of) {
    return ntp_body([](const ntp& v) { return absl_hash_of(v); });
}

PERF_TEST(ntp_hash, absl_hash_value) {
    return ntp_body([](const ntp& v) { return absl::Hash<ntp>{}(v); });
}
