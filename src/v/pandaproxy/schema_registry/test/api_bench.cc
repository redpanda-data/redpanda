/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "absl/strings/escaping.h"
#include "base/vassert.h"
#include "http/client.h"
#include "pandaproxy/schema_registry/test/client_utils.h"
#include "pandaproxy/schema_registry/test/protobuf_utils.h"
#include "pandaproxy/schema_registry/types.h"
#include "pandaproxy/test/pandaproxy_fixture.h"

#include <seastar/testing/perf_tests.hh>

namespace pps = pandaproxy::schema_registry;

namespace {

struct perf_settings {
    using lookup_version_logic = pps::schema_version (*)(int);

    int n_subjects;
    int n_versions;
    int i_subject;
    lookup_version_logic version_logic;
};

struct endpoint {
    using signature = consumed_response (*)(
      ::http::client&, const pps::context_subject&, const ss::sstring&);
    signature fn;
    ss::sstring name;
};

ss::sstring make_payload(
  const ss::sstring& schema,
  pps::schema_type type = pps::schema_type::protobuf) {
    return ss::format(
      R"({{ "schemaType": "{}", "schema": "{}" }})",
      type,
      absl::CEscape(schema));
}

pps::context_subject make_subject(int i_sub) {
    return pps::context_subject{ss::format("TestSubject{}", i_sub)};
}

pps::schema_version middle_version(int n_versions) {
    if (n_versions == 1) {
        return pps::schema_version{1};
    }
    return pps::schema_version{n_versions / 2};
}

pps::schema_version last_version(int n_versions) {
    return pps::schema_version{n_versions};
}

void setup_client(::http::client& client, perf_settings ps) {
    for (int i_sub = 0; i_sub < ps.n_subjects; ++i_sub) {
        auto sub = make_subject(i_sub);
        for (int i_ver = 1; i_ver <= ps.n_versions; ++i_ver) {
            auto schema = pps::test_utils::make_proto_schema(sub, i_ver);
            auto payload = make_payload(schema);
            auto res = post_schema(client, sub, payload);
            vassert(
              res.headers.result() == boost::beast::http::status::ok,
              "Client setup failed");
        }
    }
}

constexpr size_t inner_iters = 100;

void perf_body(
  ::http::client& client, const endpoint& ep, const perf_settings& ps) {
    const auto subject = make_subject(ps.i_subject);
    const auto version = ps.version_logic(ps.n_versions);
    const auto schema = pps::test_utils::make_proto_schema(subject, version);
    const auto payload = make_payload(schema);

    perf_tests::start_measuring_time();
    for (size_t i = 0; i < inner_iters; ++i) {
        auto res = ep.fn(client, subject, payload);
        perf_tests::do_not_optimize(res);
        dassert(
          res.headers.result() == boost::beast::http::status::ok,
          "{err_ctx} failed for sub {} version {}",
          ep.name,
          subject,
          version);
    }
    perf_tests::stop_measuring_time();
}

} // namespace

class sr_bench_fixture : public pandaproxy_test_fixture {
public:
    sr_bench_fixture()
      : pandaproxy_test_fixture() {}

    sr_bench_fixture(const sr_bench_fixture&) = delete;
    sr_bench_fixture(sr_bench_fixture&&) = delete;
    sr_bench_fixture operator=(const sr_bench_fixture&) = delete;
    sr_bench_fixture operator=(sr_bench_fixture&&) = delete;
    ~sr_bench_fixture() = default;

    future<size_t> run_test(endpoint ep, perf_settings ps) {
        auto client = make_schema_reg_client();
        ss::thread_attributes thread_attr;
        co_await ss::async(thread_attr, [&client, &ep, ps] {
            setup_client(client, ps);
            perf_body(client, ep, ps);
        });
        co_return inner_iters;
    }
};

static constexpr const char* schema_lookup{"Schema lookup"};
static constexpr const char* schema_post{"Schema post"};
static std::map<std::string_view, endpoint> perf_apis{
  {schema_lookup, endpoint{.fn = lookup_schema, .name = schema_lookup}},
  {schema_post, endpoint{.fn = post_schema, .name = schema_post}}};

PERF_TEST_CN(sr_bench_fixture, lookup_x1_1) {
    perf_settings ss{
      .n_subjects = 1,
      .n_versions = 1,
      .i_subject = 0,
      .version_logic = middle_version};
    co_return co_await run_test(perf_apis[schema_lookup], ss);
}
PERF_TEST_CN(sr_bench_fixture, lookup_x1_10) {
    perf_settings ss{
      .n_subjects = 1,
      .n_versions = 10,
      .i_subject = 0,
      .version_logic = middle_version};
    co_return co_await run_test(perf_apis[schema_lookup], ss);
}
PERF_TEST_CN(sr_bench_fixture, lookup_x1_100) {
    perf_settings ss{
      .n_subjects = 1,
      .n_versions = 100,
      .i_subject = 0,
      .version_logic = middle_version};
    co_return co_await run_test(perf_apis[schema_lookup], ss);
}

PERF_TEST_CN(sr_bench_fixture, lookup_x10_1) {
    perf_settings ss{
      .n_subjects = 10,
      .n_versions = 1,
      .i_subject = 0,
      .version_logic = middle_version};
    co_return co_await run_test(perf_apis[schema_lookup], ss);
}
PERF_TEST_CN(sr_bench_fixture, lookup_x10_10) {
    perf_settings ss{
      .n_subjects = 10,
      .n_versions = 10,
      .i_subject = 0,
      .version_logic = middle_version};
    co_return co_await run_test(perf_apis[schema_lookup], ss);
    ;
}
PERF_TEST_CN(sr_bench_fixture, lookup_x20_20) {
    perf_settings ss{
      .n_subjects = 20,
      .n_versions = 20,
      .i_subject = 0,
      .version_logic = middle_version};
    co_return co_await run_test(perf_apis[schema_lookup], ss);
}

PERF_TEST_CN(sr_bench_fixture, post_x1_1) {
    perf_settings ss{
      .n_subjects = 1,
      .n_versions = 1,
      .i_subject = 0,
      .version_logic = last_version};
    co_return co_await run_test(perf_apis[schema_post], ss);
}
PERF_TEST_CN(sr_bench_fixture, post_x1_10) {
    perf_settings ss{
      .n_subjects = 1,
      .n_versions = 10,
      .i_subject = 0,
      .version_logic = last_version};
    co_return co_await run_test(perf_apis[schema_post], ss);
}
PERF_TEST_CN(sr_bench_fixture, post_x1_100) {
    perf_settings ss{
      .n_subjects = 1,
      .n_versions = 100,
      .i_subject = 0,
      .version_logic = last_version};
    co_return co_await run_test(perf_apis[schema_post], ss);
}

PERF_TEST_CN(sr_bench_fixture, post_x10_1) {
    perf_settings ss{
      .n_subjects = 10,
      .n_versions = 1,
      .i_subject = 0,
      .version_logic = last_version};
    co_return co_await run_test(perf_apis[schema_post], ss);
}
PERF_TEST_CN(sr_bench_fixture, post_x10_10) {
    perf_settings ss{
      .n_subjects = 10,
      .n_versions = 10,
      .i_subject = 0,
      .version_logic = last_version};
    co_return co_await run_test(perf_apis[schema_post], ss);
}
PERF_TEST_CN(sr_bench_fixture, post_x20_20) {
    perf_settings ss{
      .n_subjects = 20,
      .n_versions = 20,
      .i_subject = 0,
      .version_logic = last_version};
    co_return co_await run_test(perf_apis[schema_post], ss);
}
