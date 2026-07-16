/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "bytes/iobuf.h"
#include "cluster_link/model/types.h"
#include "cluster_link/schema_registry_sync/http_source_reader.h"
#include "http/client.h"
#include "pandaproxy/schema_registry/rest_client/client.h"
#include "pandaproxy/schema_registry/types.h"
#include "test_utils/async.h"

#include <seastar/core/abort_source.hh>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <deque>
#include <memory>
#include <optional>

namespace srs = cluster_link::schema_registry_sync;
namespace rc = pandaproxy::schema_registry::rest_client;
namespace pps = pandaproxy::schema_registry;
namespace bh = boost::beast::http;
using namespace std::chrono_literals;
using namespace testing;

namespace {

constexpr auto endpoint = "http://localhost:8081";

class mock_client : public http::abstract_client {
public:
    MOCK_METHOD(
      ss::future<http::downloaded_response>,
      request_and_collect_response,
      (bh::request_header<>&&,
       std::optional<iobuf>,
       ss::lowres_clock::duration),
      (override));
    MOCK_METHOD(ss::future<>, shutdown_and_stop, (), (override));
};

// Builds a rest_client over a mocked transport; the reader takes ownership and
// drives it. `set_expectations` arms the single GET the test exercises.
std::unique_ptr<rc::client>
make_rest_client(std::function<void(mock_client&)> set_expectations) {
    auto http_client = std::make_unique<NiceMock<mock_client>>();
    ON_CALL(*http_client, shutdown_and_stop()).WillByDefault([] {
        return ss::make_ready_future<>();
    });
    set_expectations(*http_client);
    return std::make_unique<rc::client>(
      std::move(http_client),
      endpoint,
      std::nullopt,
      pps::qualified_subjects_enabled::yes);
}

auto respond(bh::status status, std::string_view body) {
    return [status, body = ss::sstring{body}](
             bh::request_header<>&&,
             std::optional<iobuf>,
             ss::lowres_clock::duration) {
        return ss::make_ready_future<http::downloaded_response>(
          http::downloaded_response{
            .status = status, .body = iobuf::from(body)});
    };
}

srs::http_source_reader reader_over(
  std::function<void(mock_client&)> set_expectations = [](mock_client&) {}) {
    return srs::http_source_reader{
      make_rest_client(std::move(set_expectations))};
}

} // namespace

// list_contexts issues GET /contexts and returns every context the source
// reports (dot-prefixed names), so subjects in non-default contexts are
// discoverable.
TEST(http_source_reader, list_contexts_enumerates_all_contexts) {
    auto reader = reader_over([](mock_client& m) {
        EXPECT_CALL(m, request_and_collect_response(_, _, _))
          .WillOnce([](
                      bh::request_header<>&& r,
                      std::optional<iobuf>,
                      ss::lowres_clock::duration) {
              EXPECT_EQ(r.target(), "/contexts");
              return ss::make_ready_future<http::downloaded_response>(
                http::downloaded_response{
                  .status = bh::status::ok,
                  .body = iobuf::from(R"([".", ".dev", ".prod"])")});
          });
    });
    ss::abort_source as;
    auto res = reader.list_contexts(as).get();
    reader.stop().get();

    ASSERT_TRUE(res.has_value());
    EXPECT_THAT(
      *res,
      ElementsAre(
        pps::default_context, pps::context{".dev"}, pps::context{".prod"}));
}

// Stopping the reader while a request is in flight aborts the request rather
// than waiting for it out: mirroring_task::stop() stops the reader before
// joining the run fibers, relying on exactly this to unwedge fibers parked in
// reader-internal waits (rate-limiter token/pause queues, pool slots).
TEST(http_source_reader, stop_aborts_in_flight_requests) {
    std::deque<ss::promise<http::downloaded_response>> parked;
    auto reader = reader_over([&](mock_client& m) {
        ON_CALL(m, request_and_collect_response(_, _, _))
          .WillByDefault([&](
                           bh::request_header<>&&,
                           std::optional<iobuf>,
                           ss::lowres_clock::duration) {
              parked.emplace_back();
              return parked.back().get_future();
          });
        // Like the real transport, shutdown fails the requests it is
        // servicing.
        ON_CALL(m, shutdown_and_stop()).WillByDefault([&] {
            for (auto& request : parked) {
                request.set_exception(
                  std::make_exception_ptr(ss::abort_requested_exception{}));
            }
            parked.clear();
            return ss::make_ready_future<>();
        });
    });
    ss::abort_source as;
    auto req = reader.list_contexts(as);
    RPTEST_REQUIRE_EVENTUALLY(5s, [&] { return !parked.empty(); });

    reader.stop().get();
    auto res = req.get();
    ASSERT_FALSE(res.has_value());
    EXPECT_EQ(res.error().kind, srs::source_error_kind::source_unavailable);
}

// A reader call after stop() must fail fast instead of lazily rebuilding the
// client: the reconcile engine's fibers can still be unwinding when the
// reader is stopped, and a rebuilt client would never be shut down.
TEST(http_source_reader, calls_after_stop_fail_without_rebuild) {
    srs::http_source_connection conn{
      .address = net::unresolved_address("example.invalid", 8081),
      .endpoint = "http://example.invalid:8081",
    };
    srs::http_source_reader reader{std::move(conn)};
    reader.stop().get();

    ss::abort_source as;
    auto res = reader.list_contexts(as).get();
    ASSERT_FALSE(res.has_value());
    EXPECT_EQ(res.error().kind, srs::source_error_kind::source_unavailable);
    EXPECT_THAT(res.error().message, HasSubstr("stopped"));
    // stop() stays idempotent.
    reader.stop().get();
}

// stop() is idempotent: the task teardown can stop the reader more than once
// (an in-flight reconciler stopping the task before link teardown stops it
// again). A second stop() must not double-close the rest_client's gate, which
// would abort. Assert the transport is shut down exactly once.
TEST(http_source_reader, stop_is_idempotent) {
    size_t shutdowns = 0;
    auto http_client = std::make_unique<NiceMock<mock_client>>();
    ON_CALL(*http_client, shutdown_and_stop()).WillByDefault([&shutdowns] {
        ++shutdowns;
        return ss::make_ready_future<>();
    });
    auto client = std::make_unique<rc::client>(
      std::move(http_client),
      endpoint,
      std::nullopt,
      pps::qualified_subjects_enabled::yes);
    auto reader = srs::http_source_reader{std::move(client)};

    // The injected client is shut down on the first stop(); the second stop()
    // must be a no-op rather than closing the client's gate a second time.
    reader.stop().get();
    reader.stop().get();

    EXPECT_EQ(shutdowns, 1);
}

// The reader does not serialize requests: the reconcile engine's fibers drive
// it concurrently, and in-flight requests are bounded by the pooled transport
// underneath (here a single injected mock, so two requests overlapping on it
// proves the reader itself imposes no serialization).
TEST(http_source_reader, requests_run_concurrently) {
    int inflight = 0;
    int max_inflight = 0;
    std::deque<ss::promise<http::downloaded_response>> parked;
    auto reader = reader_over([&](mock_client& m) {
        EXPECT_CALL(m, request_and_collect_response(_, _, _))
          .WillRepeatedly([&](
                            bh::request_header<>&&,
                            std::optional<iobuf>,
                            ss::lowres_clock::duration) {
              ++inflight;
              max_inflight = std::max(max_inflight, inflight);
              parked.emplace_back();
              return parked.back().get_future().finally(
                [&inflight] { --inflight; });
          });
    });
    ss::abort_source as;
    auto first = reader.list_contexts(as);
    auto second = reader.list_contexts(as);
    RPTEST_REQUIRE_EVENTUALLY(5s, [&] { return parked.size() == 2; });
    EXPECT_EQ(max_inflight, 2);

    for (auto& request : parked) {
        request.set_value(
          http::downloaded_response{
            .status = bh::status::ok, .body = iobuf::from(R"(["."])")});
    }
    ASSERT_TRUE(first.get().has_value());
    ASSERT_TRUE(second.get().has_value());
    reader.stop().get();
}

// The rest_client scopes GET /subjects to the requested context (a mock that
// ignores the subjectPrefix hint still returns other contexts here), so the
// reader surfaces subjects in that context only and discovery stays
// single-context.
TEST(http_source_reader, list_subjects_filters_to_requested_context) {
    auto reader = reader_over([](mock_client& m) {
        EXPECT_CALL(m, request_and_collect_response(_, _, _))
          .WillOnce(respond(bh::status::ok, R"(["s1", ":.ctx:s2"])"));
    });
    ss::abort_source as;
    auto res = reader.list_subjects(pps::default_context, as).get();
    reader.stop().get();

    ASSERT_TRUE(res.has_value());
    EXPECT_THAT(*res, ElementsAre(pps::context_subject::unqualified("s1")));
}

TEST(http_source_reader, read_subject_version_returns_schema) {
    auto reader = reader_over([](mock_client& m) {
        EXPECT_CALL(m, request_and_collect_response(_, _, _))
          .WillOnce(respond(
            bh::status::ok,
            R"({"subject":"User","version":3,"id":100001,"schemaType":"AVRO",)"
            R"("schema":"{\"type\":\"record\",\"name\":\"User\"}"})"));
    });
    ss::abort_source as;
    auto res = reader
                 .read_subject_version(
                   pps::context_subject::unqualified("User"),
                   pps::schema_version{3},
                   as)
                 .get();
    reader.stop().get();

    EXPECT_THAT(
      res,
      Optional(AllOf(
        Field("id", &pps::source_schema_read::id, pps::schema_id{100001}),
        Field(
          "version", &pps::source_schema_read::version, pps::schema_version{3}),
        Field(
          "unsupported", &pps::source_schema_read::unsupported, IsEmpty()))));
}

// Discovery and body fetch must request deleted entries so the reconcile can
// propagate soft-deletes: a subject whose only versions are deleted must still
// be enumerated, and a soft-deleted version's body must still resolve.
TEST(http_source_reader, requests_include_deleted) {
    {
        auto reader = reader_over([](mock_client& m) {
            EXPECT_CALL(m, request_and_collect_response(_, _, _))
              .WillOnce([](
                          bh::request_header<>&& r,
                          std::optional<iobuf>,
                          ss::lowres_clock::duration) {
                  // Also carries a subjectPrefix context scope (asserted in the
                  // rest_client tests); query-param order is not fixed, so
                  // match the deleted flag by substring.
                  EXPECT_THAT(
                    std::string(r.target().data(), r.target().size()),
                    HasSubstr("deleted=true"));
                  return ss::make_ready_future<http::downloaded_response>(
                    http::downloaded_response{
                      .status = bh::status::ok, .body = iobuf::from("[]")});
              });
        });
        ss::abort_source as;
        auto res = reader.list_subjects(pps::default_context, as).get();
        reader.stop().get();
        ASSERT_TRUE(res.has_value());
    }
    {
        auto reader = reader_over([](mock_client& m) {
            EXPECT_CALL(m, request_and_collect_response(_, _, _))
              .WillOnce([](
                          bh::request_header<>&& r,
                          std::optional<iobuf>,
                          ss::lowres_clock::duration) {
                  EXPECT_THAT(
                    std::string(r.target().data(), r.target().size()),
                    HasSubstr("deleted=true"));
                  return ss::make_ready_future<
                    http::downloaded_response>(http::downloaded_response{
                    .status = bh::status::ok,
                    .body = iobuf::from(
                      R"({"subject":"User","version":3,"id":7,)"
                      R"("schemaType":"AVRO",)"
                      R"("schema":"{\"type\":\"record\",\"name\":\"User\"}"})")});
              });
        });
        ss::abort_source as;
        auto res = reader
                     .read_subject_version(
                       pps::context_subject::unqualified("User"),
                       pps::schema_version{3},
                       as)
                     .get();
        reader.stop().get();
        ASSERT_TRUE(res.has_value());
    }
}

// A transport exception leaves the source unreachable: the link should park,
// not skip an item.
TEST(http_source_reader, unreachable_source_maps_to_source_unavailable) {
    auto reader = reader_over([](mock_client& m) {
        EXPECT_CALL(m, request_and_collect_response(_, _, _))
          .WillOnce([](
                      bh::request_header<>&&,
                      std::optional<iobuf>,
                      ss::lowres_clock::duration) {
              return ss::make_exception_future<http::downloaded_response>(
                std::runtime_error("connection refused"));
          });
    });
    ss::abort_source as;
    auto res = reader.list_subjects(pps::default_context, as).get();
    reader.stop().get();

    ASSERT_FALSE(res.has_value());
    EXPECT_EQ(res.error().kind, srs::source_error_kind::source_unavailable);
}

// A reachable source that returns an error status is a per-item failure: the
// sync continues with the next item rather than parking.
TEST(http_source_reader, reachable_error_maps_to_operation_failed) {
    auto reader = reader_over([](mock_client& m) {
        EXPECT_CALL(m, request_and_collect_response(_, _, _))
          .WillOnce(respond(
            bh::status::unprocessable_entity, R"({"error_code": 42201})"));
    });
    ss::abort_source as;
    auto res = reader.list_subjects(pps::default_context, as).get();
    reader.stop().get();

    ASSERT_FALSE(res.has_value());
    EXPECT_EQ(res.error().kind, srs::source_error_kind::operation_failed);
}

// Auth failures (401 Unauthorized / 403 Forbidden) are a link-wide, terminal
// condition: the credentials are wrong or lack permission, so every request
// would fail identically. The reader maps them to source_unavailable so the
// sync backs off rather than churning through and re-failing every subject.
// These statuses are not in the rest_client's retriable set, so a single
// response settles the result.
TEST(http_source_reader, auth_failure_maps_to_source_unavailable) {
    for (auto status : {bh::status::unauthorized, bh::status::forbidden}) {
        auto reader = reader_over([status](mock_client& m) {
            EXPECT_CALL(m, request_and_collect_response(_, _, _))
              .WillOnce(respond(status, R"({"error_code": 40101})"));
        });
        ss::abort_source as;
        auto res = reader.list_subjects(pps::default_context, as).get();
        reader.stop().get();

        ASSERT_FALSE(res.has_value()) << "status=" << static_cast<int>(status);
        EXPECT_EQ(res.error().kind, srs::source_error_kind::source_unavailable)
          << "status=" << static_cast<int>(status);
    }
}

// HTTP 404 / error_code 40401 maps to subject_not_found, not operation_failed.
TEST(http_source_reader, not_found_maps_to_subject_not_found) {
    auto reader = reader_over([](mock_client& m) {
        EXPECT_CALL(m, request_and_collect_response(_, _, _))
          .WillOnce(respond(bh::status::not_found, R"({"error_code": 40401})"));
    });
    ss::abort_source as;
    auto res = reader
                 .list_subject_versions(
                   pps::context_subject::unqualified("Gone"),
                   pps::include_deleted::no,
                   as)
                 .get();
    reader.stop().get();

    ASSERT_FALSE(res.has_value());
    EXPECT_EQ(res.error().kind, srs::source_error_kind::subject_not_found);
}

// read_mode narrows the source's open-enum mode to Redpanda's three-valued
// mode. READONLY_OVERRIDE has no direct destination value but is read-only, so
// it is preserved as READONLY rather than dropped.
TEST(http_source_reader, read_mode_narrows_supported_modes) {
    struct tc {
        std::string_view wire;
        pps::mode expected;
    };
    for (auto [wire, expected] : {
           tc{"READWRITE", pps::mode::read_write},
           tc{"READONLY", pps::mode::read_only},
           tc{"READONLY_OVERRIDE", pps::mode::read_only},
           tc{"IMPORT", pps::mode::import},
         }) {
        auto reader = reader_over([wire](mock_client& m) {
            EXPECT_CALL(m, request_and_collect_response(_, _, _))
              .WillOnce(respond(
                bh::status::ok, fmt::format(R"({{"mode":"{}"}})", wire)));
        });
        ss::abort_source as;
        auto res
          = reader.read_mode(pps::context_subject::unqualified("s1"), as).get();
        reader.stop().get();
        ASSERT_TRUE(res.has_value()) << "wire=" << wire;
        ASSERT_TRUE(res->has_value()) << "wire=" << wire;
        EXPECT_EQ(**res, expected) << "wire=" << wire;
    }
}

// A source mode Redpanda cannot represent (FORWARD, or an unknown value a newer
// server reports) is a per-item operation_failed the task counts as an error,
// not a nullopt (which would be misread as "no override").
TEST(http_source_reader, read_mode_unmappable_is_operation_failed) {
    for (auto wire : {"FORWARD", "SOMETHING_NEW"}) {
        auto reader = reader_over([wire](mock_client& m) {
            EXPECT_CALL(m, request_and_collect_response(_, _, _))
              .WillOnce(respond(
                bh::status::ok, fmt::format(R"({{"mode":"{}"}})", wire)));
        });
        ss::abort_source as;
        auto res
          = reader.read_mode(pps::context_subject::unqualified("s1"), as).get();
        reader.stop().get();
        ASSERT_FALSE(res.has_value()) << "wire=" << wire;
        EXPECT_EQ(res.error().kind, srs::source_error_kind::operation_failed)
          << "wire=" << wire;
    }
}

// A subject with no explicit mode override (HTTP 404 / error_code 40409) is not
// an error: it reads back as nullopt so the sync knows there is nothing to
// replicate at this level.
TEST(http_source_reader, read_mode_absent_override_is_nullopt) {
    auto reader = reader_over([](mock_client& m) {
        EXPECT_CALL(m, request_and_collect_response(_, _, _))
          .WillOnce(respond(bh::status::not_found, R"({"error_code": 40409})"));
    });
    ss::abort_source as;
    auto res
      = reader.read_mode(pps::context_subject::unqualified("s1"), as).get();
    reader.stop().get();

    ASSERT_TRUE(res.has_value());
    EXPECT_FALSE(res->has_value());
}

// The registry-wide global context is read via GET /mode/:.__GLOBAL: (a
// context-qualified subject), not the subject-less GET /mode.
TEST(http_source_reader, read_mode_global_context_hits_global_endpoint) {
    auto reader = reader_over([](mock_client& m) {
        EXPECT_CALL(m, request_and_collect_response(_, _, _))
          .WillOnce([](
                      bh::request_header<>&& r,
                      std::optional<iobuf>,
                      ss::lowres_clock::duration) {
              EXPECT_THAT(
                std::string(r.target().data(), r.target().size()),
                HasSubstr(".__GLOBAL"));
              return ss::make_ready_future<http::downloaded_response>(
                http::downloaded_response{
                  .status = bh::status::ok,
                  .body = iobuf::from(R"({"mode":"READONLY"})")});
          });
    });
    ss::abort_source as;
    auto res = reader
                 .read_mode(
                   pps::context_subject{pps::global_context, pps::subject{""}},
                   as)
                 .get();
    reader.stop().get();

    ASSERT_TRUE(res.has_value());
    ASSERT_TRUE(res->has_value());
    EXPECT_EQ(**res, pps::mode::read_only);
}

// read_config mirrors read_mode: the seven defined levels narrow one-to-one, an
// absent override (40408) is nullopt, and an unknown level is operation_failed.
TEST(http_source_reader, read_config_narrows_and_classifies) {
    {
        auto reader = reader_over([](mock_client& m) {
            EXPECT_CALL(m, request_and_collect_response(_, _, _))
              .WillOnce(respond(
                bh::status::ok, R"({"compatibilityLevel":"FULL_TRANSITIVE"})"));
        });
        ss::abort_source as;
        auto res = reader
                     .read_config(pps::context_subject::unqualified("s1"), as)
                     .get();
        reader.stop().get();
        ASSERT_TRUE(res.has_value());
        ASSERT_TRUE(res->compatibility.has_value());
        EXPECT_EQ(
          *res->compatibility, pps::compatibility_level::full_transitive);
        EXPECT_TRUE(res->unsupported.empty());
    }
    {
        auto reader = reader_over([](mock_client& m) {
            EXPECT_CALL(m, request_and_collect_response(_, _, _))
              .WillOnce(
                respond(bh::status::not_found, R"({"error_code": 40408})"));
        });
        ss::abort_source as;
        auto res = reader
                     .read_config(pps::context_subject::unqualified("s1"), as)
                     .get();
        reader.stop().get();
        ASSERT_TRUE(res.has_value());
        EXPECT_FALSE(res->compatibility.has_value());
    }
    {
        auto reader = reader_over([](mock_client& m) {
            EXPECT_CALL(m, request_and_collect_response(_, _, _))
              .WillOnce(respond(
                bh::status::ok, R"({"compatibilityLevel":"WEIRD_LEVEL"})"));
        });
        ss::abort_source as;
        auto res = reader
                     .read_config(pps::context_subject::unqualified("s1"), as)
                     .get();
        reader.stop().get();
        ASSERT_FALSE(res.has_value());
        EXPECT_EQ(res.error().kind, srs::source_error_kind::operation_failed);
    }
    {
        // Governance-only subject config: no compatibility level, only an
        // unsupported field. The read succeeds ("no override") and carries the
        // field for the policy instead of failing the parse.
        auto reader = reader_over([](mock_client& m) {
            EXPECT_CALL(m, request_and_collect_response(_, _, _))
              .WillOnce(respond(
                bh::status::ok,
                R"({"compatibilityGroup":"app.major.version"})"));
        });
        ss::abort_source as;
        auto res = reader
                     .read_config(pps::context_subject::unqualified("s1"), as)
                     .get();
        reader.stop().get();
        ASSERT_TRUE(res.has_value());
        EXPECT_FALSE(res->compatibility.has_value());
        ASSERT_EQ(res->unsupported.size(), size_t{1});
        EXPECT_EQ(res->unsupported[0].json_pointer, "/compatibilityGroup");
    }
}

// A null config (not in API mode) or an unparseable URL yields an unavailable
// reader, so the link parks rather than faulting.
TEST(http_source_reader, factory_parks_on_missing_or_unparseable_config) {
    srs::http_source_reader_factory factory;
    ss::abort_source as;

    auto null_reader = factory.create(nullptr);
    auto null_res = null_reader->list_contexts(as).get();
    ASSERT_FALSE(null_res.has_value());
    EXPECT_EQ(
      null_res.error().kind, srs::source_error_kind::source_unavailable);
    // Not-in-API-mode must not masquerade as a missing feature.
    EXPECT_THAT(null_res.error().message, HasSubstr("not configured"));

    cluster_link::model::schema_registry_sync_config::shadow_schema_registry_api
      api;
    api.source_url = "";
    auto empty_reader = factory.create(&api);
    auto empty_res = empty_reader->list_contexts(as).get();
    ASSERT_FALSE(empty_res.has_value());

    // A non-empty but unparseable URL (bad port) parks the same way, and the
    // error names the bad URL rather than a generic placeholder, so an operator
    // is not sent down a "feature missing" diagnosis path for a typo.
    api.source_url = "http://host:notaport";
    auto bad_reader = factory.create(&api);
    auto bad_res = bad_reader->list_contexts(as).get();
    ASSERT_FALSE(bad_res.has_value());
    EXPECT_THAT(
      bad_res.error().message, HasSubstr("invalid source Schema Registry URL"));
    EXPECT_THAT(bad_res.error().message, HasSubstr("http://host:notaport"));
}

// parse_source_address resolves the transport host:port. ada normalizes away a
// port equal to the scheme default, so an explicit standard port and an omitted
// one are indistinguishable; both must resolve to the scheme default rather
// than a fixed 8081, so a source behind standard 443/80 is reachable. An
// explicit non-default port is honored, and an unresolvable URL yields nullopt.
TEST(http_source_reader, parse_source_address_port_resolution) {
    auto addr = [](std::string_view url) {
        return srs::parse_source_address(url);
    };

    EXPECT_EQ(addr("https://sr.example.com")->port(), 443);
    EXPECT_EQ(addr("https://sr.example.com:443")->port(), 443);
    EXPECT_EQ(addr("http://sr.example.com")->port(), 80);
    EXPECT_EQ(addr("http://sr.example.com:80")->port(), 80);
    // Explicit non-default ports (incl. the SR convention 8081) are honored.
    EXPECT_EQ(addr("http://sr.example.com:8081")->port(), 8081);
    EXPECT_EQ(addr("https://sr.example.com:9000")->port(), 9000);

    EXPECT_EQ(addr("https://sr.example.com")->host(), "sr.example.com");

    // A bare host or a root-path "/" carries no prefix and resolves fine.
    EXPECT_TRUE(addr("https://sr.example.com/").has_value());

    // Unresolvable: no host, or a non-numeric port ada rejects at parse time.
    EXPECT_FALSE(addr("").has_value());
    EXPECT_FALSE(addr("not a url").has_value());
    EXPECT_FALSE(addr("http://host:notaport").has_value());

    // Unsupported: a path prefix, query, or fragment would be silently dropped
    // (only host:port reaches the transport), so reject rather than mislead.
    EXPECT_FALSE(addr("https://proxy.example.com/schema-registry").has_value());
    EXPECT_FALSE(addr("https://sr.example.com/api/v1").has_value());
    EXPECT_FALSE(addr("https://sr.example.com/?foo=bar").has_value());
    EXPECT_FALSE(addr("https://sr.example.com/#frag").has_value());
}
