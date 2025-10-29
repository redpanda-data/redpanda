/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "base/seastarx.h"
#include "kafka/client/types.h"
#include "kafka/protocol/fetch.h"
#include "kafka/protocol/schemata/fetch_request.h"
#include "kafka/protocol/types.h"
#include "kafka/server/fetch_session.h"
#include "kafka/server/fetch_session_cache.h"
#include "kafka/server/handlers/fetch.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "redpanda/tests/fixture.h"
#include "security/acl.h"

#include <seastar/testing/perf_tests.hh>

#include <fmt/core.h>

#include <string>

static ss::logger fpt_logger("fpt_test");

using namespace std::chrono_literals; // NOLINT
struct fixture {
    static kafka::fetch_request::topic
    make_fetch_request_topic(model::topic tp, int partitions_count) {
        kafka::fetch_request::topic fetch_topic{
          .topic = std::move(tp),
          .partitions = {},
        };

        for (int i = 0; i < partitions_count; ++i) {
            fetch_topic.partitions.push_back(
              kafka::fetch_request::partition{
                .partition = model::partition_id(i),
                .fetch_offset = model::offset(i * 10),
                .partition_max_bytes = 100_KiB,
              });
        }
        return fetch_topic;
    }
};

constexpr kafka::api_version topic_names{11};
constexpr kafka::api_version topic_ids{13};

static constexpr size_t topic_name_length = 30;

struct test_args {
    kafka::api_version fetch_version{topic_names};
    size_t topic_count;
    size_t partitions_per_topic;
    bool use_authz;
    bool operator==(const test_args&) const = default;
};

struct fixture_state {
    std::vector<model::topic> topics;
    kafka::fetch_metadata_cache* mdc;
    std::unique_ptr<kafka::op_context> octx;
};

struct fetch_plan : redpanda_thread_fixture {
    test_args _args;
    std::optional<fixture_state> _state;

    fetch_plan()
      : _args{} {}

    // get a wildcard topic read acl binding
    static auto get_wildcard_bindings() {
        security::acl_entry acl(
          security::acl_wildcard_user,
          security::acl_wildcard_host,
          security::acl_operation::all,
          security::acl_permission::allow);

        security::resource_pattern resource(
          security::resource_type::topic,
          security::resource_pattern::wildcard,
          security::pattern_type::literal);

        std::vector<security::acl_binding> bindings{
          security::acl_binding{resource, acl}};
        return bindings;
    }

    ss::future<fixture_state&> init_bench(test_args args) {
        if (_state) {
            vassert(_args == args, "args mismatch");
            co_return *_state;
        }

        _args = args;
        auto [api_version, tcount, pcount, use_authz] = args;

        co_await wait_for_controller_leadership();

        _state.emplace();

        co_await ss::parallel_for_each(boost::irange(tcount), [&, this](int i) {
            constexpr auto topic_name_format = "topic-{:05}-";
            auto name = fmt::format(topic_name_format, i);
            vassert(name.size() <= topic_name_length, "cannot fit name");
            name += std::string(topic_name_length - name.size(), 'x');
            vassert(name.size() == topic_name_length, "huh");
            model::topic topic{name};
            _state->topics.push_back(topic);
            return add_topic(
              model::topic_namespace_view(model::kafka_namespace, topic),
              pcount);
        });

        auto fetch_req = make_fetch_req(api_version);

        // we need to share a connection among any requests here since the
        // session cache is associated with a connection
        auto conn = make_connection_context(use_authz);

        if (use_authz) {
            conn->server().authorizer().add_bindings(get_wildcard_bindings());
        }

        kafka::request_header header{
          .key = kafka::fetch_handler::api::key,
          .version = kafka::fetch_handler::max_supported};

        // use this initial request to populate the fetch session
        // in the session cache
        kafka::fetch_session_id sess_id;
        {
            auto rctx = make_request_context(
              make_fetch_req(api_version), header, conn);
            // set up a fetch session
            auto ctx = rctx.fetch_sessions().maybe_get_session(fetch_req);
            vassert(
              ctx.has_error() == false,
              "Expected ctx.has_error() == false, got {}",
              ctx.has_error());
            // first fetch has to be full fetch
            vassert(
              ctx.is_full_fetch() == true,
              "Expected ctx.is_full_fetch() == true, got {}",
              ctx.is_full_fetch());
            vassert(
              ctx.is_sessionless() == false,
              "Expected ctx.is_sessionless() == false, got {}",
              ctx.is_sessionless());

            vassert(
              ctx.session()->partitions().size() == pcount * tcount,
              "Expected session()->partitions().size() == "
              "session_partition_count, got {} vs {}",
              ctx.session()->partitions().size(),
              pcount);

            sess_id = ctx.session()->id();
            vassert(sess_id > 0, "Expected sess_id > 0, got {}", sess_id);
        }

        fetch_req.data.session_id = sess_id;
        fetch_req.data.session_epoch = 1;
        fetch_req.data.topics.clear();

        auto rctx = make_request_context(std::move(fetch_req), header, conn);
        vassert(
          rctx.fetch_sessions().size() == 1,
          "Expected fetch_sessions().size() == 1, got {}",
          rctx.fetch_sessions().size());

        // add all partitions to fetch metadata
        auto& mdc = rctx.get_fetch_metadata_cache();
        _state->mdc = &mdc;

        for (auto& topic : _state->topics) {
            for (unsigned i = 0; i < args.partitions_per_topic; i++) {
                mdc.insert_or_assign(
                  {topic, model::partition_id(i)},
                  model::offset(0),
                  model::offset(100),
                  model::offset(100),
                  0,
                  0,
                  0);
            }
        }

        vassert(
          mdc.size() == args.partitions_per_topic * args.topic_count,
          "mdc.size(): {}",
          mdc.size());

        auto octx = std::make_unique<kafka::op_context>(
          std::move(rctx), ss::default_smp_service_group());

        vassert(
          !octx->session_ctx.is_sessionless(),
          "Expected !session_ctx.is_sessionless(), got {}",
          octx->session_ctx.is_sessionless());
        vassert(
          octx->session_ctx.session()->id() == sess_id,
          "Expected session()->id() == sess_id, got {} vs {}",
          octx->session_ctx.session()->id(),
          sess_id);

        _state->octx = std::move(octx);
        co_return *_state;
    }

    kafka::fetch_request make_fetch_req(kafka::api_version api_version) {
        // create a request
        kafka::fetch_request_data frq_data;
        frq_data.replica_id = kafka::client::consumer_replica_id;
        frq_data.max_wait_ms = 500ms;
        frq_data.min_bytes = 1;
        frq_data.max_bytes = 52428800;
        frq_data.isolation_level = model::isolation_level::read_uncommitted;
        frq_data.session_id = kafka::invalid_fetch_session_id;
        frq_data.session_epoch = kafka::initial_fetch_session_epoch;

        for (auto& topic : _state->topics) {
            // make the fetch topic
            kafka::fetch_topic ft;
            if (api_version >= kafka::api_version{13}) {
                ft.topic_id = app.metadata_cache.local()
                                .get_topic_metadata_ref(
                                  {model::kafka_namespace, topic})
                                ->get()
                                .get_configuration()
                                .tp_id.value();
            } else {
                ft.topic = topic;
            }

            // add the partitions to the fetch request
            for (size_t pid = 0; pid < _args.partitions_per_topic; pid++) {
                kafka::fetch_partition fp;
                fp.partition = model::partition_id(static_cast<int32_t>(pid));
                fp.fetch_offset = model::offset(0);
                fp.current_leader_epoch = kafka::leader_epoch(-1);
                fp.log_start_offset = model::offset(-1);
                fp.partition_max_bytes = 1048576;
                ft.partitions.push_back(std::move(fp));
            }

            frq_data.topics.push_back(std::move(ft));
        }

        return kafka::fetch_request{std::move(frq_data)};
    };

    ss::future<size_t> run_bench(
      kafka::api_version api_version,
      size_t tcount,
      size_t pcount,
      bool authz) {
#ifdef NDEBUG
        const size_t iters = 1000000 / (tcount * pcount);
#else
        // keep things speedy in slow debug builds
        const size_t iters = 1;
#endif

        auto& state = co_await init_bench(
          {.fetch_version = api_version,
           .topic_count = tcount,
           .partitions_per_topic = pcount,
           .use_authz = authz});

        perf_tests::start_measuring_time();
        for (size_t i = 0; i < iters; i++) {
            auto plan = kafka::testing::make_simple_fetch_plan(*state.octx);
            perf_tests::do_not_optimize(plan);
        }
        perf_tests::stop_measuring_time();

        // check that nothing was evicted
        vassert(
          state.mdc->size() == tcount * pcount,
          "mdc->size(): {}",
          state.mdc->size());

        co_return iters;
    }
};

PERF_TEST_F(fetch_plan, t1p1_no_auth) {
    return run_bench(topic_names, 1, 1, false);
}
PERF_TEST_F(fetch_plan, t1p1_yes_auth) {
    return run_bench(topic_names, 1, 1, true);
}
PERF_TEST_F(fetch_plan, t1p100_no_auth) {
    return run_bench(topic_names, 1, 100, false);
}
PERF_TEST_F(fetch_plan, t1p100_yes_auth) {
    return run_bench(topic_names, 1, 100, true);
}
PERF_TEST_F(fetch_plan, t100p1_no_auth) {
    return run_bench(topic_names, 100, 1, false);
}
PERF_TEST_F(fetch_plan, t100p1_yes_auth) {
    return run_bench(topic_names, 100, 1, true);
}

PERF_TEST_F(fetch_plan, t1p1_no_auth_ids) {
    return run_bench(topic_ids, 1, 1, false);
}
PERF_TEST_F(fetch_plan, t1p1_yes_auth_ids) {
    return run_bench(topic_ids, 1, 1, true);
}
PERF_TEST_F(fetch_plan, t1p100_no_auth_ids) {
    return run_bench(topic_ids, 1, 100, false);
}
PERF_TEST_F(fetch_plan, t1p100_yes_auth_ids) {
    return run_bench(topic_ids, 1, 100, true);
}
PERF_TEST_F(fetch_plan, t100p1_no_auth_ids) {
    return run_bench(topic_ids, 100, 1, false);
}
PERF_TEST_F(fetch_plan, t100p1_yes_auth_ids) {
    return run_bench(topic_ids, 100, 1, true);
}
