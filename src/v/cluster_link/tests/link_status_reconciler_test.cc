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

#include "cluster_link/link_status_reconciler.h"
#include "cluster_link/producer_id_barrier.h"
#include "cluster_link/tests/deps.h"
#include "test_utils/async.h"
#include "test_utils/test.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>

#include <absl/container/flat_hash_set.h>
#include <gtest/gtest.h>

using namespace std::chrono_literals;

namespace cluster_link::tests {
namespace {

static const auto link_name = model::name_t("test_link");

class fake_pid_barrier final : public producer_id_barrier {
public:
    ss::future<result<::model::producer_id, errc>> advance() noexcept final {
        ++calls;
        if (block) {
            co_await block->get_future();
            block.reset();
        }
        co_return next_result;
    }

    int calls{0};
    result<::model::producer_id, errc> next_result{::model::producer_id{0}};
    // When set, the next advance() parks until the promise is resolved,
    // letting tests race reconciler shutdown against an in-flight barrier.
    std::optional<ss::promise<>> block;
};

/// A registry whose health report declares every partition leader ready,
/// except for explicitly held-back topics.
class ready_report_registry final : public test_link_registry {
public:
    using test_link_registry::test_link_registry;

    ss::future<std::expected<
      ::cluster_link::model::aggregated_shadow_topic_report,
      errc>>
    shadow_topic_report(
      const model::id_t& id, const ::model::topic& topic) override {
        ::cluster_link::model::aggregated_shadow_topic_report report;
        report.total_partitions = 1;
        if (not_ready_topics.contains(topic)) {
            // No leaders reported: the readiness check must hold this topic
            // back.
            co_return report;
        }
        auto rev = get_last_update_revision(id);
        ::cluster_link::model::aggregated_shadow_topic_report::broker_report
          broker{
            .broker = ::model::node_id{0},
            .link_update_revision = rev.value_or(::model::revision_id{0}),
        };
        broker.leaders.push_back({.partition = ::model::partition_id{0}});
        report.brokers.push_back(std::move(broker));
        co_return report;
    }

    absl::flat_hash_set<::model::topic> not_ready_topics;
};

} // namespace

class link_status_reconciler_test : public seastar_test {
public:
    ss::future<> SetUpAsync() override {
        co_await _table.start();
        _registry = std::make_unique<ready_report_registry>(&_table.local());
        _reconciler = std::make_unique<link_status_reconciler>(
          _registry.get(), &_barrier, ::model::term_id{1});
        co_await _reconciler->start();

        co_await upsert_link(
          model::metadata{
            .name = link_name, .uuid = model::uuid_t(::uuid_t::create())});
        auto ids = _registry->get_all_link_ids();
        ASSERT_EQ_CORO(ids.size(), 1);
        _link_id = ids[0];
    }

    ss::future<> TearDownAsync() override {
        if (_reconciler) {
            co_await _reconciler->stop();
            _reconciler.reset();
        }
        _registry.reset();
        co_await _table.stop();
    }

    ss::future<> upsert_link(model::metadata md) {
        auto id = model::id_t(_next_link_id++);
        return ss::do_with(
          id, std::move(md), [this](model::id_t& id, model::metadata& md) {
              return _table.invoke_on_all([id, &md](
                                            cluster::cluster_link::table& t) {
                  return md.copy().then([id, &t](model::metadata md) {
                      return t
                        .apply_update(
                          cluster::cluster_link::testing::create_upsert_command(
                            ::model::offset{id()}, std::move(md)))
                        .then([](std::error_code ec) {
                            vassert(
                              ec.value() == 0,
                              "failed to upsert link: {}",
                              ec.message());
                        });
                  });
              });
          });
    }

    ss::future<> add_mirror_topic(const ::model::topic& topic) {
        model::mirror_topic_metadata tpmd{
          .status = model::mirror_topic_status::active,
          .source_topic_name = topic,
          .destination_topic_id = ::model::topic_id{::uuid_t::create()},
          .partition_count = 1,
          .replication_factor = 1,
        };
        auto batch
          = cluster::cluster_link::testing::create_add_mirror_topic_command(
            _link_id,
            model::add_mirror_topic_cmd{
              .topic = topic, .metadata = std::move(tpmd)});
        auto ec = co_await _table.local().apply_update(std::move(batch));
        vassert(
          ec.value() == 0, "failed to add mirror topic: {}", ec.message());
    }

    ss::future<> start_failover() {
        auto ec = co_await _registry->failover_link_topics(_link_id, 5s);
        ASSERT_EQ_CORO(ec, cluster::cluster_link::errc::success);
    }

    model::mirror_topic_status status_of(const ::model::topic& topic) {
        auto topics = _registry->get_mirror_topics_for_link(_link_id);
        vassert(topics.has_value(), "link disappeared");
        auto it = topics->find(topic);
        vassert(it != topics->end(), "topic disappeared");
        return it->second.status;
    }

protected:
    ss::sharded<cluster::cluster_link::table> _table;
    std::unique_ptr<ready_report_registry> _registry;
    fake_pid_barrier _barrier;
    std::unique_ptr<link_status_reconciler> _reconciler;
    model::id_t _link_id;
    int _next_link_id{1};
};

TEST_F_CORO(link_status_reconciler_test, barrier_error_blocks_promotion) {
    const auto topic = ::model::topic("topic-a");
    co_await add_mirror_topic(topic);
    co_await start_failover();
    _barrier.next_result = errc::rpc_error;

    _reconciler->reconcile();
    co_await ss::sleep(2500ms);

    // The readiness check passes, so the barrier is being consulted — and
    // its failure must leave the topic unpromoted.
    ASSERT_GE_CORO(_barrier.calls, 1);
    ASSERT_EQ_CORO(status_of(topic), model::mirror_topic_status::failing_over);

    // The next successful barrier lets the retrying reconciler promote.
    _barrier.next_result = ::model::producer_id{0};
    co_await ::tests::cooperative_spin_wait_with_timeout(10s, [&] {
        return status_of(topic) == model::mirror_topic_status::failed_over;
    });
}

TEST_F_CORO(link_status_reconciler_test, one_barrier_call_promotes_batch) {
    const auto topic_a = ::model::topic("topic-a");
    const auto topic_b = ::model::topic("topic-b");
    co_await add_mirror_topic(topic_a);
    co_await add_mirror_topic(topic_b);
    co_await start_failover();

    _reconciler->reconcile();
    co_await ::tests::cooperative_spin_wait_with_timeout(10s, [&] {
        return status_of(topic_a) == model::mirror_topic_status::failed_over
               && status_of(topic_b) == model::mirror_topic_status::failed_over;
    });
    // Both ready topics went through a single cluster-wide barrier.
    ASSERT_EQ_CORO(_barrier.calls, 1);
}

TEST_F_CORO(link_status_reconciler_test, not_ready_topics_are_excluded) {
    const auto topic_a = ::model::topic("topic-a");
    const auto topic_b = ::model::topic("topic-b");
    co_await add_mirror_topic(topic_a);
    co_await add_mirror_topic(topic_b);
    _registry->not_ready_topics.insert(topic_b);
    co_await start_failover();

    _reconciler->reconcile();
    co_await ::tests::cooperative_spin_wait_with_timeout(10s, [&] {
        return status_of(topic_a) == model::mirror_topic_status::failed_over;
    });
    ASSERT_EQ_CORO(
      status_of(topic_b), model::mirror_topic_status::failing_over);

    // Later ticks have nothing ready, so the barrier is not re-run for the
    // held-back topic.
    auto calls_after_promotion = _barrier.calls;
    co_await ss::sleep(2500ms);
    ASSERT_EQ_CORO(_barrier.calls, calls_after_promotion);
    ASSERT_EQ_CORO(
      status_of(topic_b), model::mirror_topic_status::failing_over);
}

TEST_F_CORO(link_status_reconciler_test, stop_during_barrier_blocks_promotion) {
    const auto topic = ::model::topic("topic-a");
    co_await add_mirror_topic(topic);
    co_await start_failover();
    // Park the barrier so reconciler shutdown (the stepdown path) races an
    // in-flight advance().
    _barrier.block.emplace();

    _reconciler->reconcile();
    co_await ::tests::cooperative_spin_wait_with_timeout(
      10s, [&] { return _barrier.calls == 1; });

    // Begin shutdown while the barrier is still in flight, then let the
    // barrier complete successfully. The stopped reconciler must not act on
    // the result.
    auto stop_fut = _reconciler->stop();
    _barrier.block->set_value();
    co_await std::move(stop_fut);
    _reconciler.reset();

    ASSERT_EQ_CORO(status_of(topic), model::mirror_topic_status::failing_over);
}

TEST_F_CORO(link_status_reconciler_test, no_pending_topics_no_barrier) {
    const auto topic = ::model::topic("topic-a");
    co_await add_mirror_topic(topic);

    _reconciler->reconcile();
    co_await ss::sleep(1500ms);

    ASSERT_EQ_CORO(_barrier.calls, 0);
    ASSERT_EQ_CORO(status_of(topic), model::mirror_topic_status::active);
}

} // namespace cluster_link::tests
