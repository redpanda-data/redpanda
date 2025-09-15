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

#include "cluster_link/link.h"
#include "cluster_link/model/types.h"
#include "cluster_link/task.h"
#include "cluster_link/tests/deps.h"
#include "kafka/data/rpc/deps.h"
#include "test_utils/test.h"

#include <gtest/gtest.h>

using namespace std::chrono_literals;

namespace cluster_link::tests {

namespace {

void validate_report(
  const model::cluster_link_task_status_report& report,
  const model::name_t& link_name,
  std::string_view task_name,
  model::task_state expected_state) {
    const auto link_task_state_report = report.link_reports.find(link_name);
    ASSERT_NE(link_task_state_report, report.link_reports.end())
      << "Link report for link " << link_name << " not found";

    const auto task_report = link_task_state_report->second.task_status_reports
                               .find(ss::sstring{task_name});
    ASSERT_NE(
      task_report, link_task_state_report->second.task_status_reports.end())
      << "Task report for task " << task_name << " not found in link "
      << link_name;

    ASSERT_EQ(task_report->second.task_state, expected_state);
}
} // namespace

class test_task : public task {
public:
    static constexpr auto name = "test_task";
    test_task(link* link, ss::lowres_clock::duration run_interval)
      : task(link, run_interval, name) {}

    void update_config(const model::metadata&) override {}
    bool should_start_impl(ss::shard_id, ::model::node_id) const override {
        return true;
    }

    bool should_stop_impl(ss::shard_id, ::model::node_id) const override {
        return false;
    }

    ss::future<> run_impl() override { return ss::now(); }
};

class controller_locked_test_task : public controller_locked_task {
public:
    static constexpr auto name = "test_task";
    controller_locked_test_task(
      link* link, ss::lowres_clock::duration run_interval)
      : controller_locked_task(link, run_interval, name) {}

    void update_config(const model::metadata&) override {}

    ss::future<> run_impl() override { return ss::now(); }
};

class test_task_factory : public task_factory {
public:
    explicit test_task_factory(bool is_locked_to_controller)
      : _is_locked_to_controller(is_locked_to_controller) {}
    std::string_view created_task_name() const noexcept override {
        return test_task::name;
    }

    std::unique_ptr<task> create_task(link* link) override {
        if (_is_locked_to_controller) {
            return std::make_unique<controller_locked_test_task>(link, 100ms);
        }
        return std::make_unique<test_task>(link, 100ms);
    }

private:
    bool _is_locked_to_controller;
};

struct test_parameters {
    bool is_locked_to_controller;

    friend std::ostream&
    operator<<(std::ostream& os, const test_parameters& tp) {
        return os << "{is_locked_to_controller: " << tp.is_locked_to_controller
                  << "}";
    }
};

class task_manager_integration_test
  : public ::testing::TestWithParam<test_parameters> {
public:
    static constexpr auto task_reconciler_interval = 1s;
    void SetUp() override {
        _clmtf = std::make_unique<cluster_link_manager_test_fixture>(self());
        _clmtf
          ->wire_up_and_start(
            std::make_unique<test_link_factory>(task_reconciler_interval))
          .get();
    }
    void TearDown() override {
        _clmtf->reset().get();
        _clmtf.reset();
    }

    cluster_link_manager_test_fixture* fixture() { return _clmtf.get(); }

    ss::future<std::optional<model::cluster_link_task_status_report>>
    await_status_report(
      ss::lowres_clock::duration timeout,
      ss::lowres_clock::duration backoff,
      std::function<bool(size_t)> predicate) {
        auto timeout_time = ss::lowres_clock::now() + timeout;
        while (ss::lowres_clock::now() < timeout_time) {
            auto report
              = fixture()->get_manager().local().get_task_status_report();
            if (predicate(report.link_reports.size())) {
                co_return report;
            }
            co_await ss::sleep(backoff);
        }

        co_return std::nullopt;
    }

    ::model::node_id self() { return ::model::node_id(0); }

private:
    std::unique_ptr<cluster_link_manager_test_fixture> _clmtf;
};

TEST_P(task_manager_integration_test, create_task_no_controller) {
    /// This test validates when no controller leader is present that:
    /// - Tasks that are locked to the controller are not started
    /// - Tasks that are not locked to the controller are started
    fixture()
      ->get_manager()
      .invoke_on_all([](manager& m) {
          m.register_task_factory<test_task_factory>(
             GetParam().is_locked_to_controller)
            .get();
      })
      .get();
    {
        auto report = fixture()->get_manager().local().get_task_status_report();
        ASSERT_TRUE(report.link_reports.empty());
    }

    auto link_name = model::name_t("test_link");

    model::metadata m1{
      .name = link_name,
      .uuid = model::uuid_t(::uuid_t::create()),
      .connection = model::connection_config{}};

    fixture()->upsert_link(std::move(m1)).get();
    auto report = await_status_report(5s, 100ms, [](size_t size) {
                      return size > 0;
                  }).get();
    ASSERT_TRUE(report.has_value()) << "Never received a task report";

    validate_report(
      *report,
      link_name,
      test_task::name,
      GetParam().is_locked_to_controller ? model::task_state::stopped
                                         : model::task_state::active);
}

TEST_P(task_manager_integration_test, create_task_with_controller) {
    /// This test validates when a controller leader is present that:
    /// - Tasks that are locked to the controller are started
    /// - Tasks that are not locked to the controller are started
    fixture()
      ->get_manager()
      .invoke_on_all([](manager& m) {
          m.register_task_factory<test_task_factory>(
             GetParam().is_locked_to_controller)
            .get();
      })
      .get();
    fixture()->elect_leader(::model::controller_ntp, self(), std::nullopt);
    auto link_name = model::name_t("test_link");

    model::metadata m1{
      .name = link_name,
      .uuid = model::uuid_t(::uuid_t::create()),
      .connection = model::connection_config{}};

    fixture()->upsert_link(std::move(m1)).get();
    auto report = await_status_report(5s, 100ms, [](size_t size) {
                      return size > 0;
                  }).get();
    ASSERT_TRUE(report.has_value()) << "Never received a task report";

    validate_report(
      *report, link_name, test_task::name, model::task_state::active);
}

TEST_P(task_manager_integration_test, controller_leadership_moved_on) {
    /// This test validates that when the controller leadership moves onto the
    /// shard that tasks that are
    /// - Locked to the controller are started
    /// - Not locked to the controller are started

    // This test starts off with no controller leader elected, creates the task,
    // and then 'elects' a controller leader

    fixture()
      ->get_manager()
      .invoke_on_all([](manager& m) {
          m.register_task_factory<test_task_factory>(
             GetParam().is_locked_to_controller)
            .get();
      })
      .get();

    auto link_name = model::name_t("test_link");

    model::metadata m1{
      .name = link_name,
      .uuid = model::uuid_t(::uuid_t::create()),
      .connection = model::connection_config{}};

    fixture()->upsert_link(std::move(m1)).get();
    auto report = await_status_report(5s, 100ms, [](size_t size) {
                      return size > 0;
                  }).get();
    ASSERT_TRUE(report.has_value()) << "Never received a task report";

    validate_report(
      *report,
      link_name,
      test_task::name,
      GetParam().is_locked_to_controller ? model::task_state::stopped
                                         : model::task_state::active);

    // Register for a callback from the task to alert us if the task changes
    // state.  If the task is not locked to the controller, then no state change
    // should be reported, however if it is locked to the controller, then the
    // state should change from stopped to active
    ss::condition_variable cv;

    task::state_change change;
    auto notif_id
      = dynamic_cast<test_link_factory*>(fixture()->get_link_factory())
          ->get_link(link_name)
          .value()
          ->register_for_task_state_changes(
            [&cv, &change, &link_name, task_name = test_task::name](
              model::name_t cb_link_name,
              std::string_view cb_task_name,
              task::state_change state_change) {
                if (cb_link_name == link_name && cb_task_name == task_name) {
                    change = std::move(state_change);
                    cv.signal();
                }
            });
    auto remove_callback = ss::defer([this, notif_id, &link_name] {
        dynamic_cast<test_link_factory*>(fixture()->get_link_factory())
          ->get_link(link_name)
          .value()
          ->unregister_for_task_state_changes(notif_id);
    });

    fixture()->elect_leader(::model::controller_ntp, self(), std::nullopt);
    try {
        cv.wait(1s).get();
        EXPECT_EQ(change.prev, model::task_state::stopped);
        EXPECT_EQ(change.cur, model::task_state::active);
    } catch (const ss::condition_variable_timed_out&) {
        if (GetParam().is_locked_to_controller) {
            FAIL() << "Task should have started after controller leadership "
                      "moved to "
                      "this shard";
        } else {
            // this is expected - should already have been running and have no
            // state change
        }
    }
}

TEST_P(task_manager_integration_test, controller_leadership_move_off) {
    /// This test validates that when the controller leadership moves off of the
    /// shard that
    /// - Tasks that are locked to the controller are stopped
    /// - Tasks that are not locked to the controller are still running

    // This test starts of with a controller leader elected, creates the tasks
    // and then moves leadership

    fixture()
      ->get_manager()
      .invoke_on_all([](manager& m) {
          m.register_task_factory<test_task_factory>(
             GetParam().is_locked_to_controller)
            .get();
      })
      .get();

    fixture()->elect_leader(::model::controller_ntp, self(), std::nullopt);
    auto link_name = model::name_t("test_link");

    model::metadata m1{
      .name = link_name,
      .uuid = model::uuid_t(::uuid_t::create()),
      .connection = model::connection_config{}};

    fixture()->upsert_link(std::move(m1)).get();

    auto report = await_status_report(5s, 100ms, [](size_t size) {
                      return size > 0;
                  }).get();
    ASSERT_TRUE(report.has_value()) << "Never received a task report";

    validate_report(
      *report, link_name, test_task::name, model::task_state::active);

    // Register for a callback from the task to alert us if the task changes
    // state.  If the task is not locked to the controller, then no state change
    // should be reported, however if it is locked to the controller, then the
    // state should change from active to stopped
    ss::condition_variable cv;

    task::state_change change;
    auto notif_id
      = dynamic_cast<test_link_factory*>(fixture()->get_link_factory())
          ->get_link(link_name)
          .value()
          ->register_for_task_state_changes(
            [&cv, &change, &link_name, task_name = test_task::name](
              model::name_t cb_link_name,
              std::string_view cb_task_name,
              task::state_change state_change) {
                if (cb_link_name == link_name && cb_task_name == task_name) {
                    change = std::move(state_change);
                    cv.signal();
                }
            });
    auto remove_callback = ss::defer([this, notif_id, &link_name] {
        dynamic_cast<test_link_factory*>(fixture()->get_link_factory())
          ->get_link(link_name)
          .value()
          ->unregister_for_task_state_changes(notif_id);
    });

    fixture()->elect_leader(::model::controller_ntp, self() + 1, std::nullopt);

    try {
        cv.wait(1s).get();
        EXPECT_EQ(change.prev, model::task_state::active);
        EXPECT_EQ(change.cur, model::task_state::stopped);
    } catch (const ss::condition_variable_timed_out&) {
        if (GetParam().is_locked_to_controller) {
            FAIL() << "Task should have halted after controller leadership "
                      "moved to "
                      "this shard";
        } else {
            // this is expected - should already have been running and have no
            // state change
        }
    }
}

INSTANTIATE_TEST_SUITE_P(
  tracks_with_controller_leader,
  task_manager_integration_test,
  ::testing::Values(
    test_parameters{.is_locked_to_controller = true},
    test_parameters{.is_locked_to_controller = false}));

/// Class that doesn't start/stop on the first try
class evil_task : public controller_locked_task {
public:
    static constexpr auto name = "evil_task";

    using controller_locked_task::controller_locked_task;

    void update_config(const model::metadata&) override {}

    ss::future<> run_impl() override { return ss::now(); }

    ss::future<result<void>> start() override {
        static unsigned num_calls = 0;
        if (num_calls < 2) {
            num_calls++;
            throw std::runtime_error("Evil task start method failed");
        } else {
            num_calls = 0; // reset for next start
            co_return co_await task::start();
        }
    }
};

class evil_task_factory : public task_factory {
public:
    using task_factory::task_factory;

    std::string_view created_task_name() const noexcept override {
        return evil_task::name;
    }

    std::unique_ptr<task> create_task(link* link) override {
        return std::make_unique<evil_task>(link, 100ms, evil_task::name);
    }
};

class evil_task_manager_integration_test : public seastar_test {
public:
    static constexpr auto task_reconciler_interval = 1s;

    ss::future<> SetUpAsync() override {
        _clmtf = std::make_unique<cluster_link_manager_test_fixture>(self());
        co_await _clmtf->wire_up_and_start(
          std::make_unique<test_link_factory>(task_reconciler_interval));
    }

    ss::future<> TearDownAsync() override {
        co_await _clmtf->reset();
        _clmtf.reset();
    }

    cluster_link_manager_test_fixture* fixture() { return _clmtf.get(); }

    ::model::node_id self() { return ::model::node_id(0); }

    ss::future<std::optional<model::cluster_link_task_status_report>>
    await_status_report(
      ss::lowres_clock::duration timeout,
      ss::lowres_clock::duration backoff,
      std::function<bool(const model::cluster_link_task_status_report&)>
        predicate) {
        auto timeout_time = ss::lowres_clock::now() + timeout;
        while (ss::lowres_clock::now() < timeout_time) {
            auto report
              = fixture()->get_manager().local().get_task_status_report();
            if (predicate(report)) {
                co_return report;
            }
            co_await ss::sleep(backoff);
        }

        co_return std::nullopt;
    }

private:
    std::unique_ptr<cluster_link_manager_test_fixture> _clmtf;
};

TEST_F_CORO(
  evil_task_manager_integration_test, evil_task_start_stop_leader_move_off) {
    /// This test verifies that when a task is supposed to start/stop but fails,
    /// the task reconciliation mechanism can recover from the failure.

    co_await fixture()->get_manager().invoke_on_all(
      [](manager& m) { return m.register_task_factory<evil_task_factory>(); });

    fixture()->elect_leader(::model::controller_ntp, self(), std::nullopt);

    auto link_name = model::name_t("link1");

    model::metadata m1{
      .name = link_name,
      .uuid = model::uuid_t(::uuid_t::create()),
      .connection = model::connection_config{}};

    co_await fixture()->upsert_link(std::move(m1));

    auto report = co_await await_status_report(
      5s, 100ms, [](const model::cluster_link_task_status_report& report) {
          return report.link_reports.size() > 0;
      });

    // Expect that the status report should not be running yet
    validate_report(
      *report, link_name, evil_task::name, model::task_state::stopped);

    // Await the task reconciler interval time (plus a fudge) to allow the loop
    // to run to start the task
    co_await ss::sleep(task_reconciler_interval * 2 + 100ms);

    report = co_await await_status_report(
      5s, 100ms, [](const model::cluster_link_task_status_report& report) {
          return report.link_reports.size() > 0;
      });

    // Expect that the status report should be running now
    validate_report(
      *report, link_name, evil_task::name, model::task_state::active);

    // Now move the controller leadership off this shard
    fixture()->elect_leader(::model::controller_ntp, self() + 1, std::nullopt);

    report = co_await await_status_report(
      5s, 100ms, [](const model::cluster_link_task_status_report& report) {
          return report.link_reports.size() > 0;
      });

    // Expect that the status report should be running now
    validate_report(
      *report, link_name, evil_task::name, model::task_state::active);

    // Await the task reconciler interval time (plus a fudge) to allow the loop
    // to run to stop the task
    co_await ss::sleep(task_reconciler_interval * 2 + 100ms);

    report = co_await await_status_report(
      5s, 100ms, [](const model::cluster_link_task_status_report& report) {
          return report.link_reports.size() > 0;
      });

    // Expect that the status report should be running now
    validate_report(
      *report, link_name, evil_task::name, model::task_state::stopped);
}

TEST_F_CORO(
  evil_task_manager_integration_test, evil_task_start_stop_leader_move_on) {
    /// This test verifies that when a task is supposed to start/stop but fails,
    /// the task reconciliation mechanism can recover from the failure.

    co_await fixture()->get_manager().invoke_on_all(
      [](manager& m) { return m.register_task_factory<evil_task_factory>(); });

    auto link_name = model::name_t("link1");

    model::metadata m1{
      .name = link_name,
      .uuid = model::uuid_t(::uuid_t::create()),
      .connection = model::connection_config{}};

    co_await fixture()->upsert_link(std::move(m1));

    auto report = co_await await_status_report(
      5s, 100ms, [](const model::cluster_link_task_status_report& report) {
          return report.link_reports.size() > 0;
      });

    // Expect that the status report should not be running yet
    validate_report(
      *report, link_name, evil_task::name, model::task_state::stopped);

    fixture()->elect_leader(::model::controller_ntp, self(), std::nullopt);

    // Await the task reconciler interval time (plus a fudge) to allow the loop
    // to run to start the task
    co_await ss::sleep(task_reconciler_interval * 2 + 100ms);

    report = co_await await_status_report(
      5s, 100ms, [](const model::cluster_link_task_status_report& report) {
          return report.link_reports.size() > 0;
      });

    // Expect that the status report should be running now
    validate_report(
      *report, link_name, evil_task::name, model::task_state::active);
}

TEST_F_CORO(
  evil_task_manager_integration_test, evil_task_register_after_start) {
    /// This test verifies the of registering a task after a link is created

    fixture()->elect_leader(::model::controller_ntp, self(), std::nullopt);

    auto link_name = model::name_t("link1");

    model::metadata m1{
      .name = link_name,
      .uuid = model::uuid_t(::uuid_t::create()),
      .connection = model::connection_config{}};

    co_await fixture()->upsert_link(std::move(m1));

    co_await fixture()->get_manager().invoke_on_all(
      [](manager& m) { return m.register_task_factory<evil_task_factory>(); });

    auto report = co_await await_status_report(
      5s,
      100ms,
      [&link_name](const model::cluster_link_task_status_report& report) {
          return report.link_reports.contains(link_name)
                 && report.link_reports.at(link_name)
                      .task_status_reports.contains(evil_task::name);
      });

    // Expect that the status report should not be running yet
    validate_report(
      *report, link_name, evil_task::name, model::task_state::stopped);

    // Await the task reconciler interval time (plus a fudge) to allow the loop
    // to run to start the task
    co_await ss::sleep(task_reconciler_interval * 2 + 100ms);

    report = co_await await_status_report(
      5s,
      100ms,
      [&link_name](const model::cluster_link_task_status_report& report) {
          return report.link_reports.contains(link_name)
                 && report.link_reports.at(link_name)
                      .task_status_reports.contains(evil_task::name);
      });

    // Expect that the status report should be running now
    validate_report(
      *report, link_name, evil_task::name, model::task_state::active);
}

} // namespace cluster_link::tests
