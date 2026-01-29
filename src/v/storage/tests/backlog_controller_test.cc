// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/seastarx.h"
#include "config/property.h"
#include "storage/backlog_controller.h"
#include "test_utils/async.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/metrics_api.hh>
#include <seastar/core/metrics_registration.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/sstring.hh>
#include <seastar/util/log.hh>

#include <gtest/gtest.h>

#include <cstdint>

using namespace std::chrono_literals; // NOLINT
static ss::logger ctrl_logger{"test-controller"};

struct simple_backlog_sampler : storage::backlog_controller::sampler {
    explicit simple_backlog_sampler(int64_t& b)
      : current_backlog(b) {}
    ss::future<int64_t> sample_backlog() final { co_return current_backlog; }

    int64_t& current_backlog;
};

class backlog_controller_fixture : public ::testing::Test {
    using labels_type = ss::metrics::impl::labels_type;
    using label_value = labels_type::mapped_type;
    const labels_type sch_group_label = {
      {"group", label_value{"sch_control_gr"}}, {"shard", label_value{"0"}}};

public:
    backlog_controller_fixture() {
        sg = ss::create_scheduling_group("sch_control_gr", 100).get();
        /**
         * Controller settings:
         *
         * proportional coefficient kp = -1.0 - negative reverse proportional to
         * backlog integral coefficient ki = 0.4 derivative coefficient kd = 0.2
         * initial setpoint sp = 20
         * initial shares = 100
         * min shares = 10
         * max shares = 800
         */
        auto mb = [](auto v) { return config::mock_binding(std::move(v)); };
        auto sp = []() { return 20; };
        auto cfg = storage::backlog_controller_config(
          mb(-1.0),
          mb(0.4),
          mb(0.2),
          1,
          std::move(sp),
          100,
          mb(10ms),
          sg,
          mb(int16_t{10}),
          mb(int16_t{800}));

        ctrl = std::make_unique<storage::backlog_controller>(
          std::make_unique<simple_backlog_sampler>(backlog),
          ctrl_logger,
          std::move(cfg));
    }

    ~backlog_controller_fixture() { ctrl->stop().get(); }

protected:
    ss::scheduling_group sg;
    std::unique_ptr<storage::backlog_controller> ctrl;
    int64_t backlog{0};

    int get_current_cpu_shares() const {
        auto metrics = ss::metrics::impl::get_value_map();
        auto it = std::find_if(
          metrics.begin(),
          metrics.end(),
          [](const ss::metrics::impl::value_map::value_type& p) {
              return p.first == "scheduler_shares";
          });

        auto m = it->second.find(sch_group_label);
        return m->second->get_function()().i();
    }
};

TEST_F(backlog_controller_fixture, test_feedback_loop) {
    ctrl->start().get();
    /**
     * current backlog is equal to 0, setpoint is set to 20, error = 20.0
     * since error hasn't changed and want some more backlog we will down
     * schedule the group until min limit is reached
     */
    tests::cooperative_spin_wait_with_timeout(1500ms, [this] {
        return get_current_cpu_shares() == 10;
    }).get();

    /**
     * huge change in backlog size should cause immediate reaction of controller
     */
    backlog = 200;

    tests::cooperative_spin_wait_with_timeout(1500ms, [this] {
        return get_current_cpu_shares() == 800;
    }).get();
}
