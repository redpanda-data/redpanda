/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "archival/fwd.h"
#include "archival/scrubber_scheduler.h"
#include "archival/types.h"
#include "cloud_storage/base_manifest.h"
#include "cloud_storage/fwd.h"
#include "cluster/fwd.h"
#include "cluster/types.h"
#include "features/feature_table.h"
#include "random/simple_time_jitter.h"
#include "storage/fwd.h"

#include <seastar/core/future.hh>

namespace archival {

class scrubber : public housekeeping_job {
public:
    scrubber(
      ntp_archiver& archiver,
      cloud_storage::remote& remote,
      retry_chain_logger& logger,
      features::feature_table& feature_table,
      config::binding<bool> config_enabled,
      config::binding<std::chrono::milliseconds> interval,
      config::binding<std::chrono::milliseconds> jitter);

    ss::future<> await_feature_enabled();

    ss::future<run_result>
    run(retry_chain_node& rtc, run_quota_t quota) override;

    void interrupt() override;

    bool interrupted() const override;

    ss::future<> stop() override;

    void set_enabled(bool) override;

    void acquire() override;
    void release() override;

    ss::sstring name() const override;

    std::pair<bool, std::optional<ss::sstring>> should_skip() const;

    model::timestamp next_scrub_at() const;

private:
    ss::abort_source _as;
    ss::gate _gate;

    // A gate holder we keep on behalf of the housekeeping service, when
    // it acquire()s us.
    std::optional<ss::gate::holder> _holder;

    // Whether the scrubbing housekeeping job is enabled (e.g. will be disabled
    // on non-leader nodes)
    bool _job_enabled{true};

    // Binding to cloud_storage_scrubbing_enabled cluster config
    config::binding<bool> _config_enabled;

    [[maybe_unused]] ntp_archiver& _archiver;
    [[maybe_unused]] cloud_storage::remote& _remote;
    [[maybe_unused]] retry_chain_logger& _logger;

    features::feature_table& _feature_table;

    scrubber_scheduler<std::chrono::system_clock> _scheduler;
};

} // namespace archival
