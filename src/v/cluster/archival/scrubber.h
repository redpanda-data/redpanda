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

#include "cloud_storage/anomalies_detector.h"
#include "cloud_storage/fwd.h"
#include "cluster/archival/fwd.h"
#include "cluster/archival/scrubber_scheduler.h"
#include "cluster/archival/types.h"
#include "features/feature_table.h"

#include <seastar/core/future.hh>

namespace archival {

/*
 * Scrubber housekeeping job managed by the upload_housekeeping_service.
 * Each cloud storage enabled partition gets its own scrubber which is owned
 * by the ntp_archiver_service.
 *
 * The goal of the scrubber is to periodically analyse the uploaded cloud
 * storage data and metadata, detect anomalies and report them to the cloud
 * storage layer. Detection is handled by the cloud_storage::anomalies_detector
 * class and anomalies are persisted on the partition's log and managed by the
 * partition manifset and archival STM.
 */
class scrubber : public housekeeping_job {
public:
    scrubber(
      ntp_archiver& archiver,
      cloud_storage::remote& remote,
      features::feature_table& feature_table,
      config::binding<bool> config_enabled,
      config::binding<std::chrono::milliseconds> partial_interval,
      config::binding<std::chrono::milliseconds> full_interval,
      config::binding<std::chrono::milliseconds> jitter);

    ss::future<> await_feature_enabled();

    ss::future<run_result> run(run_quota_t quota) override;

    void interrupt() override;

    bool interrupted() const override;

    ss::future<> stop() override;

    void set_enabled(bool) override;

    void acquire() override;
    void release() override;

    retry_chain_node* get_root_retry_chain_node() override;

    ss::sstring name() const override;

    std::pair<bool, std::optional<ss::sstring>> should_skip() const;

    // Reset the scheduler and pick a new time-point for the next scrub
    void reset_scheduler();

private:
    ss::abort_source _as;
    retry_chain_node _root_rtc;
    retry_chain_logger _logger;
    ss::gate _gate;

    // A gate holder we keep on behalf of the housekeeping service, when
    // it acquire()s us.
    std::optional<ss::gate::holder> _holder;

    // Whether the scrubbing housekeeping job is enabled (e.g. will be disabled
    // on non-leader nodes)
    bool _job_enabled{true};

    // Binding to cloud_storage_scrubbing_enabled cluster config
    config::binding<bool> _config_enabled;

    ntp_archiver& _archiver;
    cloud_storage::remote& _remote;

    features::feature_table& _feature_table;

    cloud_storage::anomalies_detector _detector;
    scrubber_scheduler<std::chrono::system_clock> _scheduler;
};

} // namespace archival
