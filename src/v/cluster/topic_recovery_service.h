/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_storage/recovery_errors.h"
#include "cloud_storage/recovery_request.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/topic_manifest.h"
#include "cluster/types.h"
#include "container/fragmented_vector.h"
#include "model/fundamental.h"

#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/http/reply.hh>
#include <seastar/http/request.hh>

#include <boost/circular_buffer.hpp>

namespace cluster {
class topics_frontend;
class topic_recovery_status_frontend;
class topic_table;
} // namespace cluster

namespace cloud_storage {

struct init_recovery_result {
    ss::http::reply::status_type status_code;
    ss::sstring message;

    bool operator==(const init_recovery_result&) const = default;
};

std::ostream& operator<<(std::ostream&, const init_recovery_result&);

struct recovery_task_config {
    cloud_storage_clients::bucket_name bucket;
    ss::lowres_clock::duration operation_timeout_ms;
    ss::lowres_clock::duration backoff_ms;
    static recovery_task_config make_config();
};

struct topic_download_counts {
    int pending_downloads;
    int successful_downloads;
    int failed_downloads;
};

std::ostream& operator<<(std::ostream&, const topic_download_counts&);

struct topic_recovery_service
  : ss::peering_sharded_service<topic_recovery_service> {
    enum class state {
        inactive,
        starting,
        scanning_bucket,
        creating_topics,
        recovering_data,
    };

    using download_counts
      = absl::flat_hash_map<model::topic_namespace, topic_download_counts>;

    struct recovery_status {
        state state;
        download_counts download_counts;
        std::optional<recovery_request> request;
    };

    static constexpr int shard_id = 0;

    topic_recovery_service(
      ss::sharded<remote>& remote,
      ss::sharded<cluster::topic_table>& topic_state,
      ss::sharded<cluster::topics_frontend>& topics_frontend,
      ss::sharded<cluster::topic_recovery_status_frontend>&
        topic_recovery_status_frontend);

    /// \brief A quick check if the recovery task is currently running. If true,
    /// no other recovery task should be launched.
    /// \return If a recovery task is already active on any shard in this node
    bool is_active() const { return _state != state::inactive; }

    ss::future<> stop();

    /// \brief Starts the recovery process as a background task. The incoming
    /// HTTP request is validated synchronously. Validation errors, if any, are
    /// returned immediately. If the request validation succeeds, a background
    /// recovery process is initiated.
    /// \return A result object with an HTTP status code and a message string,
    /// suitable for being returned as response to an HTTP call.
    ss::future<init_recovery_result> start_recovery(const ss::http::request&);

    /// \brief Stops the download check task. Intended to be stopped before the
    /// cloud storage API is stopped, so that any HTTP calls are not made using
    /// a non function remote.
    ss::future<> shutdown_recovery();

    state current_state() const { return _state; }

    /// \brief Returns the current status of recovery, including state, download
    /// counts and the request used to start recovery, if a recovery is active.
    recovery_status current_recovery_status() const;

    /// \brief Returns the last few statuses of the service. Useful for
    /// reviewing a history of actions this service has taken in the past.
    std::vector<recovery_status> recovery_status_log() const;

private:
    void set_state(state);

    /// \brief Returns a list of manifests for topics to create, filtering
    /// against existing topics in cluster. The manifests are downloaded from
    /// the bucket, and the downloads which fail are skipped from the recovery
    /// process.
    ss::future<std::vector<cloud_storage::topic_manifest>>
    filter_existing_topics(
      std::vector<remote_segment_path> items,
      const recovery_request& request,
      std::optional<model::ns> filter_ns);

    ss::future<std::vector<cluster::topic_result>>
    create_topics(const recovery_request& request);

    /// \brief Starts the background recovery process. This involves acquiring
    /// a lock, scanning the bucket for topic manifests, and finally creating
    /// the topics with the required parameters. \param The validated request
    /// received on the admin API
    ss::future<result<void, recovery_error_ctx>>
    start_bg_recovery_task(recovery_request request);

    /// \brief assigns a background task to the _pending_status_timer and exits
    /// immediately.
    void start_download_bg_tracker();

    /// \brief Acquires a lock before checking for downloads, to ensure that
    /// parallel checks are not running.
    ss::future<> check_for_downloads();

    /// \brief Scans the bucket for result files. Compares them against the
    /// pending downloads. If all downloads are finished, then clears the state
    /// and exits. If downloads are pending, rearms the timer so the check
    /// happens after some time.
    ss::future<> do_check_for_downloads();

    void populate_recovery_status();

    ss::future<> reset_topic_configurations();

    /// \brief Stores the current state in recovery status log.
    void push_status();

private:
    ss::gate _gate;
    ss::abort_source _as;
    ssx::semaphore _download_check_sem{1, "cst/topic_rec_dl_check"};

    // The state is used to ensure that recovery is only started if it is not
    // already running.
    state _state{state::inactive};
    recovery_task_config _config;
    ss::sharded<remote>& _remote;
    ss::sharded<cluster::topic_table>& _topic_state;
    ss::sharded<cluster::topics_frontend>& _topics_frontend;
    ss::sharded<cluster::topic_recovery_status_frontend>&
      _topic_recovery_status_frontend;
    // A map of the number of downloads expected per NTP. As downloads finish,
    // the number goes down. Once all downloads for all NTPs are finished
    // (whether successful or failures) the current recovery attempt ends.
    download_counts _download_counts;

    std::optional<recovery_request> _recovery_request;

    // Used to schedule the download check periodically.
    ss::timer<ss::lowres_clock> _pending_status_timer;

    // Maintains a list of manifests downloaded when recovery started, so that
    // any overrides per topic made for the recovery to progress can be reset
    // once the recovery has ended. One example is the topic retention which
    // could be set to some small value during recovery and restored back to
    // original value from manifest once recovery has ended.
    std::optional<chunked_vector<topic_manifest>> _downloaded_manifests;

    boost::circular_buffer<recovery_status> _status_log;
};

std::ostream&
operator<<(std::ostream&, const topic_recovery_service::recovery_status&);

std::ostream& operator<<(std::ostream&, const topic_recovery_service::state&);

} // namespace cloud_storage
