/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once
#include "archival/ntp_archiver_service.h"
#include "cluster/partition_manager.h"
#include "model/fundamental.h"
#include "s3/client.h"
#include "storage/api.h"
#include "storage/log_manager.h"
#include "storage/ntp_config.h"
#include "storage/segment.h"
#include "storage/segment_set.h"
#include "utils/intrusive_list_helpers.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/io_priority_class.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/core/weak_ptr.hh>

#include <absl/container/node_hash_map.h>

#include <queue>

namespace archival {
namespace internal {

using namespace std::chrono_literals;

/// Shard-local archiver service.
/// The service maintains a working set of archivers. Every archiver maintains a
/// single partition. The working set get reconciled periodically. Unused
/// archivers gets removed and new are added. The service runs a simple
/// workflow on its working set:
/// - Reconcile working set
/// - Choose next candidate(s) archiver(s)
/// - Start configured number of uploads
/// - Re-upload manifest(s)
/// - Reset timer
class scheduler_service_impl {
public:
    /// \brief create scheduler service
    ///
    /// \param configuration is a archival cnfiguration
    /// \param api is a storage api service instance
    /// \param pm is a partition_manager service instance
    scheduler_service_impl(
      const configuration& conf,
      ss::sharded<cloud_storage::remote>& remote,
      ss::sharded<storage::api>& api,
      ss::sharded<cluster::partition_manager>& pm,
      ss::sharded<cluster::topic_table>& tt);
    scheduler_service_impl(
      ss::sharded<cloud_storage::remote>& remote,
      ss::sharded<storage::api>& api,
      ss::sharded<cluster::partition_manager>& pm,
      ss::sharded<cluster::topic_table>& tt,
      ss::sharded<archival::configuration>& configs);

    /// \brief Configure scheduler service
    ///
    /// \param c is a service configuration
    /// \note two step initialization is needed when the
    ///       service is initialized in ss::sharded container
    void configure(configuration c);

    /// \brief create scheduler service config
    /// This mehtod will use shard-local redpanda configuration
    /// to generate the configuration.
    /// \param sg is a scheduling group used to run all uploads
    /// \param p is an io priority class used to throttle upload file reads
    static ss::future<archival::configuration> get_archival_service_config(
      ss::scheduling_group sg = ss::default_scheduling_group(),
      ss::io_priority_class p = ss::default_priority_class());

    /// Start archiver
    ss::future<> start();

    /// Stop archiver
    ss::future<> stop();

    void rearm_timer();

    /// \brief Sync ntp-archivers with the content of the partition_manager
    ///
    /// This method can invoke asynchronous operations that can potentially
    /// be blocked on the semaphore. The operations are ntp_archiver::stop,
    /// and ntp_archiver::download_manifest. This only happens when new ntp
    /// appers in partition_manager or got removed from it.
    ss::future<> reconcile_archivers();

    /// Return true if there is an archiver for the given ntp.
    bool contains(const model::ntp& ntp) const {
        return _archivers.contains(ntp);
    }

    /// Get remote that service uses
    cloud_storage::remote& get_remote();

    /// Get configured bucket
    s3::bucket_name get_bucket() const;

    /// Total size of data that have to be uploaded
    uint64_t estimate_backlog_size();

private:
    /// Remove archivers from the workingset
    ss::future<> remove_archivers(std::vector<model::ntp> to_remove);
    ss::future<> create_archivers(std::vector<model::ntp> to_create);
    ss::future<> upload_topic_manifest(
      model::topic_namespace topic_ns, model::initial_revision_id rev);
    /// Adds archiver to the reconciliation loop after fetching its manifest.
    ss::future<ss::stop_iteration>
    add_ntp_archiver(ss::lw_shared_ptr<ntp_archiver> archiver);

    configuration _conf;
    ss::sharded<cluster::partition_manager>& _partition_manager;
    ss::sharded<cluster::topic_table>& _topic_table;
    ss::sharded<storage::api>& _storage_api;
    simple_time_jitter<ss::lowres_clock> _jitter;
    ss::timer<ss::lowres_clock> _timer;
    ss::gate _gate;
    ss::semaphore _stop_limit;
    absl::btree_map<model::ntp, ss::lw_shared_ptr<ntp_archiver>> _archivers;
    retry_chain_node _rtcnode;
    retry_chain_logger _rtclog;
    service_probe _probe;
    ss::sharded<cloud_storage::remote>& _remote;
    ss::lowres_clock::duration _topic_manifest_upload_timeout;
    ss::lowres_clock::duration _initial_backoff;
    ss::scheduling_group _upload_sg;
};

} // namespace internal

/// Archiver service implementation
class scheduler_service : internal::scheduler_service_impl {
public:
    /// \brief create scheduler service
    ///
    /// \param configuration is a archival cnfiguration
    /// \param api is a storage api service instance
    /// \param pm is a partition_manager service instance
    using internal::scheduler_service_impl::scheduler_service_impl;

    /// Start service
    using internal::scheduler_service_impl::start;

    /// Stop service
    using internal::scheduler_service_impl::stop;

    /// \brief Configure scheduler service
    ///
    /// \param c is a service configuration
    /// \note two step initialization is needed when the
    ///       service is initialized in ss::sharded container
    using internal::scheduler_service_impl::configure;

    /// Generate configuration
    using internal::scheduler_service_impl::get_archival_service_config;

    /// Get remote that service uses
    using internal::scheduler_service_impl::get_remote;

    /// Get configured bucket
    using internal::scheduler_service_impl::get_bucket;

    /// Estimate total size of the upload backlog
    using internal::scheduler_service_impl::estimate_backlog_size;
};

} // namespace archival
