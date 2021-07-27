/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#pragma once
#include "archival/ntp_archiver_service.h"
#include "cloud_storage/manifest.h"
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
#include <seastar/core/loop.hh>
#include <seastar/core/weak_ptr.hh>

#include <absl/container/node_hash_map.h>

#include <queue>

namespace archival {
namespace internal {

using namespace std::chrono_literals;

class ntp_upload_queue {
    struct upload_queue_item {
        ss::lw_shared_ptr<ntp_archiver> archiver;
        intrusive_list_hook _upl_hook;
    };
    using hash_map = absl::node_hash_map<model::ntp, upload_queue_item>;

public:
    using value = ss::lw_shared_ptr<ntp_archiver>;
    using key = model::ntp;
    using iterator = hash_map::iterator;

    ntp_upload_queue() = default;

    // Iteration
    iterator begin();
    iterator end();

    /// Insert new item into the queue
    void insert(value archiver);
    void erase(const key& ntp);
    bool contains(const key& ntp) const;
    value operator[](const key& ntp) const;
    /// Copy all archiver ntp into output iterator
    template<class FwdIt, class Func>
    void copy_if(FwdIt it, const Func& pred) const;

    /// Return number of archivers that queue contains
    size_t size() const noexcept;

    /// Get next upload candidate
    value get_upload_candidate();

private:
    hash_map _archivers;
    intrusive_list<upload_queue_item, &upload_queue_item::_upl_hook>
      _upload_queue;
};

template<class FwdIt, class Func>
void ntp_upload_queue::copy_if(FwdIt out, const Func& pred) const {
    for (const auto& [ntp, _] : _archivers) {
        if (pred(ntp)) {
            *out++ = ntp;
        }
    }
}

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
      ss::sharded<storage::api>& api,
      ss::sharded<cluster::partition_manager>& pm,
      ss::sharded<cluster::topic_table>& tt);
    scheduler_service_impl(
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
    static ss::future<archival::configuration> get_archival_service_config();

    /// Start archiver
    ss::future<> start();

    /// Stop archiver
    ss::future<> stop();

    void rearm_timer();

    /// Get next upload or delete candidate
    ss::lw_shared_ptr<ntp_archiver> get_upload_candidate();

    /// Run next round of uploads
    ss::future<> run_uploads();

    /// \brief Sync ntp-archivers with the content of the partition_manager
    ///
    /// This method can invoke asynchronous operations that can potentially
    /// be blocked on the semaphore. The operations are ntp_archiver::stop,
    /// and ntp_archiver::download_manifest. This only happens when new ntp
    /// appers in partition_manager or got removed from it.
    ss::future<> reconcile_archivers();

    /// Return range with all available ntps
    bool contains(const model::ntp& ntp) const { return _queue.contains(ntp); }

private:
    /// Remove archivers from the workingset
    ss::future<> remove_archivers(std::vector<model::ntp> to_remove);
    ss::future<> create_archivers(std::vector<model::ntp> to_create);
    ss::future<> upload_topic_manifest(
      model::topic_namespace topic_ns, model::revision_id rev);
    /// Adds archiver to the reconciliation loop after fetching its manifest.
    ss::future<ss::stop_iteration>
    add_ntp_archiver(ss::lw_shared_ptr<ntp_archiver> archiver);
    /// Returns high watermark for the partition
    std::optional<model::offset>
    get_high_watermark(const model::ntp& ntp) const;

    configuration _conf;
    ss::sharded<cluster::partition_manager>& _partition_manager;
    ss::sharded<cluster::topic_table>& _topic_table;
    ss::sharded<storage::api>& _storage_api;
    simple_time_jitter<ss::lowres_clock> _jitter;
    ss::timer<ss::lowres_clock> _timer;
    ss::gate _gate;
    ss::abort_source _as;
    ss::semaphore _stop_limit;
    ntp_upload_queue _queue;
    simple_time_jitter<ss::lowres_clock> _backoff{100ms};
    retry_chain_node _rtcnode;
    retry_chain_logger _rtclog;
    service_probe _probe;
    cloud_storage::remote _remote;
    ss::lowres_clock::duration _topic_manifest_upload_timeout;
    ss::lowres_clock::duration _initial_backoff;
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
};

} // namespace archival
