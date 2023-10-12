// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once
#include "cluster/fwd.h"
#include "config/property.h"
#include "kafka/client/fwd.h"
#include "kafka/client/types.h"
#include "model/timeout_clock.h"
#include "net/types.h"
#include "security/audit/schemas/schemas.h"
#include "security/audit/types.h"
#include "ssx/semaphore.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>

#include <boost/multi_index/hashed_index.hpp>
#include <boost/multi_index/indexed_by.hpp>
#include <boost/multi_index/key.hpp>
#include <boost/multi_index/member.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index/sequenced_index.hpp>
#include <boost/multi_index_container.hpp>

using namespace std::chrono_literals;

namespace security::audit {

template<typename T>
concept InheritsFromOCSFBase
  = std::is_base_of<security::audit::ocsf_base_event<T>, T>::value;

class audit_sink;

class audit_log_manager
  : public ss::peering_sharded_service<audit_log_manager> {
public:
    audit_log_manager(
      cluster::controller* controller, kafka::client::configuration&);

    audit_log_manager(const audit_log_manager&) = delete;
    audit_log_manager& operator=(const audit_log_manager&) = delete;
    audit_log_manager(audit_log_manager&&) = delete;
    audit_log_manager& operator=(audit_log_manager&&) = delete;
    ~audit_log_manager();

    /// Start the underlying kafka client and create the audit log topic
    /// if necessary
    ss::future<> start();

    /// Shuts down the internal kafka client and stops all pending bg work
    ss::future<> stop();

    /// Enqueue an event to be produced onto an audit log partition
    ///
    /// Returns: bool representing if the audit msg was successfully moved into
    /// the queue or not. If unsuccessful this means the audit subsystem cannot
    /// publish messages. Consumers of this API should react accordingly, i.e.
    /// return an error to the client.
    template<InheritsFromOCSFBase T, typename... Args>
    bool enqueue_audit_event(event_type type, Args&&... args) {
        if (!_audit_enabled() || !is_audit_event_enabled(type)) {
            return true;
        }
        if (_as.abort_requested()) {
            /// Prevent auditing new messages when shutdown starts that way the
            /// queue may be entirely flushed before shutdown
            return false;
        }
        return do_enqueue_audit_event(
          std::make_unique<T>(std::forward<Args>(args)...));
    }

    /// Returns the number of items pending to be written to auditing log
    ///
    /// Note does not include records already sent to client
    size_t pending_events() const { return _queue.size(); };

    /// Returns true if the internal kafka client is allocated
    ///
    /// NOTE: Only works on shard_id{0}, use in unit tests
    bool is_client_enabled() const;

private:
    ss::future<> drain();
    ss::future<> pause();
    ss::future<> resume();

    bool is_audit_event_enabled(event_type) const;
    bool do_enqueue_audit_event(
      std::unique_ptr<security::audit::ocsf_base_impl> msg);
    void set_enabled_events();

private:
    /// Multi index container is efficent in terms of time and space, underlying
    /// internal data structures are compact requiring only one node per
    /// element. More info here:
    /// https://www.boost.org/doc/libs/1_72_0/libs/multi_index/doc/performance.html
    struct underlying_list {};
    struct underlying_unordered_map {};
    using underlying_t = boost::multi_index::multi_index_container<
      std::unique_ptr<security::audit::ocsf_base_impl>,
      boost::multi_index::indexed_by<
        /// Sequenced list of entries
        boost::multi_index::sequenced<boost::multi_index::tag<underlying_list>>,
        /// Set of audit_messages using hashed representations as comparator
        boost::multi_index::hashed_unique<
          boost::multi_index::tag<underlying_unordered_map>,
          boost::multi_index::const_mem_fun<
            security::audit::ocsf_base_impl,
            size_t,
            &security::audit::ocsf_base_impl::key>>>>;

    /// configuration options
    config::binding<bool> _audit_enabled;
    config::binding<std::chrono::milliseconds> _queue_drain_interval_ms;
    config::binding<size_t> _max_queue_elements_per_shard;
    config::binding<std::vector<ss::sstring>> _audit_event_types;
    static constexpr auto enabled_set_bitlength
      = std::underlying_type_t<event_type>(event_type::num_elements);
    std::bitset<enabled_set_bitlength> _enabled_event_types{0};

    /// Shutdown primitives
    ss::gate _gate;
    ss::abort_source _as;

    /// Main data structure and associated timer that fires thread to consume
    /// from it. The data structure chosen is a boost::multi_index_container,
    /// configured to be searchable by a hash of the element or by the sequence
    /// of insertion.
    ///
    /// The chosen design provides two benefits:
    /// 1. Uses shard local memory as a temporary cache to reduce the number of
    /// calls to the producer shard.
    ///
    /// 2. The hashed_unique interface allows for quick detection of duplicates,
    /// greatly reducing the number of messages to produce onto the audit
    /// message topic. Helpful for when audit of produce_request is enabled,
    /// since the quantity of auditable messages will be high and many requests
    /// are identical and can be combined into one.
    ss::timer<> _drain_timer;
    underlying_t _queue;
    ssx::semaphore _active_drain{1, "audit-drain"};

    /// Single instance contains a kafka::client::client instance.
    friend class audit_sink;
    std::unique_ptr<audit_sink> _sink;

    /// Other references
    cluster::controller* _controller;
    kafka::client::configuration& _config;
};

} // namespace security::audit
