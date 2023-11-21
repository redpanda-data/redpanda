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
#include "cluster/fwd.h"
#include "config/property.h"
#include "kafka/client/fwd.h"
#include "kafka/client/types.h"
#include "kafka/protocol/types.h"
#include "model/namespace.h"
#include "model/timeout_clock.h"
#include "net/types.h"
#include "security/acl.h"
#include "security/audit/probe.h"
#include "security/audit/schemas/application_activity.h"
#include "security/audit/schemas/iam.h"
#include "security/audit/schemas/schemas.h"
#include "security/audit/schemas/utils.h"
#include "security/audit/types.h"
#include "security/request_auth.h"
#include "ssx/semaphore.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>
#include <seastar/util/bool_class.hh>

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

using is_started = ss::bool_class<struct is_started_tag>;

class audit_log_manager
  : public ss::peering_sharded_service<audit_log_manager> {
public:
    static constexpr auto client_shard_id = ss::shard_id{0};

    using audit_event_passthrough
      = ss::bool_class<struct audit_event_permitted_tag>;

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
    template<InheritsFromOCSFBase T>
    bool enqueue_audit_event(event_type type, T&& t) {
        if (auto val = should_enqueue_audit_event(type); val.has_value()) {
            return (bool)*val;
        }
        return do_enqueue_audit_event(std::make_unique<T>(std::forward<T>(t)));
    }

    template<
      typename T,
      security::audit::returns_auditable_resource_vector Func,
      typename... Args>
    bool enqueue_authz_audit_event(
      kafka::api_key api,
      const T& resource_name,
      Func func,
      const char* operation_name,
      security::auth_result result,
      Args&&... args) {
        if constexpr (std::is_same_v<T, model::topic>) {
            if (auto val = should_enqueue_audit_event(
                  api, result.principal, resource_name);
                val.has_value()
                && resource_name != model::kafka_audit_logging_topic) {
                return (bool)*val;
            }
        } else {
            if (auto val = should_enqueue_audit_event(api, result.principal);
                val.has_value()) {
                return (bool)*val;
            }
        }

        return do_enqueue_audit_event(
          std::make_unique<api_activity>(make_api_activity_event(
            operation_name,
            std::move(result),
            std::forward<Args>(args)...,
            create_resource_details(restrict_topics(std::move(func))))));
    }

    template<typename T, typename... Args>
    bool enqueue_authz_audit_event(
      kafka::api_key api,
      const T& resource_name,
      const char* operation_name,
      security::auth_result result,
      Args&&... args) {
        if constexpr (std::is_same_v<T, model::topic>) {
            if (auto val = should_enqueue_audit_event(
                  api, result.principal, resource_name);
                val.has_value()
                && resource_name != model::kafka_audit_logging_topic) {
                return (bool)*val;
            }
        } else {
            if (auto val = should_enqueue_audit_event(api, result.principal);
                val.has_value()) {
                return (bool)*val;
            }
        }
        return do_enqueue_audit_event(
          std::make_unique<api_activity>(make_api_activity_event(
            operation_name,
            std::move(result),
            std::forward<Args>(args)...,
            {})));
    }

    bool enqueue_authn_event(authentication_event_options options) {
        if (auto val = should_enqueue_audit_event(
              event_type::authenticate, options.user);
            val.has_value()) {
            return (bool)*val;
        }
        return do_enqueue_audit_event(std::make_unique<authentication>(
          make_authentication_event(std::move(options))));
    }

    template<typename... Args>
    bool enqueue_app_lifecycle_event(Args&&... args) {
        if (auto val = should_enqueue_audit_event(); val.has_value()) {
            return (bool)*val;
        }

        return do_enqueue_audit_event(std::make_unique<application_lifecycle>(
          make_application_lifecycle(std::forward<Args>(args)...)));
    }

    bool enqueue_api_activity_event(
      event_type type,
      ss::httpd::const_req req,
      const request_auth_result& auth_result,
      const ss::sstring& svc_name,
      bool authorized,
      const std::optional<std::string_view>& reason) {
        if (auto val = should_enqueue_audit_event(
              type, auth_result.get_username());
            val.has_value()) {
            return (bool)*val;
        }

        return do_enqueue_audit_event(
          std::make_unique<api_activity>(make_api_activity_event(
            req, auth_result, svc_name, authorized, reason)));
    }

    bool enqueue_api_activity_event(
      event_type type,
      ss::httpd::const_req req,
      const ss::sstring& user,
      const ss::sstring& svc_name) {
        if (auto val = should_enqueue_audit_event(type, user);
            val.has_value()) {
            return (bool)*val;
        }

        return do_enqueue_audit_event(std::make_unique<api_activity>(
          make_api_activity_event(req, user, svc_name)));
    }

    /// Enqueue an event to be produced onto an audit log partition.  This will
    /// always enqueue the event (if auditing is enabled).  This is used for
    /// items like authentication events or application events.
    ///
    /// Returns: bool representing if the audit msg was successfully moved into
    /// the queue or not. If unsuccessful this means the audit subsystem cannot
    /// publish messages. Consumers of this API should react accordingly, i.e.
    /// return an error to the client.
    template<InheritsFromOCSFBase T>
    bool enqueue_mandatory_audit_event(T&& t) {
        if (auto val = should_enqueue_audit_event(); val.has_value()) {
            return (bool)*val;
        }
        return do_enqueue_audit_event(std::make_unique<T>(std::forward<T>(t)));
    }

    /// Returns the number of items pending to be written to auditing log
    ///
    /// Note does not include records already sent to client
    size_t pending_events() const { return _queue.size(); };

    /// Returns true if the internal kafka client is allocated
    ///
    /// NOTE: Only works on shard_id{0}, use in unit tests
    bool is_client_enabled() const;

    bool report_redpanda_app_event(is_started);

private:
    /// The following methods return nullopt in the case the event should
    /// be audited, otherwise the optional is filled with the value representing
    /// whether it could not be enqueued due to error or due to the event
    /// not having attributes of desired trackable events
    std::optional<audit_event_passthrough> should_enqueue_audit_event() const;
    std::optional<audit_event_passthrough>
      should_enqueue_audit_event(event_type) const;
    std::optional<audit_event_passthrough> should_enqueue_audit_event(
      event_type, const security::acl_principal&) const;
    std::optional<audit_event_passthrough> should_enqueue_audit_event(
      kafka::api_key, const security::acl_principal&) const;
    std::optional<audit_event_passthrough>
    should_enqueue_audit_event(event_type, const security::audit::user&) const;
    std::optional<audit_event_passthrough>
    should_enqueue_audit_event(event_type, const ss::sstring&) const;
    std::optional<audit_event_passthrough> should_enqueue_audit_event(
      kafka::api_key,
      const security::acl_principal&,
      const model::topic&) const;

    ss::future<> drain();
    ss::future<> pause();
    ss::future<> resume();

    bool is_audit_event_enabled(event_type) const;
    bool do_enqueue_audit_event(
      std::unique_ptr<security::audit::ocsf_base_impl> msg);
    void set_enabled_events();

    audit_probe& probe() { return *_probe; }

    template<security::audit::returns_auditable_resource_vector Func>
    auto restrict_topics(Func&& func) const noexcept {
        auto result = func();
        if constexpr (std::is_same_v<
                        std::vector<model::topic>,
                        decltype(result)>) {
            std::erase_if(result, [this](const model::topic& t) {
                return _audit_excluded_topics.contains(t);
            });
        }

        return result;
    }

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
    config::binding<std::vector<ss::sstring>> _audit_excluded_topics_binding;
    absl::flat_hash_set<model::topic> _audit_excluded_topics;
    config::binding<std::vector<ss::sstring>>
      _audit_excluded_principals_binding;
    absl::flat_hash_set<security::acl_principal> _audit_excluded_principals;

    /// This will be true when the client detects that there is an issue with
    /// authorization configuration. Auth must be enabled so the client
    /// principal can be queried. This is needed so that redpanda can give
    /// special permission to the audit client to do things like produce to the
    /// audit topic.
    bool _auth_misconfigured{false};

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
    std::unique_ptr<audit_probe> _probe;
};

} // namespace security::audit
