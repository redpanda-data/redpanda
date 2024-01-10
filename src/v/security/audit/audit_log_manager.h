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
#include "base/vlog.h"
#include "cluster/fwd.h"
#include "config/property.h"
#include "kafka/client/fwd.h"
#include "kafka/client/types.h"
#include "kafka/protocol/types.h"
#include "model/namespace.h"
#include "model/timeout_clock.h"
#include "net/types.h"
#include "security/acl.h"
#include "security/audit/logger.h"
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
                val.has_value()) {
                return (bool)*val;
            }
        } else {
            if (auto val = should_enqueue_audit_event(api, result.principal);
                val.has_value()) {
                return (bool)*val;
            }
        }

        return do_enqueue_audit_event<api_activity>(
          operation_name,
          std::move(result),
          std::forward<Args>(args)...,
          restrict_topics(std::move(func)));
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
                val.has_value()) {
                return (bool)*val;
            }
        } else {
            if (auto val = should_enqueue_audit_event(api, result.principal);
                val.has_value()) {
                return (bool)*val;
            }
        }
        return do_enqueue_audit_event<api_activity>(
          operation_name,
          std::move(result),
          std::forward<Args>(args)...,
          std::vector<model::topic>());
    }

    bool enqueue_authn_event(authentication_event_options options) {
        if (auto val = should_enqueue_audit_event(
              event_type::authenticate, options.user);
            val.has_value()) {
            return (bool)*val;
        }
        return do_enqueue_audit_event<authentication>(std::move(options));
    }

    template<typename... Args>
    bool enqueue_app_lifecycle_event(Args&&... args) {
        if (auto val = should_enqueue_audit_event(); val.has_value()) {
            return (bool)*val;
        }
        return do_enqueue_audit_event<application_lifecycle>(
          std::forward<Args>(args)...);
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
        return do_enqueue_audit_event<api_activity>(
          req, auth_result, svc_name, authorized, reason);
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
        return do_enqueue_audit_event<api_activity>(req, user, svc_name);
    }

    /// Returns the number of items pending to be written to auditing log
    ///
    /// Note does not include records already sent to client
    size_t pending_events() const { return _queue.size(); };

    /// Returns the number of bytes left until the semaphore is exhausted
    ///
    size_t avaiable_reservation() const {
        return _queue_bytes_sem.available_units();
    }

    /// Returns true if the internal fibers are up
    bool is_effectively_enabled() const { return _effectively_enabled; }

    bool report_redpanda_app_event(is_started);

private:
    using ignore_enabled_events
      = ss::bool_class<struct ignore_enabled_events_tag>;
    /// The following methods return nullopt in the case the event should
    /// be audited, otherwise the optional is filled with the value representing
    /// whether it could not be enqueued due to error or due to the event
    /// not having attributes of desired trackable events
    std::optional<audit_event_passthrough> should_enqueue_audit_event() const;
    std::optional<audit_event_passthrough> should_enqueue_audit_event(
      event_type,
      ignore_enabled_events ignore_events = ignore_enabled_events::no) const;
    std::optional<audit_event_passthrough> should_enqueue_audit_event(
      event_type,
      const security::acl_principal&,
      ignore_enabled_events ignore_events = ignore_enabled_events::no) const;
    std::optional<audit_event_passthrough> should_enqueue_audit_event(
      kafka::api_key,
      const security::acl_principal&,
      ignore_enabled_events ignore_events = ignore_enabled_events::no) const;
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
    void set_enabled_events();

    template<InheritsFromOCSFBase T, typename... Args>
    bool do_enqueue_audit_event(Args&&... args) {
        auto& map = _queue.get<underlying_unordered_map>();
        const auto hash_key = T::hash(args...);
        auto it = map.find(hash_key);
        if (it == map.end()) {
            auto msg = std::make_unique<T>(
              T::construct(std::forward<Args>(args)...));
            const auto msg_size = msg->estimated_size();
            auto units = ss::try_get_units(_queue_bytes_sem, msg_size);
            if (!units) {
                vlog(
                  adtlog.warn,
                  "Unable to enqueue audit event {}, msg size: {}, avail "
                  "units: {}",
                  *msg,
                  msg_size,
                  _queue_bytes_sem.available_units());
                probe().audit_error();
                return false;
            }
            auto& list = _queue.get<underlying_list>();
            vlog(
              adtlog.trace,
              "Successfully enqueued audit event {}, semaphore contains {} "
              "units",
              *msg,
              _queue_bytes_sem.available_units());
            list.push_back(
              audit_msg(hash_key, std::move(msg), std::move(*units)));
        } else {
            vlog(
              adtlog.trace, "Incrementing count of event {}", it->ocsf_msg());
            auto now = security::audit::timestamp_t{
              std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count()};
            it->increment(now);
        }
        return true;
    }

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

    static bool recovery_mode_enabled() noexcept;

private:
    class audit_msg {
    public:
        /// Wrapper around an ocsf event pointer
        ///
        /// Main benefit is to tie the lifetime of semaphore units with the
        /// underlying ocsf event itself
        audit_msg(
          size_t hash_key,
          std::unique_ptr<ocsf_base_impl> msg,
          ssx::semaphore_units&& units)
          : _hash_key(hash_key)
          , _msg(std::move(msg))
          , _units(std::move(units)) {
            vassert(_msg != nullptr, "Audit record cannot be null");
        }

        size_t key() const { return _hash_key; }

        void increment(timestamp_t t) const { _msg->increment(t); }

        const std::unique_ptr<ocsf_base_impl>& ocsf_msg() const { return _msg; }

        std::unique_ptr<ocsf_base_impl> release() && {
            _units.return_all();
            return std::move(_msg);
        }

    private:
        size_t _hash_key;
        std::unique_ptr<ocsf_base_impl> _msg;
        ssx::semaphore_units _units;
    };

    /// Multi index container is efficent in terms of time and space, underlying
    /// internal data structures are compact requiring only one node per
    /// element. More info here:
    /// https://www.boost.org/doc/libs/1_72_0/libs/multi_index/doc/performance.html
    struct underlying_list {};
    struct underlying_unordered_map {};
    using underlying_t = boost::multi_index::multi_index_container<
      audit_msg,
      boost::multi_index::indexed_by<
        /// Sequenced list of entries
        boost::multi_index::sequenced<boost::multi_index::tag<underlying_list>>,
        /// Set of audit_messages using hashed representations as comparator
        boost::multi_index::hashed_unique<
          boost::multi_index::tag<underlying_unordered_map>,
          boost::multi_index::
            const_mem_fun<audit_msg, size_t, &audit_msg::key>>>>;

    /// configuration options
    config::binding<bool> _audit_enabled;
    config::binding<std::chrono::milliseconds> _queue_drain_interval_ms;
    config::binding<std::vector<ss::sstring>> _audit_event_types;
    size_t _max_queue_size_bytes;
    static constexpr auto enabled_set_bitlength
      = std::underlying_type_t<event_type>(event_type::num_elements);
    std::bitset<enabled_set_bitlength> _enabled_event_types{0};
    config::binding<std::vector<ss::sstring>> _audit_excluded_topics_binding;
    absl::flat_hash_set<model::topic> _audit_excluded_topics;
    config::binding<std::vector<ss::sstring>>
      _audit_excluded_principals_binding;
    absl::flat_hash_set<security::acl_principal> _audit_excluded_principals;

    ssx::semaphore _queue_bytes_sem;

    /// This will be true when the client detects that there is an issue with
    /// authorization configuration. Auth must be enabled so the client
    /// principal can be queried. This is needed so that redpanda can give
    /// special permission to the audit client to do things like produce to the
    /// audit topic.
    bool _auth_misconfigured{false};

    /// Represents whether the feature is actually active, not the
    /// representation of the config variable
    bool _effectively_enabled{false};

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
