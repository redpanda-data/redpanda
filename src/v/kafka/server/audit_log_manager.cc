// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/audit_log_manager.h"

#include "cluster/controller.h"
#include "cluster/ephemeral_credential_frontend.h"
#include "kafka/client/client.h"
#include "kafka/client/config_utils.h"
#include "kafka/server/handlers/topics/types.h"
#include "security/acl.h"
#include "security/ephemeral_credential_store.h"
#include "storage/parser_utils.h"
#include "utils/retry.h"

#include <seastar/core/sleep.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include <memory>

namespace kafka {

/// TODO: Create a new ephemeral user for the audit principal so even clients
/// instantiated by pandaproxy cannot modify or produce to the audit topic
static const security::acl_principal audit_principal{
  security::principal_type::ephemeral_user, "__auditing"};

/// Contains a kafka client and a sempahore to bound the memory allocated
/// by it. This class may be allocated/deallocated on the owning shard depending
/// on the value of the global audit toggle config option (audit_enabled)
class audit_client {
public:
    static const auto shard_id = ss::shard_id{0};

    audit_client(cluster::controller*, client::configuration&);

    /// Initializes the client (with all necessary auth) and connects to the
    /// remote broker. If successful requests to create audit topic and all
    /// necessary ACLs will be made. produce() cannot be called until
    /// initialization completes with success.
    ss::future<> initialize();

    /// Closes the gate and flushes all buffers before severing connection to
    /// broker(s)
    ss::future<> shutdown();

    /// Produces to the audit topic, internal partitioner assigns partitions
    /// to the batches provided. Blocks if semaphore is exhausted.
    ss::future<> produce(std::vector<client::record_essence>);

    bool is_initialized() const { return _is_initialized; }

private:
    ss::future<> configure();
    ss::future<> mitigate_error(std::exception_ptr);
    ss::future<> create_internal_topic();
    ss::future<> set_auditing_permissions();
    ss::future<> inform(model::node_id id);
    ss::future<> do_inform(model::node_id id);

private:
    bool _has_ephemeral_credentials{false};
    ss::abort_source _as;
    ss::gate _gate;
    bool _is_initialized{false};
    ssx::semaphore _send_sem;
    client::client _client;
    cluster::controller* _controller;
};

/// Allocated only on the shard responsible for owning the kafka client, its
/// lifetime is the duration of the audit_log_manager. Contains a gate/mutex to
/// synchronize around actions around the internal client which may be
/// started/stopped on demand.
class audit_sink {
public:
    audit_sink(
      audit_log_manager* audit_mgr,
      cluster::controller* controller,
      client::configuration& config) noexcept;

    /// Starts a kafka::client if none is allocated, backgrounds the work
    ss::future<> start();

    /// Closes all gates, deallocates client returns when all has completed
    ss::future<> stop();

    /// Produce to the audit topic within the context of the internal locks,
    /// ensuring toggling of the audit master switch happens in lock step with
    /// calls to produce()
    ss::future<> produce(std::vector<client::record_essence> records);

    /// Allocates and connects, or deallocates and shuts down the audit client
    void toggle(bool enabled);

    /// Returns true if _client has a value
    bool is_enabled() const { return _client != nullptr; }

private:
    ss::future<> do_toggle(bool enabled);

    /// Primitives for ensuring background work and toggling of switch w/ async
    /// work occur in lock step
    ss::gate _gate;
    mutex _mutex;

    /// Reference to audit manager so synchronization with its fibers may occur.
    /// Supports pausing and resuming these fibers so the client can safely be
    /// deallocated when no more work is concurrently entering the system
    audit_log_manager* _audit_mgr;

    /// audit_client and members necessary to pass to its constructor
    std::unique_ptr<audit_client> _client;
    cluster::controller* _controller;
    client::configuration& _config;
};

audit_client::audit_client(
  cluster::controller* controller, client::configuration& client_config)
  : _send_sem(
    config::shard_local_cfg().audit_client_max_buffer_size(),
    "audit_log_producer_semaphore")
  , _client(
      config::to_yaml(client_config, config::redact_secrets::no),
      [this](std::exception_ptr eptr) { return mitigate_error(eptr); })
  , _controller(controller) {}

ss::future<> audit_client::initialize() {
    static const auto base_backoff = 250ms;
    exp_backoff_policy backoff_policy;
    while (!_as.abort_requested()) {
        try {
            co_await configure();
            _is_initialized = true;
            break;
        } catch (...) {
            /// Sleep, then try again
        }
        auto next = backoff_policy.next_backoff();
        co_await ss::sleep_abortable(base_backoff * next, _as)
          .handle_exception_type([](const ss::sleep_aborted&) {});
    }
}

ss::future<> audit_client::configure() {
    try {
        auto config = co_await client::create_client_credentials(
          *_controller,
          config::shard_local_cfg(),
          _client.config(),
          audit_principal);
        set_client_credentials(*config, _client);

        auto const& store
          = _controller->get_ephemeral_credential_store().local();
        _has_ephemeral_credentials = store.has(store.find(audit_principal));

        co_await set_auditing_permissions();
        co_await create_internal_topic();

        /// Retries should be "infinite", to avoid dropping data, there is a
        /// known issue within the client setting this value to size_t::max
        _client.config().retries.set_value(10000);
        vlog(klog.info, "Audit log client initialized");
    } catch (...) {
        vlog(
          klog.warn,
          "Audit log client failed to initialize: {}",
          std::current_exception());
        throw;
    }
}

ss::future<> audit_client::set_auditing_permissions() {
    /// Give permissions to create and write to the audit topic
    security::acl_entry acl_create_entry{
      audit_principal,
      security::acl_host::wildcard_host(),
      security::acl_operation::create,
      security::acl_permission::allow};

    security::acl_entry acl_write_entry{
      audit_principal,
      security::acl_host::wildcard_host(),
      security::acl_operation::write,
      security::acl_permission::allow};

    security::resource_pattern audit_topic_pattern{
      security::resource_type::topic,
      model::kafka_audit_logging_topic,
      security::pattern_type::literal};

    /// TODO: Add rules for User:* deny to things like deleting the topic
    /// and other unwanted behavior. User:* should be allowed to ::describe
    /// and alter topic configs
    co_await _controller->get_security_frontend().local().create_acls(
      {security::acl_binding{audit_topic_pattern, acl_create_entry},
       security::acl_binding{audit_topic_pattern, acl_write_entry}},
      5s);
}

ss::future<> audit_client::mitigate_error(std::exception_ptr eptr) {
    if (_gate.is_closed() || _as.abort_requested()) {
        /// TODO: Investigate looping behavior on shutdown
        co_return;
    }
    vlog(klog.trace, "mitigate_error: {}", eptr);
    auto f = ss::now();
    try {
        std::rethrow_exception(eptr);
    } catch (client::broker_error const& ex) {
        if (
          ex.error == error_code::sasl_authentication_failed
          && _has_ephemeral_credentials) {
            f = inform(ex.node_id).then([this]() { return _client.connect(); });
        } else {
            throw;
        }
    } catch (...) {
        throw;
    }
    co_await std::move(f);
}

ss::future<> audit_client::inform(model::node_id id) {
    vlog(klog.trace, "inform: {}", id);

    // Inform a particular node
    if (id != client::unknown_node_id) {
        return do_inform(id);
    }

    // Inform all nodes
    return seastar::parallel_for_each(
      _controller->get_members_table().local().node_ids(),
      [this](model::node_id id) { return do_inform(id); });
}

ss::future<> audit_client::do_inform(model::node_id id) {
    auto& fe = _controller->get_ephemeral_credential_frontend().local();
    auto ec = co_await fe.inform(id, audit_principal);
    vlog(klog.info, "Informed: broker: {}, ec: {}", id, ec);
}

ss::future<> audit_client::create_internal_topic() {
    constexpr std::string_view retain_forever = "-1";
    constexpr std::string_view seven_days = "604800000";
    creatable_topic audit_topic{
      .name = model::kafka_audit_logging_topic,
      .num_partitions = config::shard_local_cfg().audit_log_num_partitions(),
      .replication_factor
      = config::shard_local_cfg().audit_log_replication_factor(),
      .assignments = {},
      .configs = {
        createable_topic_config{
          .name = ss::sstring(topic_property_retention_bytes),
          .value{retain_forever}},
        createable_topic_config{
          .name = ss::sstring(topic_property_retention_duration),
          .value{seven_days}}}};
    vlog(klog.info, "Creating audit log topic with settings: {}", audit_topic);
    const auto resp = co_await _client.create_topic({std::move(audit_topic)});
    if (resp.data.topics.size() != 1) {
        throw std::runtime_error(
          fmt::format("Unexpected create topics response: {}", resp.data));
    }
    const auto& topic = resp.data.topics[0];
    if (topic.error_code == error_code::none) {
        vlog(klog.debug, "Auditing: created audit log topic: {}", topic);
    } else if (topic.error_code == error_code::topic_already_exists) {
        vlog(klog.debug, "Auditing: topic already exists");
        co_await _client.update_metadata();
    } else {
        if (topic.error_code == error_code::invalid_replication_factor) {
            vlog(
              klog.warn,
              "Auditing: invalid replication factor on audit topic, "
              "check/modify settings, then disable and re-enable "
              "'audit_enabled'");
        }
        const auto msg = topic.error_message.has_value() ? *topic.error_message
                                                         : "<no_err_msg>";
        throw std::runtime_error(
          fmt::format("{} - error_code: {}", msg, topic.error_code));
    }
}

ss::future<> audit_client::shutdown() {
    /// Repeated calls to shutdown() are possible, although unlikely
    if (_as.abort_requested()) {
        co_return;
    }
    vlog(klog.info, "Shutting down audit client");
    _as.request_abort();
    _send_sem.broken();
    co_await _client.stop();
    co_await _gate.close();
}

ss::future<>
audit_client::produce(std::vector<client::record_essence> records) {
    /// TODO: Produce with acks=1, atm -1 is hardcoded into client
    const auto records_size = [](const auto& records) {
        std::size_t size = 0;
        for (const auto& r : records) {
            if (r.value) {
                /// auditing does not fill in any of the fields of the
                /// record_essence other then the value member
                size += r.value->size_bytes();
            }
        }
        return size;
    };

    try {
        const auto size_bytes = records_size(records);
        auto units = co_await ss::get_units(_send_sem, size_bytes);
        ssx::spawn_with_gate(
          _gate,
          [this,
           units = std::move(units),
           records = std::move(records)]() mutable {
              return _client
                .produce_records(
                  model::kafka_audit_logging_topic, std::move(records))
                .discard_result()
                .handle_exception_type([](const client::partition_error& ex) {
                    /// TODO: Possible optimization to retry with different
                    /// partition strategy.
                    ///
                    /// If reached here non-mitigatable error occured, or
                    /// attempts on mitigation had been used up.
                    vlog(klog.error, "Audit records dropped, reason: {}", ex);
                })
                .finally([units = std::move(units)] {});
          });
    } catch (const ss::broken_semaphore&) {
        vlog(
          klog.debug,
          "Shutting down the auditor kafka::client, semaphore broken");
    }
    co_return;
}

/// audit_sink

audit_sink::audit_sink(
  audit_log_manager* audit_mgr,
  cluster::controller* controller,
  client::configuration& config) noexcept
  : _audit_mgr(audit_mgr)
  , _controller(controller)
  , _config(config) {}

ss::future<> audit_sink::start() {
    toggle(true);
    return ss::now();
}

ss::future<> audit_sink::stop() {
    _mutex.broken();
    if (_client) {
        co_await _client->shutdown();
    }
    co_await _gate.close();
}

ss::future<> audit_sink::produce(std::vector<client::record_essence> records) {
    /// No locks/gates since the calls to this method are done in controlled
    /// context of other synchronization primitives
    vassert(_client, "produce() called on a null client");
    co_await _client->produce(std::move(records));
}

void audit_sink::toggle(bool enabled) {
    ssx::spawn_with_gate(
      _gate, [this, enabled]() { return do_toggle(enabled); });
}

ss::future<> audit_sink::do_toggle(bool enabled) {
    try {
        ssx::semaphore_units lock;
        if (enabled && !_client) {
            lock = co_await _mutex.get_units();
            _client = std::make_unique<audit_client>(_controller, _config);
            co_await _client->initialize();
            if (_client->is_initialized()) {
                /// Only if shutdown succeeded before initializtion could
                /// complete would this case evaluate to false
                co_await _audit_mgr->resume();
            }
        } else if (!enabled && _client) {
            /// Call to shutdown does not exist within the lock so that
            /// shutting down isn't blocked on the lock held above in the
            /// case initialize() doesn't complete. This is common if for
            /// example the audit topic is improperly configured
            /// intitialization will forever hang.
            co_await _client->shutdown();
            lock = co_await _mutex.get_units();
            co_await _audit_mgr->pause();
            _client.reset(nullptr);
        }
    } catch (const ss::broken_semaphore&) {
        vlog(klog.info, "Failed to toggle audit status, shutting down");
    } catch (...) {
        vlog(
          klog.error,
          "Failed to toggle audit status: {}",
          std::current_exception());
    }
}

/// audit_log_manager

void audit_log_manager::set_enabled_events() {
    using underlying_enum_t = std::underlying_type_t<audit_event_type>;
    _enabled_event_types = underlying_enum_t(0);
    for (const auto& e : _audit_event_types()) {
        const auto as_uint = underlying_enum_t(string_to_audit_event_type(e));
        _enabled_event_types[as_uint] = true;
    }
    vassert(
      !is_audit_event_enabled(audit_event_type::unknown),
      "Unknown audit_event_type observed");
}

audit_log_manager::audit_log_manager(
  cluster::controller* controller, client::configuration& client_config)
  : _audit_enabled(config::shard_local_cfg().audit_enabled.bind())
  , _queue_drain_interval_ms(
      config::shard_local_cfg().audit_queue_drain_interval_ms.bind())
  , _max_queue_elements_per_shard(
      config::shard_local_cfg().audit_max_queue_elements_per_shard.bind())
  , _audit_event_types(
      config::shard_local_cfg().audit_enabled_event_types.bind())
  , _controller(controller)
  , _config(client_config) {
    if (ss::this_shard_id() == audit_client::shard_id) {
        _sink = std::make_unique<audit_sink>(this, controller, client_config);
    }

    _drain_timer.set_callback([this] {
        ssx::spawn_with_gate(_gate, [this]() {
            return ss::get_units(_active_drain, 1)
              .then([this](auto units) mutable {
                  return drain()
                    .handle_exception([](std::exception_ptr e) {
                        vlog(
                          klog.warn,
                          "Exception in audit_log_manager fiber: {}",
                          e);
                    })
                    .finally([this, units = std::move(units)] {
                        _drain_timer.arm(_queue_drain_interval_ms());
                    });
              });
        });
    });
    set_enabled_events();
    _audit_event_types.watch([this] { set_enabled_events(); });
}

audit_log_manager::~audit_log_manager() = default;

bool audit_log_manager::is_audit_event_enabled(
  audit_event_type event_type) const {
    using underlying_enum_t = std::underlying_type_t<audit_event_type>;
    return _enabled_event_types.test(underlying_enum_t(event_type));
}

ss::future<> audit_log_manager::start() {
    if (ss::this_shard_id() != audit_client::shard_id) {
        co_return;
    }
    _audit_enabled.watch([this] {
        try {
            _sink->toggle(_audit_enabled());
        } catch (const ss::gate_closed_exception&) {
            vlog(klog.debug, "Failed to toggle auditing state, shutting down");
        } catch (...) {
            vlog(
              klog.error,
              "Failed to toggle auditing state: {}",
              std::current_exception());
        }
    });
    if (_audit_enabled()) {
        vlog(klog.info, "Starting audit_log_manager");
        co_await _sink->start();
    }
}

ss::future<> audit_log_manager::stop() {
    _drain_timer.cancel();
    _as.request_abort();
    if (ss::this_shard_id() == audit_client::shard_id) {
        vlog(klog.info, "Shutting down audit log manager");
        co_await _sink->stop();
    }
    if (!_gate.is_closed()) {
        /// Gate may already be closed if ::pause() had been called
        co_await _gate.close();
    }
    if (_queue.size() > 0) {
        vlog(
          klog.debug,
          "{} records were not pushed to the audit log before shutdown",
          _queue.size());
    }
}

ss::future<> audit_log_manager::pause() {
    return container().invoke_on_all([](audit_log_manager& mgr) {
        /// Wait until drain() has completed, with timer cancelled it can be
        /// ensured no more work will be performed
        return ss::get_units(mgr._active_drain, 1).then([&mgr](auto) {
            mgr._drain_timer.cancel();
        });
    });
}

ss::future<> audit_log_manager::resume() {
    return container().invoke_on_all([](audit_log_manager& mgr) {
        /// If the timer is already armed that is a bug
        vassert(
          !mgr._drain_timer.armed(),
          "Timer is already armed upon call to ::resume");
        mgr._drain_timer.arm(mgr._queue_drain_interval_ms());
    });
}

bool audit_log_manager::is_client_enabled() const {
    vassert(
      ss::this_shard_id() == audit_client::shard_id,
      "Must be called on audit client shard");
    return _sink->is_enabled();
}

bool audit_log_manager::do_enqueue_audit_event(
  std::unique_ptr<security::audit::ocsf_base_impl> msg) {
    auto& map = _queue.get<underlying_unordered_map>();
    auto it = map.find(msg->key());
    if (it == map.end()) {
        if (_queue.size() >= _max_queue_elements_per_shard()) {
            return false;
        }
        auto& list = _queue.get<underlying_list>();
        list.push_back(std::move(msg));
    } else {
        auto now = security::audit::timestamp_t{
          std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count()};
        (*it)->increment(now);
    }
    return true;
}

ss::future<> audit_log_manager::drain() {
    if (_queue.empty() || _as.abort_requested()) {
        co_return;
    }

    /// Combine all batched audit msgs into record_essences
    std::vector<client::record_essence> essences;
    auto records = std::exchange(_queue, underlying_t{});
    auto& records_seq = records.get<underlying_list>();
    while (!records_seq.empty()) {
        const auto& front = records_seq.front();
        auto as_json = front->to_json();
        records_seq.pop_front();
        iobuf b;
        b.append(as_json.c_str(), as_json.size());
        essences.push_back(client::record_essence{.value = std::move(b)});
        co_await ss::maybe_yield();
    }

    /// This call may block if the audit_clients semaphore is exhausted,
    /// this represents the amount of memory used within its kafka::client
    /// produce batch queue. If the semaphore blocks it will apply
    /// backpressure here, and the \ref _queue will begin to fill closer to
    /// capacity. When it hits capacity, enqueue_audit_event() will block.
    co_await container().invoke_on(
      audit_client::shard_id,
      [recs = std::move(essences)](audit_log_manager& mgr) mutable {
          return mgr._sink->produce(std::move(recs));
      });
}

} // namespace kafka
