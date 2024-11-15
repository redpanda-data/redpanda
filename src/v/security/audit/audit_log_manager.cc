/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "security/audit/audit_log_manager.h"

#include "base/outcome.h"
#include "cluster/controller.h"
#include "cluster/ephemeral_credential_frontend.h"
#include "cluster/metadata_cache.h"
#include "cluster/security_frontend.h"
#include "config/configuration.h"
#include "kafka/client/client.h"
#include "kafka/client/config_utils.h"
#include "kafka/data/record_batcher.h"
#include "kafka/protocol/produce.h"
#include "kafka/protocol/schemata/produce_response.h"
#include "kafka/protocol/topic_properties.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "security/acl.h"
#include "security/audit/client_probe.h"
#include "security/audit/logger.h"
#include "security/audit/schemas/application_activity.h"
#include "security/audit/schemas/types.h"
#include "security/audit/schemas/utils.h"
#include "security/ephemeral_credential_store.h"
#include "storage/parser_utils.h"
#include "utils/retry.h"

#include <seastar/core/loop.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/smp.hh>
#include <seastar/coroutine/maybe_yield.hh>

#include <absl/algorithm/container.h>

#include <memory>
#include <optional>

namespace security::audit {

namespace {
struct partition_batch {
    model::partition_id pid;
    model::record_batch batch;
    std::optional<ssx::semaphore_units> send_units{};
};
} // namespace

static constexpr std::string_view subsystem_name = "Audit System";

std::ostream& operator<<(std::ostream& os, event_type t) {
    switch (t) {
    case event_type::management:
        return os << "management";
    case event_type::produce:
        return os << "produce";
    case event_type::consume:
        return os << "consume";
    case event_type::describe:
        return os << "describe";
    case event_type::heartbeat:
        return os << "heartbeat";
    case event_type::authenticate:
        return os << "authenticate";
    case event_type::admin:
        return os << "admin";
    case event_type::schema_registry:
        return os << "schema_registry";
    case event_type::unknown:
        return os << "unknown";
    default:
        return os << "invalid";
    }
}

class audit_sink;

/// Contains a kafka client and a sempahore to bound the memory allocated
/// by it. This class may be allocated/deallocated on the owning shard depending
/// on the value of the global audit toggle config option (audit_enabled)
class audit_client {
public:
    audit_client(
      audit_sink* sink, cluster::controller*, kafka::client::configuration&);

    /// Initializes the client (with all necessary auth) and connects to the
    /// remote broker. If successful requests to create audit topic and all
    /// necessary ACLs will be made. produce() cannot be called until
    /// initialization completes with success.
    ss::future<> initialize();

    /// Shuts down the client, may wait for up to
    /// kafka::config::produce_shutdown_delay_ms to complete
    ss::future<> shutdown();

    /// Produces to the audit topic partition specified with each batch.
    /// Blocks if semaphore is exhausted.
    ss::future<> produce(chunked_vector<partition_batch>, audit_probe&);

    /// Returns true if the configuration phase has completed which includes:
    /// - Connecting to the broker(s) w/ ephemeral creds
    /// - Creating ACLs
    /// - Creating internal audit topic
    bool is_initialized() const { return _is_initialized; }

private:
    ss::future<>
    do_produce(model::record_batch, model::partition_id, audit_probe&);
    ss::future<> update_status(kafka::error_code);
    ss::future<> update_status(kafka::produce_response);
    ss::future<> configure();
    ss::future<> mitigate_error(std::exception_ptr);
    ss::future<> create_internal_topic();
    ss::future<> set_auditing_permissions();
    ss::future<> inform(model::node_id id);
    ss::future<> do_inform(model::node_id id);
    ss::future<> set_client_credentials();

private:
    kafka::error_code _last_errc{kafka::error_code::unknown_server_error};
    ss::abort_source _as;
    ss::gate _gate;
    bool _is_initialized{false};
    size_t _max_buffer_size;
    ssx::semaphore _send_sem;
    kafka::client::client _client;
    audit_sink* _sink;
    cluster::controller* _controller;
    std::unique_ptr<client_probe> _probe;
};

/// Allocated only on the shard responsible for owning the kafka client, its
/// lifetime is the duration of the audit_log_manager. Contains a gate/mutex to
/// synchronize around actions around the internal client which may be
/// started/stopped on demand.
class audit_sink {
public:
    using auth_misconfigured_t = ss::bool_class<struct auth_misconfigured_tag>;

    audit_sink(
      audit_log_manager* audit_mgr,
      cluster::controller* controller,
      kafka::client::configuration& config) noexcept;

    /// Starts a kafka::client if none is allocated, backgrounds the work
    ss::future<> start();

    /// Closes all gates, deallocates client returns when all has completed
    ss::future<> stop();

    /// Produce to the audit topic within the context of the internal locks,
    /// ensuring toggling of the audit master switch happens in lock step with
    /// calls to produce()
    ss::future<> produce(chunked_vector<partition_batch> records);

    /// Allocates and connects, or deallocates and shuts down the audit client
    void toggle(bool enabled);

private:
    ss::future<>
      publish_app_lifecycle_event(application_lifecycle::activity_id);

    ss::future<> update_auth_status(auth_misconfigured_t);

    ss::future<> do_toggle(bool enabled);

    /// Primitives for ensuring background work and toggling of switch w/ async
    /// work occur in lock step
    ss::gate _gate;
    mutex _mutex{"audit_log_manager::mutex"};

    /// In the case the client did not finish intialization this optional may be
    /// fufilled by a fiber attempting to shutdown the client. The future will
    /// then later be waited on by the fiber that was initializing the client.
    std::optional<ss::future<>> _early_exit_future;

    /// Reference to audit manager so synchronization with its fibers may occur.
    /// Supports pausing and resuming these fibers so the client can safely be
    /// deallocated when no more work is concurrently entering the system
    audit_log_manager* _audit_mgr;

    /// audit_client and members necessary to pass to its constructor
    std::unique_ptr<audit_client> _client;
    cluster::controller* _controller;
    kafka::client::configuration& _config;

    friend class audit_client;
};

audit_client::audit_client(
  audit_sink* sink,
  cluster::controller* controller,
  kafka::client::configuration& client_config)
  : _max_buffer_size(config::shard_local_cfg().audit_client_max_buffer_size())
  , _send_sem(_max_buffer_size, "audit_log_producer_semaphore")
  , _client(
      config::to_yaml(client_config, config::redact_secrets::no),
      [this](std::exception_ptr eptr) { return mitigate_error(eptr); })
  , _sink(sink)
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
    if (_is_initialized) {
        _probe = std::make_unique<client_probe>();
        _probe->setup_metrics([this]() {
            return 1.0
                   - (static_cast<double>(_send_sem.available_units()) / static_cast<double>(_max_buffer_size));
        });
    }
}

ss::future<> audit_client::set_client_credentials() {
    /// Set ephemeral credential
    auto& frontend = _controller->get_ephemeral_credential_frontend().local();
    auto pw = co_await frontend.get(audit_principal);
    if (pw.err != cluster::errc::success) {
        throw std::runtime_error(fmt::format(
          "Failed to fetch credential for principal: {}", audit_principal));
    }

    _client.config().sasl_mechanism.set_value(pw.credential.mechanism());
    _client.config().scram_username.set_value(pw.credential.user()());
    _client.config().scram_password.set_value(pw.credential.password()());
}

ss::future<> audit_client::configure() {
    try {
        const auto& feature_table = _controller->get_feature_table();
        if (!feature_table.local().is_active(
              features::feature::audit_logging)) {
            throw std::runtime_error(
              "Failing to create audit client until cluster has been fully "
              "upgraded to the min supported version for audit_logging");
        }
        co_await set_client_credentials();
        co_await set_auditing_permissions();
        co_await create_internal_topic();
        co_await _client.connect();

        /// To avoid dropping data, retries should be functionally infinite,
        /// but we handle this logic at the produce call site. Individual
        /// requests should fail after a few attempts, allowing the kafka
        /// client to refresh its metadata on a subsequent request.
        /// Explicitly set `client::config::retries` to its default value.
        /// We might want to make this tunable at some point.
        _client.config().retries.reset();
        vlog(adtlog.info, "Audit log client initialized");
    } catch (...) {
        vlog(
          adtlog.warn,
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

    co_await _controller->get_security_frontend().local().create_acls(
      {security::acl_binding{audit_topic_pattern, acl_create_entry},
       security::acl_binding{audit_topic_pattern, acl_write_entry}},
      5s);
}

/// `update_auth_status` should not be called frequently since this method
/// occurs on the hot path and calls to `update_auth_status` call will boil down
/// to an invoke_on_all() call. Conditionals are wrapped around the call to
/// `update_auth_status` so that its only called when the errc changes to/from
/// a desired condition.
ss::future<> audit_client::update_status(kafka::error_code errc) {
    /// If the status changed to erraneous from anything else
    if (errc == kafka::error_code::illegal_sasl_state) {
        if (_last_errc != kafka::error_code::illegal_sasl_state) {
            co_await _sink->update_auth_status(
              audit_sink::auth_misconfigured_t::yes);
        }
    } else if (_last_errc == kafka::error_code::illegal_sasl_state) {
        /// The status changed from erraneous to anything else
        if (
          errc != kafka::error_code::illegal_sasl_state
          && errc != kafka::error_code::broker_not_available) {
            co_await _sink->update_auth_status(
              audit_sink::auth_misconfigured_t::no);
        }
    }
    _last_errc = errc;
}

ss::future<> audit_client::update_status(kafka::produce_response response) {
    /// This method should almost always call update_status() with a value of
    /// no error code. That is because kafka client mitigation will be called in
    /// the case there is a produce error, and an erraneous response will only
    /// be returned when the retry count is exhausted, which will never occur
    /// since it is artificially set high to have the effect of always retrying
    absl::flat_hash_set<kafka::error_code> errcs;
    for (const auto& topic_response : response.data.responses) {
        for (const auto& partition_response : topic_response.partitions) {
            errcs.emplace(partition_response.error_code);
        }
    }
    if (errcs.empty()) {
        vlog(seclog.warn, "Empty produce response recieved");
        co_return;
    }
    auto errc = *errcs.begin();
    if (errcs.contains(kafka::error_code::illegal_sasl_state)) {
        errc = kafka::error_code::illegal_sasl_state;
    }
    co_await update_status(errc);
}

ss::future<> audit_client::mitigate_error(std::exception_ptr eptr) {
    vlog(adtlog.trace, "mitigate_error: {}", eptr);
    auto f = ss::now();
    try {
        std::rethrow_exception(eptr);
    } catch (const kafka::client::broker_error& ex) {
        f = update_status(ex.error);
        if (ex.error == kafka::error_code::sasl_authentication_failed) {
            f = f.then([this, ex]() {
                return inform(ex.node_id).then([this]() {
                    return _client.connect();
                });
            });
        } else {
            throw;
        }
    } catch (...) {
        throw;
    }
    co_await std::move(f);
}

ss::future<> audit_client::inform(model::node_id id) {
    vlog(adtlog.trace, "inform: {}", id);

    // Inform a particular node
    if (id != kafka::client::unknown_node_id) {
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
    vlog(adtlog.info, "Informed: broker: {}, ec: {}", id, ec);
}

ss::future<> audit_client::create_internal_topic() {
    constexpr std::string_view retain_forever = "-1";
    constexpr std::string_view seven_days = "604800000";
    int16_t replication_factor
      = config::shard_local_cfg().audit_log_replication_factor().value_or(
        _controller->internal_topic_replication());
    vlog(
      adtlog.debug,
      "Attempting to create internal topic (replication={})",
      replication_factor);
    kafka::creatable_topic audit_topic{
      .name = model::kafka_audit_logging_topic,
      .num_partitions = config::shard_local_cfg().audit_log_num_partitions(),
      .replication_factor = replication_factor,
      .assignments = {},
      .configs = {
        kafka::createable_topic_config{
          .name = ss::sstring(kafka::topic_property_retention_bytes),
          .value{retain_forever}},
        kafka::createable_topic_config{
          .name = ss::sstring(kafka::topic_property_retention_duration),
          .value{seven_days}},
        kafka::createable_topic_config{
          .name = ss::sstring(kafka::topic_property_cleanup_policy),
          .value = "delete"}}};
    vlog(
      adtlog.info, "Creating audit log topic with settings: {}", audit_topic);
    const auto resp = co_await _client.create_topic({std::move(audit_topic)});
    if (resp.data.topics.size() != 1) {
        throw std::runtime_error(
          fmt::format("Unexpected create topics response: {}", resp.data));
    }
    const auto& topic = resp.data.topics[0];
    if (topic.error_code == kafka::error_code::none) {
        vlog(adtlog.debug, "Auditing: created audit log topic: {}", topic);
    } else if (topic.error_code == kafka::error_code::topic_already_exists) {
        vlog(adtlog.debug, "Auditing: topic already exists");
        co_await _client.update_metadata();
    } else {
        if (topic.error_code == kafka::error_code::invalid_replication_factor) {
            vlog(
              adtlog.warn,
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
    /// On shutdown the best attempt to send all data residing in the queues
    /// must be made, therefore we must:
    /// - Send the data from all shards queues to the kafka/client
    /// - Send the shutdown signal to the client
    /// - client::stop() will make a best attempt to send records residing
    ///   within its buffers
    ///
    /// _client->stop() must only be called when the records reside within the
    /// client, otherwise the client upon call to stop() will have no records in
    /// its buffer to send. Therefore client->stop() should only be called once
    /// the records exist within the clients internal buffers, but how to
    /// exactly know that? One could synchronously wait until
    /// client->produce_records() has finished but this is not a good solution
    /// because:
    ///
    /// 1. It returns when the data has been acked (waiting longer then
    ///    necessary)
    /// 2. If there is an error in produce it will permanently loop since
    ///    the audit retry count is set high - deadlock can occur.
    ///
    /// Therefore the solution here is to on call to audit_client::shutdown,
    /// wait until all outstanding requests complete within a fixed timeout
    /// (since the semaphore units have been taken from the most recent call to
    /// drain() via pause() during the shutdown sequence in do_toggle()
    ///
    /// If the timeout expires then the client will immediately send the
    /// batch waiting another configurable amount of time before it abruptly
    /// cancels the operation.
    _as.request_abort();
    vlog(adtlog.info, "Waiting for audit client to shutdown");
    static constexpr auto client_drain_wait_timeout = 3s;
    try {
        co_await _send_sem.wait(client_drain_wait_timeout, _max_buffer_size);
    } catch (const ss::semaphore_timed_out&) {
        vlog(
          adtlog.warn,
          "Timed out after {}ms waiting for records to be sent from the audit "
          "client",
          client_drain_wait_timeout);
    }
    _send_sem.broken();
    co_await _client.stop();
    co_await _gate.close();
    _probe.reset(nullptr);
    vlog(adtlog.info, "Audit client stopped");
}

ss::future<> audit_client::produce(
  chunked_vector<partition_batch> records, audit_probe& probe) {
    auto total_size = absl::c_accumulate(
      records, size_t{0}, [](size_t acc, const partition_batch& b) {
          return acc + b.batch.size_bytes();
      });

    vlog(
      adtlog.trace,
      "Producing {} batches, totaling {}B, wait for semaphore units...",
      records.size(),
      total_size);

    auto reserved = co_await ss::get_units(_send_sem, total_size);

    absl::c_for_each(records, [&reserved](partition_batch& pb) {
        try {
            pb.send_units.emplace(reserved.split(pb.batch.size_bytes()));
        } catch (const std::invalid_argument& e) {
            // NOTE(oren): we should never reach here because reserved should
            // always begin with precisely the number of units needed for all
            // input batches.
            vassert(false, "Error getting units for batch: {}", e.what());
        }
    });

    // limit concurrency to the number of max-sized batches that the
    // audit_client could handle. In the common case, the number of batches
    // here should usually be 1-2, since the default per-shard queue limit
    // is 1MiB, which is also the default for kafka_batch_max_bytes.
    // TODO(oren): a configurabale ratio might be better
    auto max_concurrency = std::clamp<size_t>(
      _max_buffer_size / config::shard_local_cfg().kafka_batch_max_bytes(),
      1,
      records.size());

    try {
        ssx::spawn_with_gate(
          _gate,
          [this,
           &probe,
           max_concurrency,
           records = std::move(records)]() mutable {
              return ss::do_with(
                std::move(records),
                [this, &probe, max_concurrency](auto& records) mutable {
                    return ss::max_concurrent_for_each(
                      std::make_move_iterator(records.begin()),
                      std::make_move_iterator(records.end()),
                      max_concurrency,
                      [this,
                       &probe](partition_batch rec) mutable -> ss::future<> {
                          return do_produce(
                                   std::move(rec.batch), rec.pid, probe)
                            .finally([units = std::move(rec.send_units)] {});
                      });
                });
          });
    } catch (const ss::broken_semaphore&) {
        vlog(
          adtlog.debug,
          "Shutting down the auditor kafka::client, semaphore broken");
    }
    co_return;
}

ss::future<> audit_client::do_produce(
  model::record_batch batch, model::partition_id pid, audit_probe& probe) {
    // Effectively retry forever, but start a fresh request from batch data held
    // in memory when each produce_record_batch's retries are exhausted. This
    // way the kafka client should periodically refresh its internal metadata.
    std::optional<kafka::error_code> ec;
    while (!_as.abort_requested()) {
        auto r = co_await _client.produce_record_batch(
          model::topic_partition{model::kafka_audit_logging_topic, pid},
          batch.copy());
        ec.emplace(r.error_code);
        co_await update_status(ec.value());
        if (ec.value() == kafka::error_code::none) {
            break;
        }
    }

    // report unknown server error if we aborted before making any attempt
    if (auto errc = ec.value_or(kafka::error_code::unknown_server_error);
        errc != kafka::error_code::none) {
        vlog(
          adtlog.warn,
          "{} audit records dropped, shutting down. Last error: {}",
          batch.record_count(),
          errc);
        probe.audit_error();
    } else {
        probe.audit_event();
    }
}

/// audit_sink

audit_sink::audit_sink(
  audit_log_manager* audit_mgr,
  cluster::controller* controller,
  kafka::client::configuration& config) noexcept
  : _audit_mgr(audit_mgr)
  , _controller(controller)
  , _config(config) {}

ss::future<> audit_sink::start() {
    toggle(true);
    return ss::now();
}

ss::future<> audit_sink::stop() {
    vlog(adtlog.info, "stop() invoked on audit_sink");
    toggle(false);
    co_await _gate.close();
}

ss::future<>
audit_sink::update_auth_status(auth_misconfigured_t auth_misconfigured) {
    return _audit_mgr->container().invoke_on_all(
      [auth_misconfigured](audit_log_manager& mgr) {
          mgr._auth_misconfigured = (bool)auth_misconfigured;
      });
}

ss::future<> audit_sink::produce(chunked_vector<partition_batch> records) {
    /// No locks/gates since the calls to this method are done in controlled
    /// context of other synchronization primitives
    vassert(_client, "produce() called on a null client");
    co_await _client->produce(std::move(records), _audit_mgr->probe());
}

ss::future<> audit_sink::publish_app_lifecycle_event(
  application_lifecycle::activity_id event) {
    /// Directly publish the event instead of enqueuing it like all other
    /// events. This ensures that the event won't get discarded in the case
    /// audit is disabled.
    auto lifecycle_event = std::make_unique<application_lifecycle>(
      application_lifecycle::construct(event, ss::sstring{subsystem_name}));
    auto as_json = lifecycle_event->to_json();
    iobuf b;
    b.append(as_json.c_str(), as_json.size());
    auto batch
      = kafka::data::
          record_batcher{config::shard_local_cfg().kafka_batch_max_bytes(), &adtlog}
            .make_batch_of_one(std::nullopt, std::move(b));
    chunked_vector<partition_batch> rs;
    rs.emplace_back(_audit_mgr->compute_partition_id(), std::move(batch));
    co_await produce(std::move(rs));
}

void audit_sink::toggle(bool enabled) {
    vlog(adtlog.info, "Setting auditing enabled state to: {}", enabled);
    ssx::spawn_with_gate(_gate, [this, enabled]() {
        return _mutex.with(5s, [this, enabled] { return do_toggle(enabled); })
          .handle_exception_type(
            [this, enabled](const ss::semaphore_timed_out&) {
                /// If within 5s the mutex cannot be aquired AND the client is
                /// stuck in an initialization loop, then allow it to exit.
                if (
                  !enabled && _client && !_client->is_initialized()
                  && !_early_exit_future.has_value()) {
                    _early_exit_future = _client->shutdown();
                }
            });
    });
}

ss::future<> audit_sink::do_toggle(bool enabled) {
    if (enabled && !_client) {
        _client = std::make_unique<audit_client>(this, _controller, _config);
        co_await _client->initialize();
        if (_client->is_initialized()) {
            co_await publish_app_lifecycle_event(
              application_lifecycle::activity_id::start);
            co_await _audit_mgr->resume();
            vlog(adtlog.info, "Auditing fibers started");
        } else if (_early_exit_future.has_value()) {
            /// This is for shutting down the client when initialize() hasn't
            /// completed.
            ///
            /// This special future allows the shutdown method to still execute
            /// under the scope the mutex, even though it was initiated outside
            /// outside the scope of the mutex.
            co_await std::move(*_early_exit_future);
            _early_exit_future = std::nullopt;
            _client.reset(nullptr);
        } else {
            /// There is currently no known way this could occur, that is
            /// because initialize() should loop forever in the case it cannot
            /// fully succeed.
            vlog(
              adtlog.warn,
              "Client initialization exited in an unexpected manner");
        }
    } else if (!enabled && _client) {
        co_await publish_app_lifecycle_event(
          application_lifecycle::activity_id::stop);
        co_await _audit_mgr->pause();
        vlog(adtlog.info, "Auditing fibers stopped");
        co_await _client->shutdown();
        _client.reset(nullptr);
    } else {
        vlog(
          adtlog.info,
          "Ignored update to audit_enabled(), auditing is already {}",
          (enabled ? "enabled" : "disabled"));
    }
}

/// audit_log_manager

void audit_log_manager::set_enabled_events() {
    using underlying_enum_t = std::underlying_type_t<event_type>;
    _enabled_event_types = underlying_enum_t(0);
    for (const auto& e : _audit_event_types()) {
        const auto as_uint = underlying_enum_t(string_to_event_type(e));
        _enabled_event_types[as_uint] = true;
    }
    vassert(
      !is_audit_event_enabled(event_type::unknown),
      "Unknown event_type observed");
}

bool audit_log_manager::recovery_mode_enabled() noexcept {
    return config::node().recovery_mode_enabled.value();
}

audit_log_manager::audit_log_manager(
  model::node_id self,
  cluster::controller* controller,
  kafka::client::configuration& client_config,
  ss::sharded<cluster::metadata_cache>* metadata_cache)
  : _audit_enabled(config::shard_local_cfg().audit_enabled.bind())
  , _queue_drain_interval_ms(
      config::shard_local_cfg().audit_queue_drain_interval_ms.bind())
  , _audit_event_types(
      config::shard_local_cfg().audit_enabled_event_types.bind())
  , _max_queue_size_bytes(
      config::shard_local_cfg().audit_queue_max_buffer_size_per_shard())
  , _audit_excluded_topics_binding(
      config::shard_local_cfg().audit_excluded_topics.bind())
  , _audit_excluded_principals_binding(
      config::shard_local_cfg().audit_excluded_principals.bind())
  , _queue_bytes_sem(_max_queue_size_bytes, "s/audit/buffer")
  , _self(self)
  , _controller(controller)
  , _config(client_config)
  , _metadata_cache(metadata_cache) {
    _probe.setup_metrics([this] {
        return 1.0
               - (static_cast<double>(_queue_bytes_sem.available_units()) / static_cast<double>(_max_queue_size_bytes));
    });
    if (ss::this_shard_id() == client_shard_id) {
        _sink = std::make_unique<audit_sink>(this, controller, client_config);
    }

    _drain_timer.set_callback([this] {
        ssx::spawn_with_gate(_gate, [this]() {
            return ss::get_units(_active_drain, 1)
              .then([this](auto units) mutable {
                  return drain()
                    .handle_exception([&probe = probe()](std::exception_ptr e) {
                        vlog(
                          adtlog.warn,
                          "Exception in audit_log_manager fiber: {}",
                          e);
                        probe.audit_error();
                    })
                    .finally([this, units = std::move(units)] {
                        _drain_timer.arm(_queue_drain_interval_ms());
                    });
              });
        });
    });
    set_enabled_events();
    _audit_event_types.watch([this] { set_enabled_events(); });
    _audit_excluded_topics_binding.watch([this] {
        _audit_excluded_topics.clear();
        const auto& excluded_topics = _audit_excluded_topics_binding();
        std::for_each(
          excluded_topics.cbegin(),
          excluded_topics.cend(),
          [this](const ss::sstring& topic) {
              _audit_excluded_topics.emplace(topic);
          });
    });
    _audit_excluded_principals_binding.watch([this] {
        _audit_excluded_principals.clear();
        const auto& excluded_principals = _audit_excluded_principals_binding();
        std::for_each(
          excluded_principals.cbegin(),
          excluded_principals.cend(),
          [this](const ss::sstring& principal) {
              if (principal.starts_with("User:")) {
                  _audit_excluded_principals.emplace(
                    // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers)
                    security::principal_type::user,
                    principal.substr(5));
              } else {
                  _audit_excluded_principals.emplace(
                    security::principal_type::user, principal);
              }
          });
    });
}

audit_log_manager::~audit_log_manager() = default;

bool audit_log_manager::is_audit_event_enabled(event_type event_type) const {
    using underlying_enum_t
      = std::underlying_type_t<security::audit::event_type>;
    return _enabled_event_types.test(underlying_enum_t(event_type));
}

ss::future<> audit_log_manager::start() {
    if (recovery_mode_enabled()) {
        vlog(
          adtlog.warn,
          "Redpanda is operating in recovery mode.  Auditing is disabled!");
        co_return;
    }
    if (ss::this_shard_id() != client_shard_id) {
        co_return;
    }
    _audit_enabled.watch([this] {
        try {
            _sink->toggle(_audit_enabled());
        } catch (const ss::gate_closed_exception&) {
            vlog(
              adtlog.debug, "Failed to toggle auditing state, shutting down");
        } catch (...) {
            vlog(
              adtlog.error,
              "Failed to toggle auditing state: {}",
              std::current_exception());
        }
    });
    if (_audit_enabled()) {
        vlog(adtlog.info, "Starting audit_log_manager");
        co_await _sink->start();
    }
}

ss::future<> audit_log_manager::stop() {
    _drain_timer.cancel();
    _as.request_abort();
    if (ss::this_shard_id() == client_shard_id) {
        vlog(adtlog.info, "Shutting down audit log manager");
        co_await _sink->stop();
    }
    if (!_gate.is_closed()) {
        /// Gate may already be closed if ::pause() had been called
        co_await _gate.close();
    }
    if (_queue.size() > 0) {
        vlog(
          adtlog.debug,
          "{} records were not pushed to the audit log before shutdown",
          _queue.size());
    }
}

ss::future<> audit_log_manager::pause() {
    return container().invoke_on_all([](audit_log_manager& mgr) {
        mgr._effectively_enabled = false;
        /// Wait until drain() has completed, with timer cancelled it can be
        /// ensured no more work will be performed
        return ss::get_units(mgr._active_drain, 1).then([&mgr](auto) {
            mgr._drain_timer.cancel();
            return mgr.drain().handle_exception(
              [&mgr](const std::exception_ptr& e) {
                  vlog(
                    adtlog.warn, "Exception in audit_log_manager fiber: {}", e);
                  mgr.probe().audit_error();
              });
        });
    });
}

ss::future<> audit_log_manager::resume() {
    return container().invoke_on_all([](audit_log_manager& mgr) {
        /// If the timer is already armed that is a bug
        vassert(
          !mgr._drain_timer.armed(),
          "Timer is already armed upon call to ::resume");
        mgr._effectively_enabled = true;
        mgr._drain_timer.arm(mgr._queue_drain_interval_ms());
    });
}

bool audit_log_manager::report_redpanda_app_event(is_started app_started) {
    return enqueue_app_lifecycle_event(
      app_started == is_started::yes
        ? application_lifecycle::activity_id::start
        : application_lifecycle::activity_id::stop);
}

model::partition_id audit_log_manager::compute_partition_id() {
    static thread_local model::partition_id _next_pid{0};

    model::topic_namespace_view ns_tp{model::kafka_audit_logging_nt};
    auto cfg = _metadata_cache->local().get_topic_cfg(ns_tp);
    if (!cfg.has_value()) {
        vlog(
          adtlog.debug,
          "{} missing from metadata cache, fall back to round-robin candidate "
          "{}",
          ns_tp,
          _next_pid);
        return _next_pid;
    }
    auto n_partitions = cfg.value().partition_count;
    vassert(n_partitions >= 0, "Invalid partition count {}", n_partitions);

    auto inc_pid = [n_partitions](model::partition_id pid, int32_t inc = 1) {
        return model::partition_id{(pid + inc) % n_partitions};
    };

    const auto& partition_leaders
      = _controller->get_partition_leaders().local();

    std::optional<model::partition_id> pid;
    for (auto i : boost::irange(n_partitions)) {
        auto try_pid = inc_pid(_next_pid, i);
        auto leader = partition_leaders.get_leader(ns_tp, try_pid);
        if (!leader.has_value()) {
            continue;
        } else if (leader.value() == _self) {
            vlog(adtlog.debug, "Node {} leads partition {}", _self, try_pid);
            pid.emplace(try_pid);
            break;
        }
    }

    // NOTE(oren): sort of arbitrary. if we didn't find a locally led partition,
    // then at least advance the round robin to the next PID in natural order
    _next_pid = inc_pid(pid.value_or(_next_pid));
    return pid.value_or(_next_pid);
}

ss::future<> audit_log_manager::drain() {
    if (_queue.empty()) {
        co_return;
    }

    vlog(
      adtlog.debug,
      "Attempting to drain {} audit events from sharded queue",
      _queue.size());

    /// Combine all queued audit msgs into record_batches
    kafka::data::record_batcher batcher{
      config::shard_local_cfg().kafka_batch_max_bytes(), &adtlog};

    auto records = std::exchange(_queue, underlying_t{});
    auto& records_seq = records.get<underlying_list>();
    while (!records_seq.empty()) {
        auto first = records_seq.extract(records_seq.begin());
        auto audit_msg = std::move(first.value()).release();
        auto as_json = audit_msg->to_json();
        iobuf b;
        b.append(as_json.c_str(), as_json.size());
        batcher.append(std::nullopt, std::move(b));

        co_await ss::coroutine::maybe_yield();
    }

    auto batches = std::move(batcher).finish();

    chunked_vector<partition_batch> p_batches;
    p_batches.reserve(batches.size());

    // attach a partition ID to each batch and call into the audit_sink

    std::transform(
      std::make_move_iterator(batches.begin()),
      std::make_move_iterator(batches.end()),
      std::back_inserter(p_batches),
      [this](model::record_batch recs) {
          return partition_batch{
            .pid = compute_partition_id(),
            .batch = std::move(recs),
          };
      });

    /// This call may block if the audit_clients semaphore is exhausted,
    /// this represents the amount of memory used within its kafka::client
    /// produce batch queue. If the semaphore blocks it will apply
    /// backpressure here, and the \ref _queue will begin to fill closer to
    /// capacity. When it hits capacity, enqueue_audit_event() will block.
    co_await container().invoke_on(
      client_shard_id,
      [recs = std::move(p_batches)](audit_log_manager& mgr) mutable {
          return mgr._sink->produce(std::move(recs));
      });
}

std::optional<audit_log_manager::audit_event_passthrough>
audit_log_manager::should_enqueue_audit_event() const {
    if (recovery_mode_enabled() || !_audit_enabled()) {
        return std::make_optional(audit_event_passthrough::yes);
    }
    if (_as.abort_requested()) {
        /// Prevent auditing new messages when shutdown starts that way the
        /// queue may be entirely flushed before shutdown
        return std::make_optional(audit_event_passthrough::no);
    }
    const auto& feature_table = _controller->get_feature_table();
    if (!feature_table.local().is_active(features::feature::audit_logging)) {
        vlog(
          adtlog.warn,
          "Audit message passthrough active until cluster has been fully "
          "upgraded to the min supported version for audit_logging");
        _probe.audit_error();
        return std::make_optional(audit_event_passthrough::yes);
    }
    if (_auth_misconfigured) {
        /// Audit logging depends on having auth enabled, if it is not
        /// then messages are rejected for increased observability into why
        /// things are not working.
        vlog(
          adtlog.warn,
          "Audit message rejected due to misconfigured authorization");
        return std::make_optional(audit_event_passthrough::no);
    }
    return std::nullopt;
}

std::optional<audit_log_manager::audit_event_passthrough>
audit_log_manager::should_enqueue_audit_event(
  event_type type, ignore_enabled_events ignore_events) const {
    if (
      ignore_events == ignore_enabled_events::no
      && !is_audit_event_enabled(type)) {
        return std::make_optional(audit_event_passthrough::yes);
    }
    return should_enqueue_audit_event();
}

std::optional<audit_log_manager::audit_event_passthrough>
audit_log_manager::should_enqueue_audit_event(
  event_type type,
  const security::acl_principal& principal,
  ignore_enabled_events ignore_events) const {
    if (_audit_excluded_principals.contains(principal)) {
        return std::make_optional(audit_event_passthrough::yes);
    }

    return should_enqueue_audit_event(type, ignore_events);
}

std::optional<audit_log_manager::audit_event_passthrough>
audit_log_manager::should_enqueue_audit_event(
  kafka::api_key key,
  const security::acl_principal& principal,
  ignore_enabled_events ignore_events) const {
    return should_enqueue_audit_event(
      kafka_api_to_event_type(key), principal, ignore_events);
}

std::optional<audit_log_manager::audit_event_passthrough>
audit_log_manager::should_enqueue_audit_event(
  event_type type, const ss::sstring& username) const {
    return should_enqueue_audit_event(
      type, security::acl_principal{security::principal_type::user, username});
}

std::optional<audit_log_manager::audit_event_passthrough>
audit_log_manager::should_enqueue_audit_event(
  event_type type, const security::audit::user& user) const {
    return should_enqueue_audit_event(
      type, security::acl_principal{security::principal_type::user, user.name});
}

std::optional<audit_log_manager::audit_event_passthrough>
audit_log_manager::should_enqueue_audit_event(
  kafka::api_key key,
  const security::acl_principal& principal,
  const model::topic& t) const {
    auto ignore_events = ignore_enabled_events::no;
    if (_audit_excluded_topics.contains(t)) {
        return std::make_optional(audit_event_passthrough::yes);
    } else if (t == model::kafka_audit_logging_topic) {
        ignore_events = ignore_enabled_events::yes;
    }

    return should_enqueue_audit_event(key, principal, ignore_events);
}

} // namespace security::audit
