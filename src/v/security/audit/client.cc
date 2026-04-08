/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "client.h"

#include "base/vlog.h"
#include "cluster/controller.h"
#include "cluster/ephemeral_credential_frontend.h"
#include "cluster/security_frontend.h"
#include "config/configuration.h"
#include "kafka/client/client.h"
#include "kafka/data/record_batcher.h"
#include "kafka/data/rpc/client.h"
#include "kafka/protocol/topic_properties.h"
#include "security/audit/audit_log_manager.h"
#include "security/audit/logger.h"
#include "utils/retry.h"

#include <algorithm>
#include <chrono>

namespace security::audit {

using auth_misconfigured_t = ss::bool_class<struct auth_misconfigured_tag>;

namespace internal {

class rpc_client_impl final : public audit_client {
public:
    rpc_client_impl(
      audit_sink* sink,
      cluster::controller* controller,
      kafka::data::rpc::client* rpc_client)
      : audit_client(sink, controller)
      , _rpc_client(rpc_client) {}
    rpc_client_impl(const rpc_client_impl&) = delete;
    rpc_client_impl& operator=(const rpc_client_impl&) = delete;
    rpc_client_impl(rpc_client_impl&&) = delete;
    rpc_client_impl& operator=(rpc_client_impl&&) = delete;
    ~rpc_client_impl() final = default;

    ss::future<> do_produce(
      model::record_batch batch,
      model::partition_id pid,
      audit_probe& probe,
      ss::timer<>::time_point timeout) final {
        constexpr auto map_ec = [](cluster::errc ec) -> kafka::error_code {
            vlog(adtlog.debug, "Audit data produce result: {}", ec);
            if (ec == cluster::errc::success) {
                return kafka::error_code::none;
            }
            return kafka::error_code::unknown_server_error;
        };

        static constexpr auto base_backoff = 500ms;
        static constexpr uint32_t max_backoff = 8;
        exp_backoff_policy backoff;

        std::optional<kafka::error_code> ec;
        while (!as().abort_requested() && ss::timer<>::clock::now() < timeout) {
            auto r = co_await _rpc_client->produce(
              model::topic_partition{model::kafka_audit_logging_topic, pid},
              batch.copy());
            ec.emplace(map_ec(r));
            if (ec.value() == kafka::error_code::none) {
                break;
            }
            auto delay = base_backoff
                         * std::min(max_backoff, backoff.next_backoff());
            try {
                co_await ss::sleep_abortable<ss::lowres_clock>(delay, as());
            } catch (const ss::sleep_aborted&) {
                break;
            }
        }

        // report unknown server error if we aborted before making any attempt
        if (
          auto errc = ec.value_or(kafka::error_code::unknown_server_error);
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
    ss::future<> client_shutdown() final {
        /// Nothing to do for the rpc client
        return ss::now();
    }
    ss::future<> create_internal_topic() {
        constexpr auto seven_days = 604800000ms;
        using namespace std::chrono_literals;

        int16_t replication_factor
          = config::shard_local_cfg().audit_log_replication_factor().value_or(
            controller()->internal_topic_replication());
        vlog(
          adtlog.debug,
          "Attempting to create internal topic (replication={})",
          replication_factor);

        cluster::topic_properties audit_topic_props;
        audit_topic_props.retention_bytes = tristate<size_t>{};
        audit_topic_props.retention_duration
          = tristate<std::chrono::milliseconds>{seven_days};
        audit_topic_props.cleanup_policy_bitflags
          = model::cleanup_policy_bitflags::deletion;
        vlog(
          adtlog.info,
          "Creating audit log topic with settings: {}",
          audit_topic_props);
        const auto ec = co_await _rpc_client->create_topic(
          model::kafka_audit_logging_nt,
          std::move(audit_topic_props),
          config::shard_local_cfg().audit_log_num_partitions(),
          replication_factor);
        if (ec == cluster::errc::success) {
            vlog(
              adtlog.debug,
              "Auditing: created audit log topic: {}",
              model::kafka_audit_logging_topic);
        } else if (ec == cluster::errc::topic_already_exists) {
            vlog(adtlog.debug, "Auditing: topic already exists");
        } else {
            if (ec == cluster::errc::topic_invalid_replication_factor) {
                vlog(
                  adtlog.warn,
                  "Auditing: invalid replication factor on audit topic, "
                  "check/modify settings, then disable and re-enable "
                  "'audit_enabled'");
            }
            throw std::runtime_error(
              fmt::format(
                "Error creating audit log topic - error_code: {}", ec));
        }
    }

    ss::future<> do_configure() final {
        co_await create_internal_topic();
        // TODO: may want to configure retries on the rpc client
    }

private:
    kafka::data::rpc::client* _rpc_client;
};

class rpc_sink_impl final : public audit_sink {
public:
    rpc_sink_impl(
      audit_log_manager* audit_mgr,
      cluster::controller* controller,
      kafka::data::rpc::client* rpc_client)
      : audit_sink(
          audit_mgr, controller, true) /* rpc sink is active on every shard */
      , _rpc_client(rpc_client) {}

    rpc_sink_impl(const rpc_sink_impl&) = delete;
    rpc_sink_impl& operator=(const rpc_sink_impl&) = delete;
    rpc_sink_impl(rpc_sink_impl&&) = delete;
    rpc_sink_impl& operator=(rpc_sink_impl&&) = delete;
    ~rpc_sink_impl() final = default;

    void make_client() final {
        _client = std::make_unique<rpc_client_impl>(
          this, controller(), _rpc_client);
    }

    audit_client* client() final { return _client.get(); }
    void reset_client() final { _client.reset(nullptr); }

    ss::future<> pause() final { return manager()->pause(); }
    ss::future<> resume() final { return manager()->resume(); }

private:
    kafka::data::rpc::client* _rpc_client;
    std::unique_ptr<rpc_client_impl> _client;
};

class kafka_sink_impl;

class kafka_client_impl final : public audit_client {
public:
    kafka_client_impl(
      audit_sink* sink,
      cluster::controller* controller,
      kafka::client::configuration& client_config)
      : audit_client(sink, controller)
      , _client(
          config::to_yaml(client_config, config::redact_secrets::no),
          [this](std::exception_ptr eptr) { return mitigate_error(eptr); }) {}
    kafka_client_impl(const kafka_client_impl&) = delete;
    kafka_client_impl& operator=(const kafka_client_impl&) = delete;
    kafka_client_impl(kafka_client_impl&&) = delete;
    kafka_client_impl& operator=(kafka_client_impl&&) = delete;
    ~kafka_client_impl() final = default;

    ss::future<> do_produce(
      model::record_batch batch,
      model::partition_id pid,
      audit_probe& probe,
      ss::timer<>::time_point timeout) final {
        // Effectively retry forever, but start a fresh request from batch data
        // held in memory when each produce_record_batch's retries are
        // exhausted. This way the kafka client should periodically refresh its
        // internal metadata.
        std::optional<kafka::error_code> ec;
        while (!as().abort_requested() && ss::timer<>::clock::now() < timeout) {
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
        if (
          auto errc = ec.value_or(kafka::error_code::unknown_server_error);
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
    ss::future<> client_shutdown() final { co_await _client.stop(); }
    ss::future<> do_configure() final {
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
        _client.set_max_retries(size_t{5});
    }

private:
    kafka_sink_impl* sink();

    auth_misconfigured_t _misconfigured{auth_misconfigured_t::no};
    kafka::client::client _client;

    ss::future<> update_status(kafka::error_code errc);
    ss::future<> do_update_status(auth_misconfigured_t);

    ss::future<> update_status(kafka::produce_response response) {
        /// This method should almost always call update_status() with a value
        /// of no error code. That is because kafka client mitigation will be
        /// called in the case there is a produce error, and an erraneous
        /// response will only be returned when the retry count is exhausted,
        /// which will never occur since it is artificially set high to have the
        /// effect of always retrying
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

    ss::future<> mitigate_error(std::exception_ptr eptr) {
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

    ss::future<> inform(model::node_id id) {
        vlog(adtlog.trace, "inform: {}", id);

        // Inform a particular node
        if (id != kafka::client::unknown_node_id) {
            return do_inform(id);
        }

        // Inform all nodes
        return seastar::parallel_for_each(
          controller()->get_members_table().local().node_ids(),
          [this](model::node_id id) { return do_inform(id); });
    }

    ss::future<> do_inform(model::node_id id) {
        auto& fe = controller()->get_ephemeral_credential_frontend().local();
        auto ec = co_await fe.inform(id, audit_principal);
        vlog(adtlog.info, "Informed: broker: {}, ec: {}", id, ec);
    }

    ss::future<> set_client_credentials() {
        /// Set ephemeral credential
        auto& frontend
          = controller()->get_ephemeral_credential_frontend().local();
        auto pw = co_await frontend.get(audit_principal);
        if (pw.err != cluster::errc::success) {
            throw std::runtime_error(
              fmt::format(
                "Failed to fetch credential for principal: {}",
                audit_principal));
        }

        _client.set_credentials(
          kafka::client::sasl_configuration{
            .mechanism = pw.credential.mechanism(),
            .username = pw.credential.user()(),
            .password = pw.credential.password()(),
          });
    }

    ss::future<> set_auditing_permissions() {
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

        co_await controller()->get_security_frontend().local().create_acls(
          {security::acl_binding{audit_topic_pattern, acl_create_entry},
           security::acl_binding{audit_topic_pattern, acl_write_entry}},
          5s);
    }

    ss::future<> create_internal_topic() {
        constexpr std::string_view retain_forever = "-1";
        constexpr std::string_view seven_days = "604800000";
        int16_t replication_factor
          = config::shard_local_cfg().audit_log_replication_factor().value_or(
            controller()->internal_topic_replication());
        vlog(
          adtlog.debug,
          "Attempting to create internal topic (replication={})",
          replication_factor);
        kafka::creatable_topic audit_topic{
          .name = model::kafka_audit_logging_topic,
          .num_partitions
          = config::shard_local_cfg().audit_log_num_partitions(),
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
          adtlog.info,
          "Creating audit log topic with settings: {}",
          audit_topic);
        const auto resp = co_await _client.create_topic(
          {std::move(audit_topic)});
        if (resp.data.topics.size() != 1) {
            throw std::runtime_error(
              fmt::format("Unexpected create topics response: {}", resp.data));
        }
        const auto& topic = resp.data.topics[0];
        if (topic.error_code == kafka::error_code::none) {
            vlog(adtlog.debug, "Auditing: created audit log topic: {}", topic);
        } else if (
          topic.error_code == kafka::error_code::topic_already_exists) {
            vlog(adtlog.debug, "Auditing: topic already exists");
            co_await _client.update_metadata();
        } else {
            if (
              topic.error_code
              == kafka::error_code::invalid_replication_factor) {
                vlog(
                  adtlog.warn,
                  "Auditing: invalid replication factor on audit topic, "
                  "check/modify settings, then disable and re-enable "
                  "'audit_enabled'");
            }
            const auto msg = topic.error_message.has_value()
                               ? *topic.error_message
                               : "<no_err_msg>";
            throw std::runtime_error(
              fmt::format("{} - error_code: {}", msg, topic.error_code));
        }
    }
};

class kafka_sink_impl final : public audit_sink {
public:
    kafka_sink_impl(
      audit_log_manager* audit_mgr,
      cluster::controller* controller,
      kafka::client::configuration& config,
      audit_sink::update_auth_fn update_auth_status)
      : audit_sink(
          audit_mgr,
          controller,
          ss::this_shard_id()
            == audit_sink::client_shard_id) /* kafka sink only active on shard 0
                                             */
      , _config(config)
      , _update_auth_status(std::move(update_auth_status)) {}

    kafka_sink_impl(const kafka_sink_impl&) = delete;
    kafka_sink_impl& operator=(const kafka_sink_impl&) = delete;
    kafka_sink_impl(kafka_sink_impl&&) = delete;
    kafka_sink_impl& operator=(kafka_sink_impl&&) = delete;
    ~kafka_sink_impl() final = default;

    void make_client() final {
        _client = std::make_unique<kafka_client_impl>(
          this, controller(), std::ref(_config));
    }

    audit_client* client() final { return _client.get(); }

    void reset_client() final { _client.reset(nullptr); }

    ss::future<> pause() final {
        return manager()->container().invoke_on_all(
          [](audit_log_manager& mgr) { return mgr.pause(); });
    }
    ss::future<> resume() final {
        return manager()->container().invoke_on_all(
          [](audit_log_manager& mgr) { return mgr.resume(); });
    }

    ss::future<> update_auth_status(auth_misconfigured_t auth_misconfigured) {
        return _update_auth_status((bool)auth_misconfigured);
    }

private:
    kafka::client::configuration& _config;
    audit_sink::update_auth_fn _update_auth_status;
    std::unique_ptr<kafka_client_impl> _client;
};

} // namespace internal

audit_client::audit_client(audit_sink* sink, cluster::controller* controller)
  : _max_buffer_size(config::shard_local_cfg().audit_client_max_buffer_size())
  , _send_sem(_max_buffer_size, "audit_log_producer_semaphore")
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
            auto avail = static_cast<double>(_send_sem.available_units());
            auto max = static_cast<double>(_max_buffer_size);
            return 1.0 - (avail / max);
        });
    }
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
        co_await do_configure();
        vlog(adtlog.info, "Audit log client initialized");
    } catch (...) {
        vlog(
          adtlog.warn,
          "Audit log client failed to initialize: {}",
          std::current_exception());
        throw;
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
    co_await client_shutdown();
    co_await _gate.close();
    _probe.reset(nullptr);
    vlog(adtlog.info, "Audit client stopped");
}

ss::future<> audit_client::produce(
  chunked_vector<partition_batch> records,
  audit_probe& probe,
  std::optional<ss::timer<>::duration> timeout) {
    auto total_size = absl::c_accumulate(
      records, size_t{0}, [](size_t acc, const partition_batch& b) {
          return acc + b.batch.size_bytes();
      });

    vlog(
      adtlog.trace,
      "Producing {} batches, totaling {}B, wait for semaphore units...",
      records.size(),
      total_size);

    auto timepoint = timeout.has_value() ? ss::timer<>::clock::now() + *timeout
                                         : ss::timer<>::time_point::max();
    auto reserved = co_await ss::get_units(_send_sem, total_size, timepoint);

    std::ranges::for_each(records, [&reserved](partition_batch& pb) {
        try {
            pb.send_units.emplace(reserved.split(pb.batch.size_bytes()));
        } catch (const std::invalid_argument& e) {
            // NOTE: we should never reach here because reserved should
            // always begin with precisely the number of units needed for all
            // input batches.
            vunreachable("Error getting units for batch: {}", e.what());
        }
    });

    // limit concurrency to the number of max-sized batches that the
    // audit_client could handle. In the common case, the number of batches
    // here should usually be 1-2, since the default per-shard queue limit
    // is 1MiB, which is also the default for kafka_batch_max_bytes.
    // TODO: a configurabale ratio might be better
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
           records = std::move(records),
           timepoint]() mutable {
              return ss::do_with(
                std::move(records),
                timepoint,
                [this, &probe, max_concurrency](
                  auto& records, ss::timer<>::time_point& timeout) mutable {
                    return ss::max_concurrent_for_each(
                      std::make_move_iterator(records.begin()),
                      std::make_move_iterator(records.end()),
                      max_concurrency,
                      [this, &probe, &timeout](
                        partition_batch rec) mutable -> ss::future<> {
                          return do_produce(
                                   std::move(rec.batch),
                                   rec.pid,
                                   probe,
                                   timeout)
                            .finally([units = std::move(rec.send_units)] {});
                      });
                });
          });
    } catch (const ss::broken_semaphore&) {
        vlog(adtlog.debug, "Shutting down the audit client, semaphore broken");
    }
    co_return;
}

ss::future<> audit_sink::start() {
    toggle(true);
    return ss::now();
}

ss::future<> audit_sink::stop() {
    vlog(adtlog.info, "stop() invoked on audit_sink");
    toggle(false);
    co_await _gate.close();
}

ss::future<> audit_sink::produce(
  chunked_vector<partition_batch> records,
  std::optional<ss::timer<>::duration> timeout) {
    /// No locks/gates since the calls to this method are done in controlled
    /// context of other synchronization primitives

    vassert(client(), "produce() called on a null client");
    co_await client()->produce(
      std::move(records), _audit_mgr->probe(), timeout);
}

static constexpr std::string_view subsystem_name = "Audit System";

ss::future<> audit_sink::publish_app_lifecycle_event(
  application_lifecycle::activity_id event) {
    if (ss::this_shard_id() != audit_log_manager::client_shard_id) {
        co_return;
    }
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

    auto timeout = _audit_mgr->_audit_log_reject_policy()
                       == config::audit_failure_policy::permit
                     ? std::make_optional(5s)
                     : std::nullopt;

    try {
        co_await produce(std::move(rs), timeout);
    } catch (ss::semaphore_timed_out& e) {
        vlog(
          adtlog.error,
          "Failed to produce application lifecycle event: {}",
          e.what());
    }
}

void audit_sink::toggle(bool enabled) {
    if (!_active) {
        return;
    }
    vlog(adtlog.info, "Setting auditing enabled state to: {}", enabled);
    ssx::spawn_with_gate(_gate, [this, enabled]() {
        return _mutex.with(5s, [this, enabled] { return do_toggle(enabled); })
          .handle_exception_type(
            [this, enabled](const ss::semaphore_timed_out&) {
                /// If within 5s the mutex cannot be aquired AND the client is
                /// stuck in an initialization loop, then allow it to exit.
                if (
                  !enabled && client() && !client()->is_initialized()
                  && !_early_exit_future.has_value()) {
                    _early_exit_future = client()->shutdown();
                }
            });
    });
}

ss::future<> audit_sink::do_toggle(bool enabled) {
    if (enabled && !client()) {
        make_client();
        co_await client()->initialize();
        if (client()->is_initialized()) {
            co_await publish_app_lifecycle_event(
              application_lifecycle::activity_id::start);
            co_await resume();
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
            reset_client();
        } else {
            /// There is currently no known way this could occur, that is
            /// because initialize() should loop forever in the case it cannot
            /// fully succeed.
            vlog(
              adtlog.warn,
              "Client initialization exited in an unexpected manner");
        }
    } else if (!enabled && client()) {
        co_await publish_app_lifecycle_event(
          application_lifecycle::activity_id::stop);
        co_await pause();
        vlog(adtlog.info, "Auditing fibers stopped");
        co_await client()->shutdown();
        reset_client();
    } else {
        vlog(
          adtlog.info,
          "Ignored update to audit_enabled(), auditing is already {}",
          (enabled ? "enabled" : "disabled"));
    }
}
namespace internal {

kafka_sink_impl* kafka_client_impl::sink() {
    return dynamic_cast<kafka_sink_impl*>(audit_client::sink());
}
ss::future<>
kafka_client_impl::do_update_status(auth_misconfigured_t misconfigured) {
    _misconfigured = misconfigured;
    return sink()->update_auth_status(misconfigured);
}

ss::future<> kafka_client_impl::update_status(kafka::error_code errc) {
    /// If the status changed to erroneous from anything else
    if (errc == kafka::error_code::illegal_sasl_state && !_misconfigured) {
        return do_update_status(auth_misconfigured_t::yes);
    }

    constexpr auto failed_codes = std::to_array(
      {kafka::error_code::illegal_sasl_state,
       kafka::error_code::broker_not_available});
    const bool success = !std::ranges::contains(failed_codes, errc);
    if (success && _misconfigured) {
        /// The status changed from erroneous to anything else
        return do_update_status(auth_misconfigured_t::no);
    }

    /// No change in status
    return ss::make_ready_future();
}
} // namespace internal

audit_sink::~audit_sink() = default;

std::unique_ptr<audit_sink> audit_sink::make_rpc_sink(
  audit_log_manager* m, cluster::controller* c, kafka::data::rpc::client* r) {
    return std::make_unique<internal::rpc_sink_impl>(m, c, r);
}

std::unique_ptr<audit_sink> audit_sink::make_kafka_sink(
  audit_log_manager* m,
  cluster::controller* c,
  kafka::client::configuration& k,
  audit_sink::update_auth_fn set_auth_status) {
    return std::make_unique<internal::kafka_sink_impl>(
      m, c, k, std::move(set_auth_status));
}

} // namespace security::audit
