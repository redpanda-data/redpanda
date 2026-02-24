/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "pandaproxy/schema_registry/kafka_client_transport.h"

#include "cluster/cluster_link/frontend.h"
#include "cluster/controller.h"
#include "cluster/ephemeral_credential_frontend.h"
#include "cluster/members_table.h"
#include "cluster/security_frontend.h"
#include "config/broker_authn_endpoint.h"
#include "config/configuration.h"
#include "kafka/client/client.h"
#include "kafka/client/client_fetch_batch_reader.h"
#include "kafka/client/config_utils.h"
#include "kafka/client/exceptions.h"
#include "kafka/protocol/create_topics.h"
#include "kafka/server/handlers/topics/types.h"
#include "model/namespace.h"
#include "pandaproxy/logger.h"
#include "pandaproxy/schema_registry/exceptions.h"
#include "security/acl.h"
#include "security/credential_store.h"
#include "security/ephemeral_credential_store.h"

#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/parallel_for_each.hh>

namespace pandaproxy::schema_registry {

namespace {

ss::future<> create_acls(cluster::security_frontend& security_fe) {
    std::vector<security::acl_binding> princpal_acl_binding{
      security::acl_binding{
        security::resource_pattern{
          security::resource_type::topic,
          model::schema_registry_internal_tp.topic,
          security::pattern_type::literal},
        security::acl_entry{
          security::schema_registry_principal,
          security::acl_host::wildcard_host(),
          security::acl_operation::all,
          security::acl_permission::allow}}};

    auto err_vec = co_await security_fe.create_acls(princpal_acl_binding, 5s);
    auto it = std::find_if(err_vec.begin(), err_vec.end(), [](const auto& err) {
        return err != cluster::errc::success;
    });

    if (it != err_vec.end()) {
        vlog(
          srlog.warn,
          "Failed to create ACLs for {}, err {} - {}",
          security::schema_registry_principal,
          *it,
          cluster::make_error_code(*it).message());
    } else {
        vlog(
          srlog.debug,
          "Successfully created ACLs for {}",
          security::schema_registry_principal);
    }
}

} // namespace

kafka_client_transport::kafka_client_transport(
  ss::sharded<kafka::client::client>& client,
  const YAML::Node& client_config,
  std::unique_ptr<cluster::controller>& controller)
  : _client(client)
  , _client_config(client_config)
  , _controller(controller) {}

ss::future<produce_result>
kafka_client_transport::produce(model::record_batch batch) {
    try {
        auto res = co_await _client.local().produce_record_batch(
          model::schema_registry_internal_tp, std::move(batch));
        if (res.error_code != kafka::error_code::none) {
            throw kafka::exception(
              res.error_code, res.error_message.value_or(""));
        }
        co_return produce_result{.base_offset = res.base_offset};
    } catch (const kafka::client::partition_error& ex) {
        if (
          ex.error == kafka::error_code::unknown_topic_or_partition
          && ex.tp.topic == model::schema_registry_internal_tp.topic) {
            throw exception(
              kafka::error_code::unknown_server_error,
              "_schemas topic does not exist");
        }
        throw;
    }
}

ss::future<model::offset> kafka_client_transport::get_high_watermark() {
    try {
        auto offsets = co_await _client.local().list_offsets(
          model::schema_registry_internal_tp);
        if (
          offsets.data.topics.size() != 1
          || offsets.data.topics[0].partitions.size() != 1) {
            throw kafka::exception(
              kafka::error_code::unknown_server_error,
              "Malformed ListOffsets Kafka response for internal topic");
        }
        co_return offsets.data.topics[0].partitions[0].offset;
    } catch (const kafka::client::partition_error& ex) {
        if (
          ex.error == kafka::error_code::unknown_topic_or_partition
          && ex.tp.topic == model::schema_registry_internal_tp.topic) {
            throw exception(
              kafka::error_code::unknown_server_error,
              "_schemas topic does not exist");
        }
        throw;
    }
}

ss::future<> kafka_client_transport::consume_range(
  model::offset start,
  model::offset end,
  ss::noncopyable_function<ss::future<>(model::record_batch)> consumer) {
    try {
        struct batch_consumer {
            ss::noncopyable_function<ss::future<>(model::record_batch)> fn;
            ss::future<ss::stop_iteration>
            operator()(model::record_batch batch) {
                return fn(std::move(batch)).then([] {
                    return ss::stop_iteration::no;
                });
            }
            void end_of_stream() {}
        };
        auto rdr = kafka::client::make_client_fetch_batch_reader(
          _client.local(), model::schema_registry_internal_tp, start, end);
        co_await std::move(rdr).consume(
          batch_consumer{std::move(consumer)}, model::no_timeout);
    } catch (const kafka::client::partition_error& ex) {
        if (
          ex.error == kafka::error_code::unknown_topic_or_partition
          && ex.tp.topic == model::schema_registry_internal_tp.topic) {
            throw exception(
              kafka::error_code::unknown_server_error,
              "_schemas topic does not exist");
        }
        throw;
    }
}

ss::future<> kafka_client_transport::configure() {
    auto sasl_config = co_await kafka::client::create_client_credentials(
      *_controller, _client_config, security::schema_registry_principal);
    co_await _client.invoke_on_all(
      [sasl_config = std::move(sasl_config)](kafka::client::client& c) {
          c.set_credentials(sasl_config);
      });

    const auto& store = _controller->get_ephemeral_credential_store().local();
    _has_ephemeral_credentials = store.has(
      store.find(security::schema_registry_principal));

    if (_has_ephemeral_credentials) {
        vlog(srlog.info, "[configure] Creating ACLs for ephemeral credentials");
        co_await create_acls(_controller->get_security_frontend().local());
    }
}

ss::future<> kafka_client_transport::mitigate_error(std::exception_ptr eptr) {
    vlog(srlog.warn, "mitigate_error: {}", eptr);
    return ss::make_exception_future<>(eptr)
      .handle_exception_type(
        [this, eptr](const kafka::client::broker_error& ex) {
            if (
              ex.error == kafka::error_code::sasl_authentication_failed
              && _has_ephemeral_credentials) {
                return inform(ex.node_id).then([this]() {
                    // This fully mitigates, don't rethrow.
                    return _client.local().connect();
                });
            }

            // Rethrow unhandled exceptions
            return ss::make_exception_future<>(eptr);
        })
      .handle_exception_type(
        [this, eptr](const kafka::client::partition_error& ex) {
            if (
              (ex.error == kafka::error_code::topic_authorization_failed
               || ex.error == kafka::error_code::unknown_topic_or_partition)
              && _has_ephemeral_credentials) {
                vlog(
                  srlog.info,
                  "Creating ACLs to mitigate partition error: {}",
                  ex);
                return create_acls(_controller->get_security_frontend().local())
                  .then([this]() { return _client.local().update_metadata(); });
            }

            return ss::make_exception_future<>(eptr);
        })
      .handle_exception_type(
        [this, eptr](const kafka::client::topic_error& ex) {
            if (
              (ex.error == kafka::error_code::topic_authorization_failed
               || ex.error == kafka::error_code::unknown_topic_or_partition)
              && _has_ephemeral_credentials) {
                vlog(
                  srlog.info,
                  "Creating ACLs to mitigate partition error: {}",
                  ex);
                return create_acls(_controller->get_security_frontend().local())
                  .then([this]() { return _client.local().update_metadata(); });
            }

            return ss::make_exception_future<>(eptr);
        });
}

ss::future<> kafka_client_transport::inform(model::node_id id) {
    vlog(srlog.trace, "inform: {}", id);

    // Inform a particular node
    if (id != kafka::client::unknown_node_id) {
        return do_inform(id);
    }

    // Inform all nodes
    return seastar::parallel_for_each(
      _controller->get_members_table().local().node_ids(),
      [this](model::node_id id) { return do_inform(id); });
}

ss::future<> kafka_client_transport::do_inform(model::node_id id) {
    auto& fe = _controller->get_ephemeral_credential_frontend().local();
    auto ec = co_await fe.inform(id, security::schema_registry_principal);
    vlog(srlog.info, "Informed: broker: {}, ec: {}", id, ec);
}

ss::future<> kafka_client_transport::validate_topic_creation_authorization(
  int16_t replication_factor) {
    kafka::metadata_request req;
    req.data.topics = {kafka::metadata_request_topic{
      .name = model::schema_registry_internal_tp.topic}};
    req.data.include_topic_authorized_operations = true;
    auto resp = co_await _client.local().fetch_metadata(std::move(req));
    vlog(srlog.trace, "Validating topic creation authorization");
    // If authz is not enabled on the cluster, then no need to validate
    // authn/authz
    if (!config::kafka_authz_enabled()) {
        co_return;
    }

    // If the client is not configured with a SCRAM user, it will be using
    // ephemeral credentials which are assumed to work
    if (!kafka::client::is_scram_configured(_client_config)) {
        co_return;
    }

    kafka::creatable_topic ct{
      .name{model::schema_registry_internal_tp.topic},
      .num_partitions = 1,
      .replication_factor = replication_factor,
    };

    auto res = co_await _client.local().create_topic(
      std::move(ct), kafka::client::client::validate_only_t::yes);

    if (res.data.topics.size() != 1) {
        throw kafka::exception(
          kafka::error_code::unknown_server_error,
          "Malformed CreateTopics Kafka response for internal topic");
    }

    const auto& topic_res = res.data.topics[0];
    if (
      topic_res.error_code == kafka::error_code::none
      || topic_res.error_code == kafka::error_code::topic_already_exists
      || (topic_res.error_code == kafka::error_code::topic_authorization_failed && shadow_linking_active())) {
        // if shadow linking is active, then the user must be a superuser to
        // create the topic via the Kafka API.  To continue with normal
        // operations, we will assume the user is authorized to create the
        // topic.
        vlog(srlog.trace, "User is properly authorized");
        co_return;
    }
    throw kafka::exception(
      topic_res.error_code,
      fmt::format(
        "User is not authorized to create internal schema registry topic "
        "'{}'",
        model::schema_registry_internal_tp.topic));
}

bool kafka_client_transport::has_ephemeral_credentials() const {
    return _has_ephemeral_credentials;
}

bool kafka_client_transport::shadow_linking_active() const {
    const auto& clfe = _controller->get_cluster_link_frontend().local();
    return clfe.cluster_linking_enabled() && clfe.cluster_link_active();
}

} // namespace pandaproxy::schema_registry
