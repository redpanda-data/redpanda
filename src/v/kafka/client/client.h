/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "absl/container/flat_hash_set.h"
#include "absl/container/node_hash_map.h"
#include "container/chunked_vector.h"
#include "kafka/client/assignment_plans.h"
#include "kafka/client/cluster.h"
#include "kafka/client/configuration.h"
#include "kafka/client/consumer.h"
#include "kafka/client/partitioners.h"
#include "kafka/client/producer.h"
#include "kafka/client/types.h"
#include "kafka/client/utils.h"
#include "kafka/protocol/create_topics.h"
#include "kafka/protocol/describe_configs.h"
#include "kafka/protocol/fetch.h"
#include "kafka/protocol/list_offset.h"
#include "kafka/protocol/metadata.h"
#include "ssx/semaphore.h"
#include "utils/prefix_logger.h"
#include "utils/unresolved_address.h"

#include <seastar/core/condition-variable.hh>

namespace kafka::client {

/// \brief wait or start a function
///
/// Start the function and wait for it to finish, or, if an instance of the
/// function is already running, wait for that one to finish.
class wait_or_start {
public:
    // Prevent accidentally calling the protected func.
    struct tag {};
    using func = ss::noncopyable_function<ss::future<>(tag)>;

    explicit wait_or_start(func func)
      : _func{std::move(func)} {}

    ss::future<> operator()() {
        if (_lock.try_wait()) {
            return _func(tag{}).finally(
              [this]() { _lock.signal(_lock.waiters() + 1); });
        }
        return _lock.wait();
    }

private:
    func _func;
    ssx::semaphore _lock{1, "k/client"};
};

class client {
public:
    using external_mitigate
      = ss::noncopyable_function<ss::future<>(std::exception_ptr)>;
    explicit client(
      const YAML::Node& cfg,
      std::optional<external_mitigate> mitigater = std::nullopt);

    explicit client(
      const client_configuration& config, std::optional<external_mitigate>);

    /// \brief Connect to all brokers.
    ss::future<> connect();
    /// \brief Disconnect from all brokers.
    ss::future<> stop() noexcept;

    /// \brief Invoke func, on failure, mitigate error and retry.
    template<typename Func>
    std::invoke_result_t<Func> gated_retry_with_mitigation(Func func) {
        return gated_retry_with_mitigation_impl(
          _gate,
          _retries_config.max_retries,
          _retries_config.retry_base_backoff,
          std::move(func),
          [this](std::exception_ptr ex) { return mitigate_error(ex); },
          _as);
    }

    /// \brief Dispatch a request to any broker.
    template<typename Func>
    requires requires {
        typename std::invoke_result_t<Func>::api_type::response_type;
    }
    ss::future<typename std::invoke_result_t<Func>::api_type::response_type>
    dispatch(Func func) {
        using api_type = std::invoke_result_t<Func>::api_type;
        return gated_retry_with_mitigation([this, func{std::move(func)}]() {
            return _cluster->dispatch_to_any(
              func(), api_version_for(api_type::key));
        });
    }

    /// \brief Dispatch a request to a specific broker.
    /// \param req The metadata request to send
    ss::future<metadata_response> fetch_metadata(metadata_request req);

    using validate_only_t = ss::bool_class<struct validate_only_tag>;
    ss::future<create_topics_response> create_topic(
      kafka::creatable_topic req,
      validate_only_t validate_only = validate_only_t::no);

    ss::future<produce_response::partition> produce_record_batch(
      model::topic_partition tp, model::record_batch&& batch);

    ss::future<produce_response>
    produce_records(model::topic topic, chunked_vector<record_essence> batch);

    ss::future<list_offsets_response> list_offsets(list_offsets_request req);

    ss::future<list_offsets_response> list_offsets(model::topic_partition tp);

    ss::future<fetch_response> fetch_partition(
      model::topic_partition tp,
      model::offset offset,
      std::chrono::milliseconds timeout,
      std::optional<int32_t> max_bytes = std::nullopt);

    ss::future<member_id>
    create_consumer(const group_id& g_id, member_id name = kafka::no_member);

    ss::future<shared_consumer_t>
    get_consumer(const group_id& g_id, const member_id& m_id);

    ss::future<> remove_consumer(group_id g_id, const member_id& m_id);

    ss::future<> subscribe_consumer(
      const group_id& group_id,
      const member_id& member_id,
      chunked_vector<model::topic> topics);

    ss::future<chunked_vector<model::topic>>
    consumer_topics(const group_id& g_id, const member_id& m_id);

    ss::future<assignment>
    consumer_assignment(const group_id& g_id, const member_id& m_id);

    ss::future<offset_fetch_response> consumer_offset_fetch(
      const group_id& g_id,
      const member_id& m_id,
      chunked_vector<offset_fetch_request_topic> topics);

    ss::future<offset_commit_response> consumer_offset_commit(
      const group_id& g_id,
      const member_id& m_id,
      chunked_vector<offset_commit_request_topic> topics);

    ss::future<fetch_response> consumer_fetch(
      const group_id& g_id,
      const member_id& m_id,
      std::optional<std::chrono::milliseconds> timeout,
      std::optional<int32_t> max_bytes);

    ss::future<describe_configs_response> describe_topics(
      chunked_vector<model::topic> topics,
      std::optional<chunked_vector<ss::sstring>> configuration_keys
      = std::nullopt);

    ss::future<describe_configs_response> describe_topics(
      model::topic topic,
      std::optional<chunked_vector<ss::sstring>> configuration_keys
      = std::nullopt) {
        return describe_topics(
          chunked_vector<model::topic>{std::move(topic)},
          std::move(configuration_keys));
    }

    ss::future<> update_metadata();

    bool is_connected() const { return !_cluster->is_connected(); }

    void set_credentials(std::optional<sasl_configuration> creds);

    void set_max_retries(size_t max_retries) {
        _retries_config.max_retries = max_retries;
    }

    void set_retry_base_backoff(std::chrono::milliseconds retry_base_backoff) {
        _retries_config.retry_base_backoff = retry_base_backoff;
    }

    void set_batch_record_count(int32_t count) {
        _producer.set_batch_record_count(count);
    }

    void set_batch_size_bytes(int32_t size) {
        _producer.set_batch_size_bytes(size);
    }

    void set_batch_delay(std::chrono::milliseconds delay) {
        _producer.set_batch_delay(delay);
    }

    const std::optional<sasl_configuration>& get_credentials() const {
        return _cluster->get_sasl_configuration();
    }

private:
    friend class client_fetcher;
    ss::future<list_offsets_response>
    do_list_offsets(const list_offsets_request&);

    ss::future<describe_configs_response> do_describe_topics(
      chunked_vector<model::topic> topics,
      std::optional<chunked_vector<ss::sstring>> configuration_keys);

    void on_metadata_update(const metadata_update& res);

    /// \brief Connect and update metdata.
    ss::future<> do_connect(net::unresolved_address addr);

    /// \brief Update metadata
    ///
    /// If an existing update is in progress, the future returned will be
    /// satisfied by the outstanding request.
    ///
    /// Uses round-robin load-balancing strategy.
    ss::future<> update_metadata(wait_or_start::tag);

    /// \brief Handle errors by performing an action that may fix the cause of
    /// the error
    ss::future<> mitigate_error(std::exception_ptr ex);

    /// \brief Handle errors by performing the optionally-configurable external
    /// mitigation action that may fix the cause of the error
    ss::future<> external_mitigate_error(std::exception_ptr ex) const;

    /// \brief Apply metadata update
    ss::future<> apply(metadata_response res);

    prefix_logger& logger() { return _logger; }
    /// \brief Client holds a copy of its configuration
    retries_configuration _retries_config;
    producer_configuration _producer_config;
    consumer_configuration _consumer_config;
    prefix_logger _logger;
    std::optional<external_mitigate> _external_mitigate;
    std::unique_ptr<cluster> _cluster;
    /// \brief Batching producer.
    producer _producer;
    /// \brief Consumers
    absl::node_hash_map<
      kafka::group_id,
      absl::flat_hash_set<
        shared_consumer_t,
        detail::consumer_hash,
        detail::consumer_eq>>
      _consumers;

    partitioners_cache _partitioners;
    cluster::callback_id _metadata_callback_id;
    /// \brief Wait for retries.
    ss::gate _gate;

    bool _is_started{false};
    bool _is_stopped{false};
    // we must keep track of the retry count to avoid infinite retries to adhere
    // to previous behavior where the client would gave up after a certain
    // number of retries.
    size_t _current_reconnect_retry{0};
    ss::abort_source _as;
};

} // namespace kafka::client
