// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/client/producer.h"

#include "kafka/client/brokers.h"
#include "kafka/client/configuration.h"
#include "kafka/client/exceptions.h"
#include "kafka/client/logger.h"
#include "kafka/client/retry_with_mitigation.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/produce.h"
#include "model/fundamental.h"

#include <seastar/core/gate.hh>

#include <exception>

namespace kafka::client {

produce_request
make_produce_request(model::topic_partition tp, model::record_batch&& batch) {
    std::vector<produce_request::partition> partitions;
    partitions.emplace_back(produce_request::partition{
      .partition_index{tp.partition},
      .records = produce_request_record_data(std::move(batch))});

    std::vector<produce_request::topic> topics;
    topics.emplace_back(produce_request::topic{
      .name{std::move(tp.topic)}, .partitions{std::move(partitions)}});
    std::optional<ss::sstring> t_id;
    int16_t acks = -1;
    return produce_request(t_id, acks, std::move(topics));
}

produce_response::partition
make_produce_response(model::partition_id p_id, std::exception_ptr ex) {
    auto response = produce_response::partition{
      .partition_index{p_id},
      .error_code = error_code::none,
    };
    try {
        std::rethrow_exception(std::move(ex));
    } catch (const partition_error& ex) {
        vlog(kclog.debug, "handling partition_error {}", ex.what());
        response.error_code = ex.error;
    } catch (const broker_error& ex) {
        vlog(kclog.debug, "handling broker_error {}", ex.what());
        response.error_code = ex.error;
    } catch (const ss::gate_closed_exception&) {
        vlog(kclog.debug, "gate_closed_exception");
        response.error_code = error_code::operation_not_attempted;
    } catch (const std::exception& ex) {
        vlog(kclog.warn, "std::exception {}", ex.what());
        response.error_code = error_code::unknown_server_error;
    }
    return response;
}

ss::future<produce_response::partition>
producer::produce(model::topic_partition tp, model::record_batch&& batch) {
    return get_context(std::move(tp))->produce(std::move(batch));
}

ss::future<produce_response::partition>
producer::do_send(model::topic_partition tp, model::record_batch&& batch) {
    return _topic_cache.leader(tp)
      .then([this](model::node_id leader) { return _brokers.find(leader); })
      .then([tp{std::move(tp)},
             batch{std::move(batch)}](shared_broker_t broker) mutable {
          return broker->dispatch(
            make_produce_request(std::move(tp), std::move(batch)));
      })
      .then([](produce_response res) mutable {
          auto topic = std::move(res.data.responses[0]);
          auto partition = std::move(topic.partitions[0]);
          if (partition.error_code != error_code::none) {
              return ss::make_exception_future<produce_response::partition>(
                partition_error(
                  model::topic_partition(topic.name, partition.partition_index),
                  partition.error_code));
          }
          return ss::make_ready_future<produce_response::partition>(
            std::move(partition));
      });
}

ss::future<>
producer::send(model::topic_partition tp, model::record_batch&& batch) {
    auto record_count = batch.record_count();
    vlog(
      kclog.debug,
      "send record_batch: {}, {{record_count: {}}}",
      tp,
      record_count);
    auto p_id = tp.partition;
    return ss::do_with(
             std::move(batch),
             [this, tp](model::record_batch& batch) mutable {
                 return retry_with_mitigation(
                   _config.retries(),
                   _config.retry_base_backoff(),
                   [this, tp{std::move(tp)}, &batch]() {
                       return do_send(tp, batch.share());
                   },
                   [this](std::exception_ptr ex) {
                       return _error_handler(std::move(ex))
                         .handle_exception([](std::exception_ptr ex) {
                             vlog(
                               kclog.trace, "Error during mitigation: {}", ex);
                             // ignore failed mitigation
                         });
                   });
             })
      .handle_exception([p_id](std::exception_ptr ex) {
          return make_produce_response(p_id, std::move(ex));
      })
      .then([this, tp, record_count](produce_response::partition res) mutable {
          vlog(
            kclog.debug,
            "sent record_batch: {}, {{record_count: {}}}, {}",
            tp,
            record_count,
            res.error_code);
          get_context(std::move(tp))->handle_response(std::move(res));
      });
}

} // namespace kafka::client
