#include "pandaproxy/client/producer.h"

#include "kafka/errors.h"
#include "kafka/requests/produce_request.h"
#include "model/fundamental.h"
#include "pandaproxy/client/brokers.h"
#include "pandaproxy/client/configuration.h"
#include "pandaproxy/client/error.h"
#include "pandaproxy/client/logger.h"
#include "pandaproxy/client/retry_with_mitigation.h"

#include <seastar/core/gate.hh>

#include <exception>

namespace pandaproxy::client {

kafka::produce_request
make_produce_request(model::topic_partition tp, model::record_batch&& batch) {
    std::vector<kafka::produce_request::partition> partitions;
    partitions.emplace_back(kafka::produce_request::partition{
      .id{tp.partition},
      .data{},
      .adapter = kafka::kafka_batch_adapter{
        .v2_format = true, .valid_crc = true, .batch{std::move(batch)}}});

    std::vector<kafka::produce_request::topic> topics;
    topics.emplace_back(kafka::produce_request::topic{
      .name{std::move(tp.topic)}, .partitions{std::move(partitions)}});
    std::optional<ss::sstring> t_id;
    int16_t acks = -1;
    return kafka::produce_request(t_id, acks, std::move(topics));
}

kafka::produce_response::partition
make_produce_response(model::partition_id p_id, std::exception_ptr ex) {
    auto response = kafka::produce_response::partition{
      .id{p_id},
      .error = kafka::error_code::none,
    };
    try {
        std::rethrow_exception(std::move(ex));
    } catch (const partition_error& ex) {
        vlog(ppclog.debug, "handling partition_error {}", ex.what());
        response.error = ex.error;
    } catch (const broker_error& ex) {
        vlog(ppclog.debug, "handling broker_error {}", ex.what());
        response.error = ex.error;
    } catch (const ss::gate_closed_exception&) {
        vlog(ppclog.debug, "gate_closed_exception");
        response.error = kafka::error_code::operation_not_attempted;
    } catch (const std::exception& ex) {
        vlog(ppclog.warn, "std::exception {}", ex.what());
        response.error = kafka::error_code::unknown_server_error;
    } catch (const std::exception_ptr&) {
        vlog(ppclog.error, "std::exception_ptr");
        response.error = kafka::error_code::unknown_server_error;
    }
    return response;
}

ss::future<kafka::produce_response::partition>
producer::produce(model::topic_partition tp, model::record_batch&& batch) {
    return get_context(std::move(tp))->produce(std::move(batch));
}

ss::future<kafka::produce_response::partition>
producer::do_send(model::topic_partition tp, model::record_batch&& batch) {
    return _brokers.find(tp)
      .then([tp{std::move(tp)},
             batch{std::move(batch)}](shared_broker_t broker) mutable {
          return broker->dispatch(
            make_produce_request(std::move(tp), std::move(batch)));
      })
      .then([](kafka::produce_response res) mutable {
          auto topic = std::move(res.topics[0]);
          auto partition = std::move(topic.partitions[0]);
          if (partition.error != kafka::error_code::none) {
              return ss::make_exception_future<
                kafka::produce_response::partition>(partition_error(
                model::topic_partition(topic.name, partition.id),
                partition.error));
          }
          return ss::make_ready_future<kafka::produce_response::partition>(
            std::move(partition));
      });
}

ss::future<>
producer::send(model::topic_partition tp, model::record_batch&& batch) {
    auto record_count = batch.record_count();
    vlog(
      ppclog.debug,
      "send record_batch: {}, {{record_count: {}}}",
      tp,
      record_count);
    auto p_id = tp.partition;
    return ss::do_with(
             std::move(batch),
             [this, tp](model::record_batch& batch) mutable {
                 return retry_with_mitigation(
                   shard_local_cfg().retries(),
                   shard_local_cfg().retry_base_backoff(),
                   [this, tp{std::move(tp)}, &batch]() {
                       return do_send(tp, batch.share());
                   },
                   [this](std::exception_ptr ex) {
                       return _error_handler(std::move(ex))
                         .handle_exception([](std::exception_ptr ex) {
                             vlog(
                               ppclog.trace, "Error during mitigation: {}", ex);
                             // ignore failed mitigation
                         });
                   });
             })
      .handle_exception([p_id](std::exception_ptr ex) {
          return make_produce_response(p_id, std::move(ex));
      })
      .then([this, tp, record_count](
              kafka::produce_response::partition res) mutable {
          vlog(
            ppclog.debug,
            "sent record_batch: {}, {{record_count: {}}}, {}",
            tp,
            record_count,
            res.error);
          get_context(std::move(tp))->handle_response(std::move(res));
      });
}

} // namespace pandaproxy::client
