// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/client/producer.h"

#include "container/fragmented_vector.h"
#include "kafka/client/brokers.h"
#include "kafka/client/configuration.h"
#include "kafka/client/exceptions.h"
#include "kafka/client/logger.h"
#include "kafka/client/utils.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/produce.h"
#include "model/fundamental.h"

#include <seastar/core/gate.hh>

#include <chrono>
#include <exception>

using namespace std::chrono_literals;

namespace kafka::client {

produce_request make_produce_request(
  model::topic_partition tp, model::record_batch&& batch, int16_t acks) {
    chunked_vector<produce_request::partition> partitions;
    partitions.emplace_back(produce_request::partition{
      .partition_index{tp.partition},
      .records = produce_request_record_data(std::move(batch))});

    chunked_vector<produce_request::topic> topics;
    topics.emplace_back(produce_request::topic{
      .name{std::move(tp.topic)}, .partitions{std::move(partitions)}});
    std::optional<ss::sstring> t_id;
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
    } catch (const ss::abort_requested_exception&) {
        /// Could only occur when abort_source is triggered via stop()
        vlog(kclog.debug, "sleep_aborted / abort_requested exception");
        response.error_code = error_code::operation_not_attempted;
    } catch (const std::exception& ex) {
        vlog(kclog.warn, "std::exception {}", ex.what());
        response.error_code = error_code::unknown_server_error;
    }
    return response;
}

ss::future<> producer::stop() {
    vlog(kclog.debug, "Stopping kafka/client producer");
    /// Stop new messages from entering the system, the second abort source is
    /// triggered when the timeout below expires
    _ingest_as.request_abort();

    /// produce_partition::drain() will invoke send(), it is a last chance best
    /// effort attempt for the current queued records to be sent.
    co_await ssx::parallel_transform(
      _partitions,
      [](partitions_t::value_type p) { return p.second->maybe_drain(); });

    /// send() is wrapped by a gate, and can be aborted with the internal
    /// abort source (_as). This future triggers the abort source after the
    /// configured interval or when the gate is eventually closed whichever
    /// comes first.
    ss::abort_source exit;
    if (_config.produce_shutdown_delay() > 0ms) {
        vlog(
          kclog.debug,
          "Waiting {}ms to allow final flush of producers batched records",
          _config.produce_shutdown_delay());
    }
    auto abort = ss::sleep_abortable(_config.produce_shutdown_delay(), exit)
                   .then([this] {
                       if (_config.produce_shutdown_delay() > 0ms) {
                           vlog(
                             kclog.warn,
                             "Forcefully stopping kafka client producer after "
                             "waiting {}ms for its gate to close",
                             _config.produce_shutdown_delay());
                       }
                       _as.request_abort();
                   })
                   .handle_exception_type([](ss::sleep_aborted) {
                       vlog(kclog.debug, "Producer shutdown cleanly");
                   });
    co_await _gate.close();
    exit.request_abort();
    co_await std::move(abort);
    vlog(kclog.debug, "Waiting for inflight state of false");
    /// Wait until the produce_partition has no inflight records. That is
    /// because if in_flight is true, drain() and stop() will actually not call
    /// consume -> send(). This may have been the case when maybe_drain() above
    /// was called.
    co_await ssx::parallel_transform(
      _partitions,
      [](partitions_t::value_type p) { return p.second->await_in_flight(); });
    vlog(kclog.debug, "Calling produce_partition::stop()");
    /// At this point in time there are no inflight requests, for any data that
    /// remains in the buffers stop() will be guaranteed to call send() which
    /// will return error responses to the initial caller
    co_await ssx::parallel_transform(
      _partitions, [](partitions_t::value_type p) { return p.second->stop(); });
    vlog(kclog.debug, "Producer stopped");
}

ss::future<produce_response::partition>
producer::produce(model::topic_partition tp, model::record_batch&& batch) {
    if (_ingest_as.abort_requested()) {
        return ss::make_ready_future<produce_response::partition>(
          make_produce_response(
            tp.partition,
            std::make_exception_ptr(ss::abort_requested_exception())));
    }
    return get_context(std::move(tp))->produce(std::move(batch));
}

ss::future<produce_response::partition>
producer::do_send(model::topic_partition tp, model::record_batch batch) {
    auto leader = co_await _topic_cache.leader(tp);
    auto broker = co_await _brokers.find(leader);
    auto res = co_await broker->dispatch(
      make_produce_request(std::move(tp), std::move(batch), _acks));
    auto topic = std::move(res.data.responses[0]);
    auto partition = std::move(topic.partitions[0]);
    if (partition.error_code != error_code::none) {
        throw partition_error(
          model::topic_partition(topic.name, partition.partition_index),
          partition.error_code);
    }

    co_return partition;
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
                 return ss::with_gate(_gate, [this, tp, &batch]() {
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
                                   kclog.trace,
                                   "Error during mitigation: {}",
                                   ex);
                                 // ignore failed mitigation
                             });
                       },
                       _as);
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
