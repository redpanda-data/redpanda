// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/client/client.h"

#include "kafka/errors.h"
#include "kafka/requests/fetch_request.h"
#include "model/fundamental.h"
#include "pandaproxy/client/broker.h"
#include "pandaproxy/client/configuration.h"
#include "pandaproxy/client/error.h"
#include "pandaproxy/client/logger.h"
#include "pandaproxy/client/retry_with_mitigation.h"
#include "seastarx.h"
#include "ssx/future-util.h"
#include "utils/unresolved_address.h"

#include <seastar/core/gate.hh>

#include <exception>

namespace pandaproxy::client {

client::client(std::vector<unresolved_address> broker_addrs)
  : _seeds{std::move(broker_addrs)}
  , _brokers{}
  , _wait_or_start_update_metadata{[this](wait_or_start::tag tag) {
      return update_metadata(tag);
  }}
  , _producer{_brokers, [this](std::exception_ptr ex) {
                  return mitigate_error(std::move(ex));
              }} {}

ss::future<> client::do_connect(unresolved_address addr) {
    return ss::with_gate(_gate, [this, addr]() {
        return make_broker(unknown_node_id, addr)
          .then([this](shared_broker_t broker) {
              return broker
                ->dispatch(kafka::metadata_request{.list_all_topics = true})
                .then([this, broker](kafka::metadata_response res) {
                    return _brokers.apply(std::move(res));
                });
          });
    });
}

ss::future<> client::connect() {
    return ss::do_with(size_t{0}, [this](size_t& retries) {
        return retry_with_mitigation(
          shard_local_cfg().retries(),
          shard_local_cfg().retry_base_backoff(),
          [this, retries]() {
              return do_connect(_seeds[retries % _seeds.size()]);
          },
          [&retries](std::exception_ptr) {
              ++retries;
              return ss::now();
          });
    });
}

ss::future<> client::stop() {
    return _gate.close()
      .then([this]() { return _producer.stop(); })
      .then([this]() { return _brokers.stop(); });
}

ss::future<> client::update_metadata(wait_or_start::tag) {
    vlog(ppclog.debug, "updating metadata");
    return _brokers.any().then([this](shared_broker_t broker) {
        return broker
          ->dispatch(kafka::metadata_request{.list_all_topics = true})
          .then([this](kafka::metadata_response res) {
              // Create new seeds from the returned set of brokers
              std::vector<unresolved_address> seeds;
              seeds.reserve(res.brokers.size());
              for (const auto& b : res.brokers) {
                  seeds.emplace_back(b.host, b.port);
              }
              std::swap(_seeds, seeds);

              return _brokers.apply(std::move(res));
          })
          .finally([]() { vlog(ppclog.trace, "updated metadata"); });
    });
}

ss::future<> client::mitigate_error(std::exception_ptr ex) {
    try {
        std::rethrow_exception(ex);
    } catch (const broker_error& ex) {
        // If there are no brokers, reconnect
        if (ex.node_id == unknown_node_id) {
            vlog(ppclog.warn, "broker_error: {}", ex);
            return connect();
        } else {
            vlog(ppclog.debug, "broker_error: {}", ex);
            return _brokers.erase(ex.node_id).then([this]() {
                return _wait_or_start_update_metadata();
            });
        }
    } catch (const partition_error& ex) {
        switch (ex.error) {
        case kafka::error_code::unknown_topic_or_partition:
        case kafka::error_code::not_leader_for_partition:
        case kafka::error_code::leader_not_available: {
            vlog(ppclog.debug, "partition_error: {}", ex);
            return _wait_or_start_update_metadata();
        }
        default:
            // TODO(Ben): Maybe vassert
            vlog(ppclog.warn, "partition_error: ", ex);
            return ss::make_exception_future(ex);
        }
    } catch (const ss::gate_closed_exception&) {
        vlog(ppclog.debug, "gate_closed_exception");
    } catch (const std::exception_ptr& ex) {
        // TODO(Ben): Probably vassert
        vlog(ppclog.error, "unknown exception");
    }
    return ss::make_exception_future(ex);
}

ss::future<kafka::produce_response::partition> client::produce_record_batch(
  model::topic_partition tp, model::record_batch&& batch) {
    vlog(
      ppclog.debug,
      "produce record_batch: {}, {{record_count: {}}}",
      tp,
      batch.record_count());
    return _producer.produce(std::move(tp), std::move(batch));
}

ss::future<kafka::fetch_response::partition> client::fetch_partition(
  model::topic_partition tp,
  model::offset offset,
  int32_t max_bytes,
  std::chrono::milliseconds timeout) {
    using namespace std::chrono_literals;
    auto build_request =
      [offset, max_bytes, timeout](model::topic_partition& tp) {
          return make_fetch_request(tp, offset, max_bytes, timeout);
      };

    return ss::do_with(
      std::move(build_request),
      std::move(tp),
      [this](auto& build_request, model::topic_partition& tp) {
          return retry_with_mitigation(
                   shard_local_cfg().retries(),
                   shard_local_cfg().retry_base_backoff(),
                   [this, &tp, &build_request]() {
                       return _brokers.find(tp)
                         .then([&tp, &build_request](shared_broker_t&& b) {
                             return b->dispatch(build_request(tp));
                         })
                         .then([](kafka::fetch_response res) {
                             return std::move(res.partitions[0]);
                         });
                   },
                   [this](std::exception_ptr ex) { return mitigate_error(ex); })
            .handle_exception([&tp](std::exception_ptr ex) {
                return make_fetch_response(tp, ex);
            });
      });
}

} // namespace pandaproxy::client
