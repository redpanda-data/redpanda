// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/client/client.h"

#include "kafka/client/broker.h"
#include "kafka/client/configuration.h"
#include "kafka/client/consumer.h"
#include "kafka/client/exceptions.h"
#include "kafka/client/logger.h"
#include "kafka/client/retry_with_mitigation.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/fetch.h"
#include "kafka/protocol/find_coordinator.h"
#include "kafka/protocol/leave_group.h"
#include "kafka/types.h"
#include "model/fundamental.h"
#include "seastarx.h"
#include "ssx/future-util.h"
#include "utils/unresolved_address.h"

#include <seastar/core/gate.hh>

#include <exception>

namespace kafka::client {

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
              return broker->dispatch(metadata_request{.list_all_topics = true})
                .then([this, broker](metadata_response res) {
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
      .then([this]() {
          return ss::do_for_each(
            _consumers.begin(), _consumers.end(), [](auto c) {
                return c->leave().discard_result();
            });
      })
      .then([this]() { return _brokers.stop(); });
}

ss::future<> client::update_metadata(wait_or_start::tag) {
    return ss::with_gate(_gate, [this]() {
        vlog(kclog.debug, "updating metadata");
        return _brokers.any().then([this](shared_broker_t broker) {
            return broker->dispatch(metadata_request{.list_all_topics = true})
              .then([this](metadata_response res) {
                  // Create new seeds from the returned set of brokers
                  std::vector<unresolved_address> seeds;
                  seeds.reserve(res.brokers.size());
                  for (const auto& b : res.brokers) {
                      seeds.emplace_back(b.host, b.port);
                  }
                  std::swap(_seeds, seeds);

                  return _brokers.apply(std::move(res));
              })
              .finally([]() { vlog(kclog.trace, "updated metadata"); });
        });
    });
}

ss::future<> client::mitigate_error(std::exception_ptr ex) {
    try {
        std::rethrow_exception(ex);
    } catch (const broker_error& ex) {
        // If there are no brokers, reconnect
        if (ex.node_id == unknown_node_id) {
            vlog(kclog.warn, "broker_error: {}", ex);
            return connect();
        } else {
            vlog(kclog.debug, "broker_error: {}", ex);
            return _brokers.erase(ex.node_id).then([this]() {
                return _wait_or_start_update_metadata();
            });
        }
    } catch (const partition_error& ex) {
        switch (ex.error) {
        case error_code::unknown_topic_or_partition:
        case error_code::not_leader_for_partition:
        case error_code::leader_not_available: {
            vlog(kclog.debug, "partition_error: {}", ex);
            return _wait_or_start_update_metadata();
        }
        default:
            // TODO(Ben): Maybe vassert
            vlog(kclog.warn, "partition_error: ", ex);
            return ss::make_exception_future(ex);
        }
    } catch (const ss::gate_closed_exception&) {
        vlog(kclog.debug, "gate_closed_exception");
    } catch (const std::exception_ptr& ex) {
        // TODO(Ben): Probably vassert
        vlog(kclog.error, "unknown exception");
    }
    return ss::make_exception_future(ex);
}

ss::future<produce_response::partition> client::produce_record_batch(
  model::topic_partition tp, model::record_batch&& batch) {
    return ss::with_gate(
      _gate, [this, tp{std::move(tp)}, batch{std::move(batch)}]() mutable {
          vlog(
            kclog.debug,
            "produce record_batch: {}, {{record_count: {}}}",
            tp,
            batch.record_count());
          return _producer.produce(std::move(tp), std::move(batch));
      });
}

ss::future<fetch_response> client::fetch_partition(
  model::topic_partition tp,
  model::offset offset,
  int32_t max_bytes,
  std::chrono::milliseconds timeout) {
    auto build_request =
      [offset, max_bytes, timeout](model::topic_partition& tp) {
          return make_fetch_request(tp, offset, max_bytes, timeout);
      };

    return ss::do_with(
      std::move(build_request),
      std::move(tp),
      [this](auto& build_request, model::topic_partition& tp) {
          vlog(kclog.debug, "fetching: {}", tp);
          return gated_retry_with_mitigation([this, &tp, &build_request]() {
                     return _brokers.find(tp).then(
                       [&tp, &build_request](shared_broker_t&& b) {
                           return b->dispatch(build_request(tp));
                       });
                 })
            .handle_exception([&tp](std::exception_ptr ex) {
                return make_fetch_response(tp, ex);
            });
      });
}

ss::future<member_id> client::create_consumer(const group_id& group_id) {
    auto build_request = [group_id]() {
        return find_coordinator_request(group_id);
    };
    return dispatch(build_request)
      .then([](find_coordinator_response res) {
          return make_broker(
            res.data.node_id, unresolved_address(res.data.host, res.data.port));
      })
      .then([this, group_id](shared_broker_t coordinator) mutable {
          return make_consumer(
            _brokers, std::move(coordinator), std::move(group_id));
      })
      .then([this](shared_consumer_t c) {
          auto m_id = c->member_id();
          _consumers.insert(std::move(c));
          return m_id;
      });
}

ss::future<shared_consumer_t>
client::get_consumer(const group_id& g_id, const member_id& m_id) {
    if (auto c_it = _consumers.find(m_id); c_it != _consumers.end()) {
        return ss::make_ready_future<shared_consumer_t>(*c_it);
    }
    return ss::make_exception_future<shared_consumer_t>(
      consumer_error(g_id, m_id, error_code::unknown_member_id));
}

ss::future<>
client::remove_consumer(const group_id& g_id, const member_id& m_id) {
    return get_consumer(g_id, m_id).then([this](shared_consumer_t c) {
        return c->leave().then([this, c](leave_group_response res) {
            _consumers.erase(c);
            if (res.data.error_code != error_code::none) {
                return ss::make_exception_future<>(consumer_error(
                  c->group_id(), c->member_id(), res.data.error_code));
            }
            return ss::now();
        });
    });
}

ss::future<> client::subscribe_consumer(
  const group_id& g_id,
  const member_id& m_id,
  std::vector<model::topic> topics) {
    return get_consumer(g_id, m_id)
      .then([topics{std::move(topics)}](shared_consumer_t c) mutable {
          return c->subscribe(std::move(topics));
      });
}

ss::future<std::vector<model::topic>>
client::consumer_topics(const group_id& g_id, const member_id& m_id) {
    return get_consumer(g_id, m_id).then([](shared_consumer_t c) {
        return ss::make_ready_future<std::vector<model::topic>>(c->topics());
    });
}

ss::future<assignment>
client::consumer_assignment(const group_id& g_id, const member_id& m_id) {
    return get_consumer(g_id, m_id).then([](shared_consumer_t c) {
        return ss::make_ready_future<assignment>(c->assignment());
    });
}

ss::future<offset_fetch_response> client::consumer_offset_fetch(
  const group_id& g_id,
  const member_id& m_id,
  std::vector<offset_fetch_request_topic> topics) {
    return get_consumer(g_id, m_id)
      .then([topics{std::move(topics)}](shared_consumer_t c) mutable {
          return c->offset_fetch(std::move(topics));
      });
}

ss::future<offset_commit_response> client::consumer_offset_commit(
  const group_id& g_id,
  const member_id& m_id,
  std::vector<offset_commit_request_topic> topics) {
    return get_consumer(g_id, m_id)
      .then([topics{std::move(topics)}](shared_consumer_t c) mutable {
          return c->offset_commit(std::move(topics));
      });
}

} // namespace kafka::client
