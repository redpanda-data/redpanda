// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/client/manual_consumer.h"

#include "kafka/client/broker.h"
#include "kafka/client/configuration.h"
#include "kafka/client/consumer.h"
#include "kafka/client/exceptions.h"
#include "kafka/client/logger.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/fetch.h"
#include "kafka/protocol/metadata.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "ssx/future-util.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/sleep.hh>

#include <algorithm>
#include <chrono>
#include <exception>
#include <iterator>
#include <utility>

namespace kafka::client {

using namespace std::chrono_literals;

manual_consumer::manual_consumer(
  const configuration& config,
  topic_cache& topic_cache,
  brokers& brokers,
  cursors cursors,
  ss::noncopyable_function<ss::future<>(std::exception_ptr)> mitigater)
  : _config(config)
  , _topic_cache(topic_cache)
  , _brokers(brokers)
  , _cursors(std::move(cursors))
  , _external_mitigate(std::move(mitigater)) {}

ss::future<> manual_consumer::stop() {
    vlog(kclog.debug, "Manual consumer: stop");

    if (_as.abort_requested()) {
        co_return;
    }
    _as.request_abort();
    co_await _gate.close();
}

void manual_consumer::upsert_partition(
  model::topic_partition tp, model::offset offset) {
    _cursors[tp.topic][tp.partition].offset = offset;
}

void manual_consumer::remove_partition(model::topic_partition tp) {
    auto it = _cursors.find(tp.topic);
    vassert(it != _cursors.end(), "Topic partition to unassign doesn't exist");
    it->second.erase(tp.partition);
    if (it->second.empty()) {
        _cursors.erase(it);
    }
}

ss::future<fetch_response>
manual_consumer::dispatch_fetch(shared_broker_t broker, fetch_request req) {
    vlog(kclog.warn, "Manual consumer -> {}: fetch_req: {}", broker->id(), req);
    auto res = co_await broker->dispatch(std::move(req));
    vlog(kclog.warn, "Manual consumer -> {}: fetch_res: {}", broker->id(), res);

    _fetch_sessions[broker].apply(res);

    co_return res;
}

ss::future<> manual_consumer::await_mitigate() {
    if (_mitigation_promise) {
        return _mitigation_promise->get_shared_future();
    }
    return ss::now();
}

void manual_consumer::start_mitigate(std::exception_ptr eptr) {
    if (!_mitigation_promise) {
        vlog(kclog.debug, "Starting to mitigate: {}", eptr);
        _mitigation_promise.emplace();

        ssx::spawn_with_gate(_gate, [this, eptr]() {
            return ss::futurize_invoke(
                     [this, eptr] { return _external_mitigate(eptr); })
              .then_wrapped([this](auto f) {
                  try {
                      f.get();
                  } catch (...) {
                      std::cout << "mitigation: failed\n";
                  }
                  _mitigation_promise->set_value();
                  _mitigation_promise.reset();
              });
        });
    }
}

ss::future<fetch_response> manual_consumer::fetch() {
    co_await await_mitigate();
    co_return co_await ss::with_gate(_gate, [this]() { return do_fetch(); });
}

ss::future<fetch_response> manual_consumer::do_fetch() {
    // Split requests by broker
    broker_reqs_t broker_reqs;

    auto make_fetch_req = [&](fetch_session& session) {
        return fetch_request{
          .data = {
            .replica_id = consumer_replica_id,
            .max_wait_ms = _config.consumer_request_timeout,
            .min_bytes = _config.consumer_request_min_bytes,
            .max_bytes = _config.consumer_request_max_bytes,
            .isolation_level = model::isolation_level::read_uncommitted,
            .session_id = session.id(),
            .session_epoch = session.epoch(),
          }};
    };

    std::optional<std::exception_ptr> first_err;

    for (const auto& [t, ps] : _cursors) {
        for (const auto& [p, c] : ps) {
            try {
                auto tp = model::topic_partition{t, p};
                auto leader = co_await _topic_cache.leader(tp);
                auto broker = co_await _brokers.find(leader);
                auto& session = _fetch_sessions[broker];

                auto& req = broker_reqs
                              .try_emplace(broker, make_fetch_req(session))
                              .first->second;

                session.fill_fetch_add_partition(req, tp, c.offset);
            } catch (...) {
                if (!first_err) {
                    first_err = std::current_exception();
                }
            }
        }
    }

    for (auto& [broker, req] : broker_reqs) {
        _fetch_sessions[broker].fill_fetch_complete(req);
    }

    auto res = co_await ss::map_reduce(
      std::make_move_iterator(broker_reqs.begin()),
      std::make_move_iterator(broker_reqs.end()),
      [this, &first_err](broker_reqs_t::value_type br) {
          return dispatch_fetch(br.first, std::move(br.second))
            .then([&first_err, broker = br.first](fetch_response res) {
                // Check the error codes inside the response
                if (first_err) {
                    return res;
                }

                if (res.data.error_code != error_code::none) {
                    first_err = std::make_exception_ptr(
                      broker_error(broker->id(), res.data.error_code));
                    return res;
                }

                for (const auto& topic : res.data.responses) {
                    for (const auto& part : topic.partitions) {
                        if (part.error_code != error_code::none) {
                            first_err = std::make_exception_ptr(partition_error(
                              model::topic_partition{
                                topic.topic, part.partition_index},
                              part.error_code));
                            return res;
                        }
                    }
                }
                return res;
            })
            .handle_exception([&first_err](std::exception_ptr eptr) {
                if (!first_err) {
                    first_err = eptr;
                }
                // Surface an error even if this is a broker/connection error so
                // that the caller is aware that this is aware that this is not
                // a successful response
                return ss::make_ready_future<fetch_response>(fetch_response{
                  .data{.error_code = error_code::unknown_server_error}});
            });
      },
      fetch_response{
        .data
        = {.throttle_time_ms{}, .error_code = error_code::none, .session_id = kafka::invalid_fetch_session_id}},
      detail::reduce_fetch_response);

    vlog(kclog.debug, "First error during fetch: {}", first_err);
    if (first_err) {
        start_mitigate(*first_err);
    }

    co_return std::move(res);
}

} // namespace kafka::client
