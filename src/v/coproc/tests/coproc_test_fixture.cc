// Copyright 2020 Vectorized, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md

#include "coproc_test_fixture.h"

#include "coproc/errc.h"
#include "coproc/logger.h"
#include "coproc/tests/utils.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "model/timestamp.h"
#include "vassert.h"
#include "vlog.h"

#include <seastar/core/circular_buffer.hh>

#include <chrono>

using namespace std::literals;

void coproc_test_fixture::startup(log_layout_map&& data, active_copros&& cps) {
    _data = to_ntps(std::move(data));
    _api.start(default_kvstorecfg(), default_logcfg()).get0();
    _api.invoke_on_all(&storage::api::start).get();
    _router
      .start(
        ss::socket_address(ss::net::inet_address("127.0.0.1"), 43189),
        std::ref(_api))
      .get();
    expand().get();
    expand_copros(std::move(cps)).get();
    if (_start_router) {
        // All tests must involve a router or to even compile. A coproc::service
        // takes a router as its main argument. However in some cases its not
        // desired that the router start, as it will create side effects or
        // even throw due to it attempting to connect to a processor engine on
        // call to start
        _router.invoke_on_all(&coproc::router::start).get();
    }
}

coproc_test_fixture::~coproc_test_fixture() {
    _router.stop().get();
    _api.stop().get();
    _data.clear();
}

// Keep calling 'fn' until function returns true
// OR if 'timeout' was specified, exit when timeout is reached
template<typename Func>
ss::future<> poll_until(
  Func&& fn, model::timeout_clock::time_point timeout = model::no_timeout) {
    return ss::do_with(
      model::timeout_clock::now(),
      std::forward<Func>(fn),
      false,
      [timeout](auto& now, auto& fn, auto& exit) {
          return ss::do_until(
                   [&exit, &now, timeout] { return exit || (now > timeout); },
                   [&exit, &now, &fn] {
                       return fn().then([&exit, &now](bool r) {
                           exit = r;
                           now = model::timeout_clock::now();
                       });
                   })
            .then([]() { return ss::sleep(200ms); });
      });
}

ss::future<coproc_test_fixture::opt_reader_data_t> coproc_test_fixture::drain(
  const model::ntp& ntp,
  std::size_t limit,
  model::timeout_clock::time_point timeout) {
    if (!model::is_materialized_topic(ntp.tp.topic)) {
        return ss::make_ready_future<opt_reader_data_t>(std::nullopt);
    }
    const auto c = hash_scheme(ntp.tp.topic);
    vlog(
      coproc::coproclog.info,
      "searching for ntp {} on shard id {} ...with value for limit: {}",
      ntp,
      c,
      limit);
    return _api.invoke_on(c, [this, ntp, limit, timeout](storage::api& api) {
        return poll_until(
                 [&api, ntp]() {
                     return ss::make_ready_future<bool>(
                       api.log_mgr().get(ntp).has_value());
                 },
                 model::timeout_clock::now() + 2s)
          .then([this, &api, ntp, limit, timeout] {
              auto log = api.log_mgr().get(ntp);
              if (!log.has_value()) {
                  return ss::make_ready_future<opt_reader_data_t>(std::nullopt);
              }
              return do_drain(std::move(*log), limit, timeout)
                .then([ntp](auto rval) mutable {
                    return opt_reader_data_t(std::move(rval));
                });
          });
    });
}

ss::future<model::record_batch_reader::data_t> coproc_test_fixture::do_drain(
  storage::log&& log,
  std::size_t limit,
  model::timeout_clock::time_point timeout) {
    return ss::do_with(
      timeout,
      std::move(log),
      static_cast<std::size_t>(0),
      model::offset(0),
      model::record_batch_reader::data_t(),
      [limit](auto& timeout, auto& log, auto& acc, auto& offset, auto& b) {
          // Loop until the expected number of records is read
          // OR a non-configurable timeout has been reached
          return poll_until(
                   [&b, &log, &acc, &offset, &timeout, limit] {
                       return log.make_reader(log_rdr_cfg(model::offset(acc)))
                         .then([&timeout](model::record_batch_reader reader) {
                             return model::consume_reader_to_memory(
                               std::move(reader), timeout);
                         })
                         .then([&acc, &b, &offset](
                                 model::record_batch_reader::data_t data) {
                             offset += data.size();
                             for (auto&& r : data) {
                                 acc += r.record_count();
                                 b.push_back(std::move(r));
                             }
                         })
                         .then([&acc, limit] { return acc >= limit; });
                   },
                   timeout)
            .then([&b, &acc] { return std::move(b); });
      });
}

ss::future<> coproc_test_fixture::push(
  const model::ntp& ntp, ss::circular_buffer<model::record_batch>&& data) {
    vlog(
      coproc::coproclog.info,
      "Pushing {} record batches to ntp: {}",
      data.size(),
      ntp);
    return _api.invoke_on(
      hash_scheme(ntp.tp.topic),
      [data = std::move(data), ntp](storage::api& api) mutable {
          auto log = api.log_mgr().get(ntp);
          // TODO(rob): Future enhancement, push can make a log if one doesn't
          // exist, for now no test needs or desires this
          vassert(log.has_value(), "Log doesn't exist when it is expected to");
          auto rbr = model::make_memory_record_batch_reader(std::move(data));
          return std::move(rbr)
            .for_each_ref(log->make_appender(log_app_cfg()), model::no_timeout)
            .then([log](auto) mutable { return log->flush(); });
      });
}

ss::future<> coproc_test_fixture::expand_copros(active_copros&& copros) {
    return ss::do_with(std::move(copros), [this](auto& copros) {
        return _router.invoke_on_all([&copros](coproc::router& r) {
            return ss::do_for_each(
              copros.begin(), copros.end(), [&r](const auto& data) {
                  for (const auto& topic : data.topics) {
                      if (hash_scheme(topic.first) != ss::this_shard_id()) {
                          continue;
                      }
                      vlog(
                        coproc::coproclog.info,
                        "Adding source with id: {} and topic: {}",
                        data.id,
                        topic.first);
                      const auto v = r.add_source(
                        data.id, make_ts(topic.first), topic.second);
                      if (v != coproc::errc::success) {
                          vassert(false, "r.add_source() failed, {}", v);
                      }
                  }
              });
        });
    });
}

ss::future<> coproc_test_fixture::expand() {
    return _api.invoke_on_all([this](storage::api& api) {
        return ss::do_for_each(
          _data.cbegin(), _data.cend(), [this, &api](const auto& ntp) {
              return maybe_create_log(api, ntp);
          });
    });
}

ss::future<> coproc_test_fixture::maybe_create_log(
  storage::api& api, const model::ntp& ntp) {
    if (hash_scheme(ntp.tp.topic) != ss::this_shard_id()) {
        return ss::make_ready_future<>();
    }
    vlog(
      coproc::coproclog.info,
      "Inserting ntp {} onto shard id {}",
      ntp,
      ss::this_shard_id());
    return api.log_mgr()
      .manage(storage::ntp_config(ntp, _cfg_dir))
      .then([](const storage::log&) {});
}
