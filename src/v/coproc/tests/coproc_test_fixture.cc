// Copyright 2020 Vectorized, Inc.
//
// Licensed as a Redpanda Enterprise file under the Redpanda Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
// https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md

#include "coproc_test_fixture.h"

#include "coproc/logger.h"
#include "coproc/tests/utils.h"
#include "model/timeout_clock.h"
#include "model/timestamp.h"
#include "vassert.h"
#include "vlog.h"

#include <seastar/core/smp.hh>

#include <chrono>

using namespace std::literals;

ss::future<> coproc_test_fixture::startup(log_layout_map&& llm) {
    _llm = llm;
    return ss::do_with(std::move(llm), [this](log_layout_map& llm) {
        return wait_for_controller_leadership().then([this, &llm] {
            return ss::parallel_for_each(llm, [this](auto& p) {
                return add_topic(p.first, p.second).then([this, &p] {});
            });
        });
    });
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
    const auto m_ntp = model::materialized_ntp(std::move(ntp));
    const auto shard_id = shard_for_ntp(m_ntp.source_ntp()).get0();
    if (!shard_id) {
        vlog(
          coproc::coproclog.error,
          "No ntp exists, cannot drain from ntp: {}",
          m_ntp.input_ntp());
        return ss::make_ready_future<opt_reader_data_t>(std::nullopt);
    }
    vlog(
      coproc::coproclog.info,
      "searching for ntp {} on shard id {} ...with value for limit: {}",
      m_ntp.input_ntp(),
      *shard_id,
      limit);
    return ss::smp::submit_to(*shard_id, [this, m_ntp, limit, timeout]() {
        return tests::cooperative_spin_wait_with_timeout(
                 5s,
                 [this, m_ntp] {
                     auto& pm = app.partition_manager.local();
                     auto partition = pm.get(m_ntp.source_ntp());
                     return partition->is_leader()
                            && (!m_ntp.is_materialized() || pm.log(m_ntp.input_ntp()));
                 })
          .then([this, m_ntp, limit, timeout] {
              auto& pm = app.partition_manager.local();
              auto partition = pm.get(m_ntp.source_ntp());
              std::optional<storage::log> log;
              if (m_ntp.is_materialized()) {
                  if (auto olog = pm.log(m_ntp.input_ntp())) {
                      log = olog;
                  }
              }
              return do_drain(
                       kafka::partition_wrapper(partition, log), limit, timeout)
                .then(
                  [](auto rval) { return opt_reader_data_t(std::move(rval)); });
          });
    });
}

ss::future<model::record_batch_reader::data_t> coproc_test_fixture::do_drain(
  kafka::partition_wrapper pw,
  std::size_t limit,
  model::timeout_clock::time_point timeout) {
    return ss::do_with(
      static_cast<std::size_t>(0),
      model::offset(0),
      model::record_batch_reader::data_t(),
      [timeout, limit, pw](auto& acc, auto& offset, auto& b) mutable {
          // Loop until the expected number of records is read
          // OR a non-configurable timeout has been reached
          return poll_until(
                   [&b, &acc, &offset, &timeout, limit, pw]() mutable {
                       return pw.make_reader(log_rdr_cfg(model::offset(acc)))
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

ss::future<model::offset> coproc_test_fixture::push(
  const model::ntp& ntp, model::record_batch_reader&& rbr) {
    const auto shard_id = shard_for_ntp(ntp).get0();
    if (!shard_id) {
        vlog(
          coproc::coproclog.error,
          "No ntp exists, cannot push data to log: {}",
          ntp);
        return ss::make_ready_future<model::offset>(model::offset(-1));
    }
    vlog(
      coproc::coproclog.info,
      "Pushing record_batch_reader to ntp: {} on shard_id: {}",
      ntp,
      *shard_id);
    return app.partition_manager.invoke_on(
      *shard_id,
      [ntp, rbr = std::move(rbr)](cluster::partition_manager& pm) mutable {
          auto partition = pm.get(ntp);
          return tests::cooperative_spin_wait_with_timeout(
                   5s, [partition] { return partition->is_leader(); })
            .then([rbr = std::move(rbr), partition]() mutable {
                return partition
                  ->replicate(
                    std::move(rbr),
                    raft::replicate_options(
                      raft::consistency_level::quorum_ack))
                  .then([](auto r) { return r.value().last_offset; });
            });
      });
}

ss::future<std::optional<ss::shard_id>>
coproc_test_fixture::shard_for_ntp(const model::ntp& ntp) {
    return ss::do_with(
      std::vector<ss::shard_id>(),
      [this, ntp](std::vector<ss::shard_id>& shards) {
          return app.storage
            .invoke_on_all([this, ntp, &shards](storage::api& api) {
                if (auto log = api.log_mgr().get(ntp)) {
                    shards.push_back(ss::this_shard_id());
                }
            })
            .then([&shards] {
                vassert(shards.size() <= 1, "Same ntp detected across shards");
                return shards.empty()
                         ? std::nullopt
                         : std::optional<ss::shard_id>(*shards.begin());
            });
      });
}
