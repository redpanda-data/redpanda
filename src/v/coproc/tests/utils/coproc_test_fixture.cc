/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "coproc_test_fixture.h"

#include "coproc/logger.h"
#include "coproc/tests/utils/helpers.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "model/timestamp.h"
#include "vassert.h"
#include "vlog.h"

#include <seastar/core/smp.hh>

#include <chrono>

using namespace std::literals;

coproc_test_fixture::~coproc_test_fixture() {
    if (_client.is_valid()) {
        _client.stop().get();
    }
}

coproc_test_fixture::script_manager_client coproc_test_fixture::make_client() {
    return rpc::client<coproc::script_manager_client_protocol>(
      rpc::transport_configuration{
        .server_addr = ss::socket_address(
          ss::net::inet_address("127.0.0.1"), 43118),
        .credentials = nullptr});
}

ss::future<> coproc_test_fixture::startup(log_layout_map llm) {
    _llm = llm;
    return ss::do_with(std::move(llm), [this](log_layout_map& llm) {
        return wait_for_controller_leadership()
          .then([this] { return _client.connect(); })
          .then([this, &llm] {
              return ss::parallel_for_each(
                llm, [this](auto& p) { return add_topic(p.first, p.second); });
          });
    });
}

ss::future<coproc_test_fixture::opt_reader_data_t> coproc_test_fixture::drain(
  const model::ntp& ntp,
  std::size_t limit,
  model::offset offset,
  model::timeout_clock::time_point timeout) {
    const auto m_ntp = model::materialized_ntp(std::move(ntp));
    return shard_for_ntp(m_ntp.source_ntp())
      .then(
        [this, m_ntp, offset, limit, timeout](auto shard_id) {
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
            return ss::smp::submit_to(
              *shard_id, [this, m_ntp, offset, limit, timeout]() {
                  return tests::cooperative_spin_wait_with_timeout(
                           60s,
                           [this, m_ntp] {
                               auto& pm = app.partition_manager.local();
                               auto partition = pm.get(m_ntp.source_ntp());
                               return partition->is_leader()
                            && (!m_ntp.is_materialized() || pm.log(m_ntp.input_ntp()));
                           })
                    .then([this, m_ntp, limit, offset, timeout] {
                        auto& pm = app.partition_manager.local();
                        auto partition = pm.get(m_ntp.source_ntp());
                        std::optional<storage::log> log;
                        if (m_ntp.is_materialized()) {
                            if (auto olog = pm.log(m_ntp.input_ntp())) {
                                log = olog;
                            }
                        }
                        return do_drain(
                                 kafka::partition_wrapper(partition, log),
                                 offset,
                                 limit,
                                 timeout)
                          .then([](auto rval) {
                              return opt_reader_data_t(std::move(rval));
                          });
                    });
              });
        });
}

storage::log_reader_config log_rdr_cfg(const model::offset& min_offset) {
    return storage::log_reader_config(
      min_offset,
      model::model_limits<model::offset>::max(),
      0,
      std::numeric_limits<size_t>::max(),
      ss::default_priority_class(),
      raft::data_batch_type,
      std::nullopt,
      std::nullopt);
}

ss::future<model::record_batch_reader::data_t> coproc_test_fixture::do_drain(
  kafka::partition_wrapper pw,
  model::offset offset,
  std::size_t limit,
  model::timeout_clock::time_point timeout) {
    struct state {
        std::size_t batches_read{0};
        model::offset next_offset;
        model::record_batch_reader::data_t batches;
        explicit state(model::offset o)
          : next_offset(o) {}
    };
    return ss::do_with(state(offset), [limit, timeout, pw](state& s) mutable {
        return ss::do_until(
                 [&s, limit, timeout] {
                     const auto now = model::timeout_clock::now();
                     return (s.batches_read >= limit) || (now > timeout);
                 },
                 [&s, pw]() mutable {
                     return pw.make_reader(log_rdr_cfg(s.next_offset))
                       .then([](model::record_batch_reader rbr) {
                           return model::consume_reader_to_memory(
                             std::move(rbr), model::no_timeout);
                       })
                       .then([&s](model::record_batch_reader::data_t b) {
                           if (b.empty()) {
                               return ss::sleep(20ms);
                           }
                           s.batches_read += b.size();
                           s.next_offset = ++b.back().last_offset();
                           for (model::record_batch& rb : b) {
                               s.batches.push_back(std::move(rb));
                           }
                           return ss::now();
                       });
                 })
          .then([&s] { return std::move(s.batches); });
    });
}

ss::future<model::offset> coproc_test_fixture::push(
  const model::ntp& ntp, model::record_batch_reader rbr) {
    return shard_for_ntp(ntp).then([this, ntp, rbr = std::move(rbr)](
                                     auto shard_id) mutable {
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
                       60s, [partition] { return partition->is_leader(); })
                .then([rbr = std::move(rbr), partition]() mutable {
                    return partition
                      ->replicate(
                        std::move(rbr),
                        raft::replicate_options(
                          raft::consistency_level::quorum_ack))
                      .then([](auto r) { return r.value().last_offset; });
                });
          });
    });
}

ss::future<std::optional<ss::shard_id>>
coproc_test_fixture::shard_for_ntp(const model::ntp& ntp) {
    return app.storage
      .map_reduce0(
        [ntp](storage::api& api) -> std::optional<ss::shard_id> {
            if (auto log = api.log_mgr().get(ntp)) {
                return ss::this_shard_id();
            }
            return std::nullopt;
        },
        std::vector<ss::shard_id>(),
        reduce::push_back_opt())
      .then([ntp](std::vector<ss::shard_id> sids) {
          vassert(
            sids.size() <= 1, "ntp {} duplicate detected across shards", ntp);
          return sids.size() == 1 ? std::optional<ss::shard_id>(sids.front())
                                  : std::nullopt;
      });
}
