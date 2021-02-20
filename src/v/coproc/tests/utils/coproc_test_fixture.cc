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

#include "config/configuration.h"
#include "coproc/logger.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "model/timestamp.h"
#include "test_utils/async.h"
#include "vassert.h"
#include "vlog.h"

#include <seastar/core/smp.hh>

#include <chrono>

ss::future<> coproc_test_fixture::setup(log_layout_map llm) {
    return ss::do_with(std::move(llm), [this](const log_layout_map& llm) {
        return wait_for_controller_leadership().then([this, &llm] {
            return ss::do_for_each(
              llm, [this](const log_layout_map::value_type& p) {
                  return add_topic(
                    model::topic_namespace(model::kafka_namespace, p.first),
                    p.second);
              });
        });
    });
}

static storage::log_reader_config log_rdr_cfg(const model::offset& min_offset) {
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

static ss::future<model::record_batch_reader::data_t> do_drain(
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

ss::future<coproc_test_fixture::opt_reader_data_t> coproc_test_fixture::drain(
  const model::ntp& ntp,
  std::size_t limit,
  model::offset offset,
  model::timeout_clock::time_point timeout) {
    const auto m_ntp = model::materialized_ntp(std::move(ntp));
    auto shard_id = app.shard_table.local().shard_for(m_ntp.source_ntp());
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
    return app.partition_manager.invoke_on(
      *shard_id,
      [m_ntp, limit, offset, timeout](cluster::partition_manager& pm) {
          return tests::cooperative_spin_wait_with_timeout(
                   60s,
                   [&pm, m_ntp] {
                       auto partition = pm.get(m_ntp.source_ntp());
                       return partition && partition->is_leader()
                              && (!m_ntp.is_materialized() || pm.log(m_ntp.input_ntp()));
                   })
            .then([&pm, m_ntp, limit, offset, timeout] {
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
}

ss::future<model::offset> coproc_test_fixture::push(
  const model::ntp& ntp, model::record_batch_reader rbr) {
    auto shard_id = app.shard_table.local().shard_for(ntp);
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
                   60s,
                   [partition] { return partition && partition->is_leader(); })
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
