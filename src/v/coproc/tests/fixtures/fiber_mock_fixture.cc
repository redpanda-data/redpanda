/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "coproc/tests/fixtures/fiber_mock_fixture.h"

#include "coproc/api.h"
#include "coproc/pacemaker.h"
#include "coproc/tests/utils/batch_utils.h"

#include <seastar/core/coroutine.hh>

enable_coproc::enable_coproc() {
    ss::smp::invoke_on_all([]() {
        auto& config = config::shard_local_cfg();
        config.get("coproc_offset_flush_interval_ms").set_value(500ms);
        config.get("enable_coproc").set_value(true);
    }).get0();
}

ss::future<> fiber_mock_fixture::init_test(test_parameters params) {
    co_await _state.start();
    co_await wait_for_controller_leadership();
    co_await add_topic(params.tn, params.partitions);
    for (auto i = 0; i < params.partitions; ++i) {
        auto ntp = model::ntp(
          params.tn.ns, params.tn.tp, model::partition_id(i));
        auto shard = app.shard_table.local().shard_for(ntp);
        vassert(shard, "topic not up yet");
        co_await _state.invoke_on(
          *shard, [this, ntp, params](state& s) -> ss::future<> {
              return make_source(ntp, s, params).then([ntp, &s](auto src) {
                  auto [_, success] = s.routes.emplace(ntp, src);
                  vassert(success, "no double insert attempt should occur");
              });
          });
    }
}

static ss::future<>
do_verify_partition(storage::log log, model::ntp ntp, std::size_t n) {
    auto cfg = storage::log_reader_config(
      model::offset{0},
      log.offsets().dirty_offset,
      ss::default_priority_class());
    cfg.type_filter = model::record_batch_type::raft_data;
    auto rbr = co_await log.make_reader(cfg);
    auto batch = co_await model::consume_reader_to_memory(
      std::move(rbr), model::no_timeout);
    BOOST_CHECK_EQUAL(num_records(batch), n);
}

ss::future<> fiber_mock_fixture::verify_results(expected_t expected) {
    return _state.invoke_on_all([this, expected](state&) mutable {
        return ss::do_with(std::move(expected), [this](expected_t& expected) {
            return ss::parallel_for_each(
              expected, [this](expected_t::value_type& p) {
                  auto log = app.storage.local().log_mgr().get(p.first);
                  return log ? do_verify_partition(*log, p.first, p.second)
                             : ss::now();
              });
        });
    });
}

ss::future<> fiber_mock_fixture::do_run(
  fiber_mock_fixture::state& s, model::timeout_clock::time_point timeout) {
    auto finished = [&s]() {
        for (auto& [ntp, routes] : s.routes) {
            auto ofs = routes->wctx.unique_offsets();
            auto found = s.high_input.find(ntp);
            vassert(found != s.high_input.end(), "Misconfigured setup");
            if (ofs.empty()) {
                /// Since all of our test copros produce data, assuming if 'no
                /// data was ever published finished must be false' is OK
                return false;
            } else if (ofs.size() > 1) {
                /// Some retry logic is going on, cannot be the case that its
                /// finished
                return false;
            } else if (*(ofs.begin()) < found->second) {
                /// Theres still data to consume and process
                return false;
            }
        }
        return true;
    };

    /// Emmulates a single script_context fiber, only instead of looping
    /// until shutdown, this loops until single input has been
    /// successfully read
    while (!finished()) {
        if (model::timeout_clock::now() >= timeout) {
            throw ss::timed_out_error();
        }
        /// Read from all coproc inputs
        auto read_results = co_await coproc::read_from_inputs(
          make_input_read_args(s));
        if (read_results.empty()) {
            /// Read returned nothing, sleep and try again
            co_await ss::sleep(100ms);
            continue;
        }

        /// Push data through coprocessor
        auto transforms = co_await transform(
          _id, std::move(read_results), s.copro);

        /// Write to materialized topics
        co_await coproc::write_materialized(
          std::move(transforms), make_output_write_args(s));
    }
}

ss::future<coproc::output_write_inputs> fiber_mock_fixture::transform(
  coproc::script_id id,
  coproc::input_read_results irr,
  const std::unique_ptr<basic_copro_base>& cp) {
    coproc::output_write_inputs owi;
    for (auto& e : irr) {
        auto data = co_await model::consume_reader_to_memory(
          std::move(e.reader), model::no_timeout);
        auto result = cp->operator()(e.ntp, std::move(data));
        if (result.empty()) {
            owi.push_back(coproc::process_batch_reply::data{
              .id = id,
              .source = e.ntp,
              .ntp = e.ntp,
              .reader = model::make_memory_record_batch_reader(
                model::record_batch_reader::data_t())});
            continue;
        }
        for (auto& [topic, result_batch] : result) {
            owi.push_back(coproc::process_batch_reply::data{
              .id = id,
              .source = e.ntp,
              .ntp = model::ntp(e.ntp.ns, topic, e.ntp.tp.partition),
              .reader = model::make_memory_record_batch_reader(
                std::move(result_batch))});
        }
    }
    co_return owi;
}

coproc::input_read_args fiber_mock_fixture::make_input_read_args(state& s) {
    auto& shared_res = app.coprocessing->get_pacemaker().local().resources();
    return coproc::input_read_args{
      .id = _id,
      .read_sem = shared_res.read_sem,
      .abort_src = s.as,
      .inputs = s.routes};
}

coproc::output_write_args fiber_mock_fixture::make_output_write_args(state& s) {
    auto& shared_res = app.coprocessing->get_pacemaker().local().resources();
    return coproc::output_write_args{
      .id = _id,
      .metadata = shared_res.rs.metadata_cache,
      .frontend = shared_res.rs.mt_frontend,
      .pm = shared_res.rs.cp_partition_manager,
      .inputs = s.routes,
      .denylist = shared_res.in_progress_deletes,
      .locks = shared_res.log_mtx};
}

ss::future<ss::lw_shared_ptr<coproc::source>> fiber_mock_fixture::make_source(
  model::ntp input, state& s, test_parameters params) {
    auto partition = app.partition_manager.local().get(input);
    vassert(partition, "Input must not be nullptr");
    auto batch = make_random_batch(params.records_per_input);
    co_await tests::cooperative_spin_wait_with_timeout(
      2s, [partition]() { return partition->is_elected_leader(); });
    auto r = co_await partition->raft()->replicate(
      std::move(batch),
      raft::replicate_options(raft::consistency_level::leader_ack));
    vassert(!r.has_error(), "Write error: {}", r.error());
    s.high_input.emplace(input, r.value().last_offset);
    vassert(
      params.policy != coproc::topic_ingestion_policy::stored,
      "Stored policy not supported in this fixture");
    auto srcs = ss::make_lw_shared<coproc::source>();
    srcs->rctx.absolute_start = params.policy
                                    == coproc::topic_ingestion_policy::earliest
                                  ? model::offset{0}
                                  : r.value().last_offset;
    srcs->rctx.input = partition;
    co_return srcs;
}
