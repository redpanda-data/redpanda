/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "coproc/logger.h"
#include "coproc/tests/fixtures/fiber_mock_fixture.h"
#include "coproc/tests/utils/batch_utils.h"
#include "model/tests/random_batch.h"

#include <seastar/core/coroutine.hh>

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test_log.hpp>

fiber_mock_fixture::expected_t make_expected_set(
  const std::vector<model::topic>& ts,
  int records_per_input,
  int n_partitions) {
    fiber_mock_fixture::expected_t et;
    et.reserve(n_partitions);
    for (auto i = 0; i < n_partitions; ++i) {
        for (const auto& t : ts) {
            et.emplace(
              model::ntp(model::kafka_namespace, t, model::partition_id(i)),
              records_per_input);
        }
    }
    return et;
}

/// Test that everything works ok in the simplest of cases. Note that its highly
/// likely there will be retries here too. The common case is upon creation of a
/// materialized topic, the run loop won't wait until its completed, it will
/// sleep, retry, and come back around to find the topic already created
FIXTURE_TEST(identity_offset_test, fiber_mock_fixture) {
    /// This coprocessor maps its input to three outputs
    struct identity_copro final : public basic_copro_base {
        result_t operator()(model::ntp, batch_t data) final {
            result_t r;
            r.emplace(output_a(), copy_batch(data));
            r.emplace(output_b(), copy_batch(data));
            r.emplace(output_c(), copy_batch(data));
            return r;
        }
        static model::topic output_a() { return model::topic("DEF"); }
        static model::topic output_b() { return model::topic("GHI"); }
        static model::topic output_c() { return model::topic("JKL"); }
    };
    constexpr int records_per_input = 325;
    constexpr int n_partitions = 1;
    auto et = make_expected_set(
      {{identity_copro::output_a(),
        identity_copro::output_b(),
        identity_copro::output_c()}},
      records_per_input,
      n_partitions);

    init_test(test_parameters{
                .tn = model::topic_namespace(
                  model::kafka_namespace, model::topic("ABC")),
                .partitions = n_partitions,
                .records_per_input = records_per_input,
                .policy = coproc::topic_ingestion_policy::earliest})
      .get();
    run<identity_copro>().get();
    verify_results(std::move(et)).get();
}

/// Test that an output that recieves data once and no more doesn't cause an
/// infinite loop. This coprocessor produces onto \ref output_a_ntp() only once,
/// after that it produces onto all other topics in its output set (3 others).
FIXTURE_TEST(lagging_topic_test, fiber_mock_fixture) {
    struct lagging_copro final : public basic_copro_base {
        static model::topic output_a() { return model::topic("456"); }
        static model::topic output_b() { return model::topic("789"); }
        static model::topic output_c() { return model::topic("000"); }
        static model::ntp output_a_ntp() {
            return model::ntp(
              model::kafka_namespace, output_a(), model::partition_id(0));
        }
        result_t operator()(model::ntp ntp, batch_t data) final {
            result_t r;
            if (ntp.tp.partition == model::partition_id(0)) {
                if (data.begin()->base_offset() == model::offset{1}) {
                    /// Push a single record
                    r.emplace(
                      output_a(),
                      model::test::make_random_batches(
                        model::test::record_batch_spec{
                          .count = 1, .records = 1}));
                }
            } else {
                r.emplace(output_a(), copy_batch(data));
            }
            /// output_a is to be 'filtered', should observe no infinite
            /// loop, as its offset is pushed even though its not the
            /// result set
            r.emplace(output_b(), copy_batch(data));
            r.emplace(output_c(), copy_batch(data));
            return r;
        }
    };
    constexpr int records_per_input = 1225;
    constexpr int n_partitions = 10;
    auto et = make_expected_set(
      {{lagging_copro::output_a(),
        lagging_copro::output_b(),
        lagging_copro::output_c()}},
      records_per_input,
      n_partitions);
    /// With the exception of the partition that filters...
    et[lagging_copro::output_a_ntp()] = 1;

    init_test(test_parameters{
                .tn = model::topic_namespace(
                  model::kafka_namespace, model::topic("123")),
                .partitions = n_partitions,
                .records_per_input = records_per_input,
                .policy = coproc::topic_ingestion_policy::earliest})
      .get();
    run<lagging_copro>().get();
    verify_results(std::move(et)).get();
}

/// Tests that materialized topics that are generated long after the first,
/// start from the programmed offset instead of continuing at wherever the tip
/// of the respective input is currently at.
FIXTURE_TEST(latecomer_topic_earliest_test, fiber_mock_fixture) {
    struct latecomer_copro final : public basic_copro_base {
        static model::topic output_a() { return model::topic("wvu"); }
        static model::topic output_b() { return model::topic("tsr"); }
        result_t operator()(model::ntp ntp, batch_t data) final {
            result_t r;
            r.emplace(output_a(), copy_batch(data));
            if (data.begin()->base_offset() > model::offset{500}) {
                _started = true;
            }
            if (_started) {
                r.emplace(output_b(), copy_batch(data));
            }
            return r;
        }

    private:
        bool _started{true};
    };

    constexpr int records_per_input = 875;
    constexpr int n_partitions = 5;
    auto et = make_expected_set(
      {{latecomer_copro::output_a(), latecomer_copro::output_b()}},
      records_per_input,
      n_partitions);

    init_test(test_parameters{
                .tn = model::topic_namespace(
                  model::kafka_namespace, model::topic("zyx")),
                .partitions = n_partitions,
                .records_per_input = records_per_input,
                .policy = coproc::topic_ingestion_policy::earliest})
      .get();
    run<latecomer_copro>().get();
    verify_results(std::move(et)).get();
}
