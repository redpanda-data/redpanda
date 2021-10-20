/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "coproc/api.h"
#include "coproc/pacemaker.h"
#include "coproc/tests/fixtures/coproc_test_fixture.h"
#include "coproc/tests/utils/coprocessor.h"
#include "model/namespace.h"
#include "storage/tests/utils/random_batch.h"
#include "test_utils/fixture.h"

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test_log.hpp>

#include <chrono>

class tip_fixture : public coproc_test_fixture {
public:
    std::size_t run(coproc::topic_ingestion_policy tip, std::size_t n) {
        model::topic infoo("infoo");
        model::ntp infoo_ntp(
          model::kafka_namespace, infoo, model::partition_id(0));
        setup({{infoo, 1}}).get();

        produce(
          infoo_ntp,
          storage::test::make_random_memory_record_batch_reader(
            model::offset(0), n, 1, false))
          .get();

        enable_coprocessors(
          {{.id = 78,
            .data{
              .tid = coproc::registry::type_identifier::identity_coprocessor,
              .topics = {{infoo, tip}}}}})
          .get();

        /// Wait for the coprocessor to startup before next batch
        coproc::script_id id(78);
        tests::cooperative_spin_wait_with_timeout(60s, [this, id]() {
            return root_fixture()
              ->app.coprocessing->get_pacemaker()
              .map_reduce0(
                [id](coproc::pacemaker& p) {
                    return p.local_script_id_exists(id);
                },
                false,
                std::logical_or<>());
        }).get();

        produce(
          infoo_ntp,
          storage::test::make_random_memory_record_batch_reader(
            model::offset{0}, n, 1, false))
          .get();

        model::ntp output_ntp(
          model::kafka_namespace,
          to_materialized_topic(infoo, identity_coprocessor::identity_topic),
          model::partition_id(0));
        return consume(output_ntp).get0().size();
    }
};

/// 'tip' stands for topic_ingestion_policy
FIXTURE_TEST(test_copro_tip_latest, tip_fixture) {
    auto result = run(tp_latest, 40);
    BOOST_CHECK_EQUAL(result, 40);
}

FIXTURE_TEST(test_copro_tip_earliest, tip_fixture) {
    auto result = run(tp_earliest, 40);
    BOOST_CHECK_EQUAL(result, 80);
}

FIXTURE_TEST(test_copro_tip_stored, coproc_test_fixture) {
    model::topic sttp("sttp");
    model::ntp sttp_ntp(model::kafka_namespace, sttp, model::partition_id(0));
    model::ntp output_ntp(
      model::kafka_namespace,
      to_materialized_topic(sttp, identity_coprocessor::identity_topic),
      model::partition_id(0));
    setup({{sttp, 1}}).get();

    enable_coprocessors(
      {{.id = 7843,
        .data{
          .tid = coproc::registry::type_identifier::identity_coprocessor,
          .topics = {{sttp, tp_stored}}}}})
      .get();

    produce(
      sttp_ntp,
      storage::test::make_random_memory_record_batch_reader(
        model::offset{0}, 40, 1, false))
      .get();

    auto a_results = consume(output_ntp).get();
    BOOST_CHECK(a_results.size() == 40);

    ss::sleep(1s).get();
    info("Restarting....");
    restart().get();

    produce(
      sttp_ntp,
      storage::test::make_random_memory_record_batch_reader(
        model::offset{0}, 40, 1, false))
      .get();

    auto results = consume(output_ntp).get();
    BOOST_CHECK(results.size() >= 80);
}
