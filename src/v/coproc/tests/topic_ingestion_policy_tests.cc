/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "coproc/api.h"
#include "coproc/pacemaker.h"
#include "coproc/tests/fixtures/coproc_test_fixture.h"
#include "coproc/tests/utils/batch_utils.h"
#include "coproc/tests/utils/coprocessor.h"
#include "model/namespace.h"
#include "test_utils/fixture.h"

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test_log.hpp>

#include <chrono>

class tip_fixture : public coproc_test_fixture {
public:
    std::size_t run(
      coproc::topic_ingestion_policy tip,
      std::size_t records,
      std::size_t expect) {
        model::topic infoo("infoo");
        model::ntp infoo_ntp(
          model::kafka_namespace, infoo, model::partition_id(0));
        setup({{infoo, 1}}).get();

        produce(infoo_ntp, make_random_batch(records)).get();
        consume(infoo_ntp, 400).get();

        enable_coprocessors(
          {{.id = 78,
            .data{
              .tid = coproc::registry::type_identifier::identity_coprocessor,
              .topics = {{infoo, tip}}}}})
          .get();

        produce(infoo_ntp, make_random_batch(records)).get();
        consume(infoo_ntp, records).get();

        model::ntp output_ntp(
          model::kafka_namespace,
          to_materialized_topic(infoo, identity_coprocessor::identity_topic),
          model::partition_id(0));

        auto rbs = consume(output_ntp, expect).get0();
        return num_records(rbs);
    }
};

FIXTURE_TEST(test_copro_tip_latest, tip_fixture) {
    auto results = run(tp_latest, 400, 400);
    BOOST_CHECK_EQUAL(400, results);
}

FIXTURE_TEST(test_copro_tip_earliest, tip_fixture) {
    auto results = run(tp_earliest, 400, 800);
    BOOST_CHECK_EQUAL(800, results);
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

    produce(sttp_ntp, make_random_batch(200)).get();
    auto a_results = consume(output_ntp, 200).get();
    BOOST_CHECK_EQUAL(num_records(a_results), 200);

    ss::sleep(1s).get();
    info("Restarting....");
    restart().get();

    produce(sttp_ntp, make_random_batch(200)).get();
    auto results = consume(output_ntp, 400).get();
    BOOST_CHECK_GE(num_records(results), 400);
}
