// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "model/fundamental.h"
#include "raft/offset_translator.h"
#include "random/generators.h"
#include "storage/api.h"
#include "storage/kvstore.h"
#include "storage/log_manager.h"
#include "storage/record_batch_builder.h"
#include "test_utils/fixture.h"

using namespace std::chrono_literals; // NOLINT

struct offset_translator_fixture {
    offset_translator_fixture()
      : _test_dir(
        fmt::format("test_{}", random_generators::gen_alphanum_string(6)))
      , _api(make_kv_cfg(), make_log_cfg())
      , tr(
          {model::record_batch_type::raft_configuration},
          raft::group_id(0),
          test_ntp,
          _api) {
        _api.start().get();
        tr.start(raft::offset_translator::must_reset::yes, {}).get();
    }

    storage::kvstore_config make_kv_cfg() const {
        return storage::kvstore_config(
          1_MiB, 10ms, _test_dir, storage::debug_sanitize_files::yes);
    }

    storage::log_config make_log_cfg() const {
        return storage::log_config(
          storage::log_config::storage_type::disk,
          _test_dir,
          100_MiB,
          storage::debug_sanitize_files::yes);
    }

    model::record_batch
    create_batch(model::record_batch_type type, model::offset o) const {
        storage::record_batch_builder b(type, o);
        b.add_raw_kv(iobuf{}, iobuf{});
        return std::move(b).build();
    }

    model::ntp test_ntp = model::ntp(
      model::ns("test"), model::topic("tp"), model::partition_id(0));
    ss::sstring _test_dir;
    storage::api _api;
    raft::offset_translator tr;

    ~offset_translator_fixture() { _api.stop().get(); }

    void validate_offset_translation(
      model::offset log_offset, model::offset kafka_offset) {
        BOOST_REQUIRE_EQUAL(tr.from_log_offset(log_offset), kafka_offset);
        BOOST_REQUIRE_EQUAL(tr.to_log_offset(kafka_offset), log_offset);
    }
};

FIXTURE_TEST(test_translating_to_kafka_offsets, offset_translator_fixture) {
    std::set<model::offset> batch_offsets{
      model::offset(0),
      model::offset(1),
      // data batch @ 2 -> kafka 0
      model::offset(3),
      model::offset(4),
      model::offset(5),
      // data batch @ 6 -> kafka 1
      model::offset(7),
      model::offset(8),
      // data batch @ 9 -> kafka 3
      model::offset(10)};

    for (auto o : batch_offsets) {
        tr.process(
          create_batch(model::record_batch_type::raft_configuration, o));
    }

    validate_offset_translation(model::offset(2), model::offset(0));
    validate_offset_translation(model::offset(6), model::offset(1));
    validate_offset_translation(model::offset(9), model::offset(2));
    validate_offset_translation(model::offset(11), model::offset(3));
    validate_offset_translation(model::offset(12), model::offset(4));
}

FIXTURE_TEST(
  test_translating_to_kafka_offsets_first, offset_translator_fixture) {
    std::set<model::offset> batch_offsets{// data batch @ 0 -> kafka 0
                                          // data batch @ 1 -> kafka 1
                                          model::offset(2),
                                          model::offset(3),
                                          model::offset(4),
                                          model::offset(5),
                                          model::offset(6),
                                          // data batch @ 7 -> kafka 2
                                          model::offset(8),
                                          // data batch @ 9 -> kafka 3
                                          model::offset(10)};

    for (auto o : batch_offsets) {
        tr.process(
          create_batch(model::record_batch_type::raft_configuration, o));
    }

    validate_offset_translation(model::offset(0), model::offset(0));
    validate_offset_translation(model::offset(1), model::offset(1));
    validate_offset_translation(model::offset(7), model::offset(2));
    validate_offset_translation(model::offset(9), model::offset(3));
    validate_offset_translation(model::offset(11), model::offset(4));
}

FIXTURE_TEST(random_translation_test, offset_translator_fixture) {
    auto batches_count = 1000;
    std::set<model::offset> batch_offsets;
    /**
     * control batches in random places
     */
    for (auto i = 0; i < batches_count; ++i) {
        batch_offsets.emplace(random_generators::get_int(10000));
    }

    for (auto o : batch_offsets) {
        tr.process(
          create_batch(model::record_batch_type::raft_configuration, o));
    }

    // go over whole offset space
    for (auto o : boost::irange(0, 11000)) {
        model::offset log_offset(o);

        if (batch_offsets.contains(log_offset)) {
            continue;
        }
        auto kafka_offset = tr.from_log_offset(log_offset);
        auto reverse_log_offset = tr.to_log_offset(kafka_offset);
        BOOST_REQUIRE_EQUAL(log_offset, reverse_log_offset);
    }
}

FIXTURE_TEST(random_translation_test_with_hint, offset_translator_fixture) {
    auto batches_count = 1000;
    std::set<model::offset> batch_offsets;
    /**
     * control batches in random places
     */
    for (auto i = 0; i < batches_count; ++i) {
        batch_offsets.emplace(random_generators::get_int(10000));
    }

    for (auto o : batch_offsets) {
        tr.process(
          create_batch(model::record_batch_type::raft_configuration, o));
    }

    // go over whole offset space
    model::offset prev_log_offset;
    for (auto o : boost::irange(0, 11000)) {
        model::offset log_offset(o);

        if (batch_offsets.contains(log_offset)) {
            continue;
        }
        auto kafka_offset = tr.from_log_offset(log_offset);
        auto reverse_log_offset = tr.to_log_offset(
          kafka_offset, prev_log_offset);
        prev_log_offset = reverse_log_offset;
        BOOST_REQUIRE_EQUAL(log_offset, reverse_log_offset);
    }
}

FIXTURE_TEST(immutabitlity_test, offset_translator_fixture) {
    auto batches_count = 100;
    auto end_offset = 1100; // exclusive
    std::set<model::offset> batch_offsets;
    /**
     * control batches in random places
     */
    for (auto i = 0; i < batches_count; ++i) {
        batch_offsets.emplace(random_generators::get_int(1000));
    }

    for (auto o : batch_offsets) {
        tr.process(
          create_batch(model::record_batch_type::raft_configuration, o));
    }

    tr.process(create_batch(
      model::record_batch_type::raft_data, model::offset(end_offset - 1)));

    // log offset -> kafka offset
    std::unordered_map<model::offset, model::offset> offsets_mapping;

    // go over whole offset space
    for (auto o : boost::irange(0, end_offset)) {
        model::offset log_offset(o);

        if (batch_offsets.contains(log_offset)) {
            continue;
        }
        auto kafka_offset = tr.from_log_offset(log_offset);
        auto reverse_log_offset = tr.to_log_offset(kafka_offset);

        BOOST_REQUIRE_EQUAL(log_offset, reverse_log_offset);
        offsets_mapping.emplace(log_offset, kafka_offset);
    }

    auto validate_offsets_immutable = [&](int64_t start) {
        for (auto o : boost::irange<int64_t>(start, end_offset)) {
            model::offset log_offset(o);
            if (batch_offsets.contains(log_offset)) {
                continue;
            }
            model::offset k_offset = tr.from_log_offset(log_offset);
            // validate that offset havent changed
            BOOST_REQUIRE_EQUAL(offsets_mapping[log_offset], k_offset);
        }
    };

    // prefix truncate
    auto truncate_at = random_generators::get_int(end_offset - 1);
    tr.prefix_truncate(model::offset(truncate_at)).get();

    validate_offsets_immutable(truncate_at + 1);

    // add new config at the end
    tr.process(create_batch(
      model::record_batch_type::raft_configuration,
      model::offset(end_offset + 1)));

    validate_offsets_immutable(truncate_at + 1);
}
