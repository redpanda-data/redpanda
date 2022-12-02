// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "model/fundamental.h"
#include "model/record.h"
#include "pandaproxy/schema_registry/avro.h"
#include "pandaproxy/schema_registry/sharded_store.h"
#include "pandaproxy/schema_registry/storage.h"
#include "pandaproxy/schema_registry/util.h"

#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

#include <stdexcept>

namespace pps = pandaproxy::schema_registry;

const pps::canonical_schema_definition string_def0{
  pps::sanitize_avro_schema_definition(
    {R"({"type":"string"})", pps::schema_type::avro})
    .value()};
const pps::canonical_schema_definition int_def0{
  pps::sanitize_avro_schema_definition(
    {R"({"type": "int"})", pps::schema_type::avro})
    .value()};
const pps::subject subject0{"subject0"};
constexpr pps::topic_key_magic magic0{0};
constexpr pps::topic_key_magic magic1{1};
constexpr pps::topic_key_magic magic2{2};
constexpr pps::schema_version version0{0};
constexpr pps::schema_version version1{1};
constexpr pps::schema_id id0{0};
constexpr pps::schema_id id1{1};

inline model::record_batch make_delete_subject_batch(pps::subject sub) {
    storage::record_batch_builder rb{
      model::record_batch_type::raft_data, model::offset{0}};

    rb.add_raw_kv(
      to_json_iobuf(pps::delete_subject_key{
        .seq{model::offset{0}}, .node{model::node_id{0}}, .sub{sub}}),
      to_json_iobuf(pps::delete_subject_value{.sub{sub}}));
    return std::move(rb).build();
}

inline model::record_batch make_delete_subject_permanently_batch(
  pps::subject sub, const std::vector<pps::schema_version>& versions) {
    storage::record_batch_builder rb{
      model::record_batch_type::raft_data, model::offset{0}};

    std::for_each(versions.cbegin(), versions.cend(), [&](auto version) {
        rb.add_raw_kv(
          to_json_iobuf(pps::schema_key{
            .seq{model::offset{0}},
            .node{model::node_id{0}},
            .sub{sub},
            .version{version}}),
          std::nullopt);
    });
    return std::move(rb).build();
}

SEASTAR_THREAD_TEST_CASE(test_consume_to_store) {
    pps::sharded_store s;
    s.start(ss::default_smp_service_group()).get();
    auto stop_store = ss::defer([&s]() { s.stop().get(); });

    // This kafka client will not be used by the sequencer
    // (which itself is only instantiated to receive consume_to_store's
    //  offset updates), is just needed for constructor;
    ss::sharded<kafka::client::client> dummy_kafka_client;
    dummy_kafka_client
      .start(
        to_yaml(kafka::client::configuration{}, config::redact_secrets::no))
      .get();
    auto stop_kafka_client = ss::defer(
      [&dummy_kafka_client]() { dummy_kafka_client.stop().get(); });

    ss::sharded<pps::seq_writer> seq;
    seq
      .start(
        model::node_id{0},
        ss::default_smp_service_group(),
        std::reference_wrapper(dummy_kafka_client),
        std::reference_wrapper(s))
      .get();
    auto stop_seq = ss::defer([&seq]() { seq.stop().get(); });

    auto c = pps::consume_to_store(s, seq.local());

    auto sequence = model::offset{0};
    const auto node_id = model::node_id{123};

    auto good_schema_1 = pps::as_record_batch(
      pps::schema_key{sequence, node_id, subject0, version0, magic1},
      pps::schema_value{{subject0, string_def0}, version0, id0});
    BOOST_REQUIRE_NO_THROW(c(good_schema_1.copy()).get());

    auto s_res = s.get_subject_schema(
                    subject0, version0, pps::include_deleted::no)
                   .get();
    BOOST_REQUIRE_EQUAL(s_res.schema.def(), string_def0);

    pps::canonical_schema::references refs{
      {.name{"ref"}, .sub{subject0}, .version{version0}}};
    auto good_schema_ref_1 = pps::as_record_batch(
      pps::schema_key{sequence, node_id, subject0, version1, magic1},
      pps::schema_value{{subject0, string_def0, refs}, version1, id1});
    BOOST_REQUIRE_NO_THROW(c(good_schema_ref_1.copy()).get());

    auto s_ref_res = s.get_subject_schema(
                        subject0, version1, pps::include_deleted::no)
                       .get();
    BOOST_REQUIRE_EQUAL(s_ref_res.schema.def(), string_def0);
    BOOST_REQUIRE_EQUAL(s_ref_res.schema.sub(), subject0);
    BOOST_REQUIRE_EQUAL(s_ref_res.id, id1);
    BOOST_REQUIRE_EQUAL_COLLECTIONS(
      s_ref_res.schema.refs().begin(),
      s_ref_res.schema.refs().end(),
      refs.begin(),
      refs.end());

    auto bad_schema_magic = pps::as_record_batch(
      pps::schema_key{sequence, node_id, subject0, version0, magic2},
      pps::schema_value{{subject0, string_def0}, version0, id0});
    BOOST_REQUIRE_THROW(c(bad_schema_magic.copy()).get(), pps::exception);

    BOOST_REQUIRE(
      s.get_compatibility().get() == pps::compatibility_level::backward);
    BOOST_REQUIRE(
      s.get_compatibility(subject0, pps::default_to_global::yes).get()
      == pps::compatibility_level::backward);

    auto good_config = pps::as_record_batch(
      pps::config_key{sequence, node_id, subject0, magic0},
      pps::config_value{pps::compatibility_level::full});
    BOOST_REQUIRE_NO_THROW(c(good_config.copy()).get());

    BOOST_REQUIRE(
      s.get_compatibility(subject0, pps::default_to_global::yes).get()
      == pps::compatibility_level::full);

    auto bad_config_magic = pps::as_record_batch(
      pps::config_key{sequence, node_id, subject0, magic1},
      pps::config_value{pps::compatibility_level::full});
    BOOST_REQUIRE_THROW(c(bad_config_magic.copy()).get(), pps::exception);

    // Test soft delete
    BOOST_REQUIRE_EQUAL(
      s.get_subjects(pps::include_deleted::no).get().size(), 1);
    BOOST_REQUIRE_EQUAL(
      s.get_subjects(pps::include_deleted::yes).get().size(), 1);
    auto delete_sub = make_delete_subject_batch(subject0);
    BOOST_REQUIRE_NO_THROW(c(delete_sub.copy()).get());
    BOOST_REQUIRE_EQUAL(
      s.get_subjects(pps::include_deleted::no).get().size(), 0);
    BOOST_REQUIRE_EQUAL(
      s.get_subjects(pps::include_deleted::yes).get().size(), 1);

    // Test permanent delete
    auto v_res = s.get_versions(subject0, pps::include_deleted::yes).get();
    BOOST_REQUIRE_EQUAL(v_res.size(), 2);
    auto perm_delete_sub = make_delete_subject_permanently_batch(
      subject0, v_res);
    BOOST_REQUIRE_NO_THROW(c(perm_delete_sub.copy()).get());
    // Perma-deleting all versions also deletes the subject
    BOOST_REQUIRE_THROW(
      s.get_versions(subject0, pps::include_deleted::yes).get(),
      pps::exception);

    // Expect subject is deleted
    auto sub_res = s.get_subjects(pps::include_deleted::no).get();
    BOOST_REQUIRE_EQUAL(sub_res.size(), 0);
}
