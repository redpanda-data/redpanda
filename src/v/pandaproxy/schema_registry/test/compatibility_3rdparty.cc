// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "model/fundamental.h"
#include "model/record.h"
#include "pandaproxy/schema_registry/error.h"
#include "pandaproxy/schema_registry/exceptions.h"
#include "pandaproxy/schema_registry/sharded_store.h"
#include "pandaproxy/schema_registry/storage.h"
#include "pandaproxy/schema_registry/types.h"
#include "pandaproxy/schema_registry/util.h"

#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

#include <stdexcept>
#include <string_view>

namespace pps = pandaproxy::schema_registry;

inline model::record_batch make_record_batch(
  std::string_view key, std::string_view val, model::offset base_offset) {
    storage::record_batch_builder rb{
      model::record_batch_type::raft_data, base_offset};

    iobuf key_buf;
    iobuf val_buf;
    key_buf.append(key.data(), key.size());
    val_buf.append(val.data(), val.size());

    rb.add_raw_kv(std::move(key_buf), std::move(val_buf));
    return std::move(rb).build();
}

constexpr std::string_view config_key_0{
  R"({"keytype":"CONFIG","subject":"subject_0","magic":0})"};
constexpr std::string_view config_value_0{
  R"({"compatibilityLevel":"BACKWARD"})"};

constexpr std::string_view schema_key_0{
  R"({"keytype":"SCHEMA","subject":"subject_0","version":1,"magic":1})"};
constexpr std::string_view schema_value_0{
  R"({"subject":"subject_0","version":1,"id":1,"schema":"{\"type\":\"record\",\"name\":\"init\",\"fields\":[{\"name\":\"inner\",\"type\":[\"string\",\"int\"]}]}","deleted":false})"};

constexpr std::string_view del_sub_key_0{
  R"({"keytype":"DELETE_SUBJECT","subject":"subject_0","magic":0})"};
constexpr std::string_view del_sub_value_0{
  R"({"subject":"subject_0","version":2})"};

SEASTAR_THREAD_TEST_CASE(test_consume_to_store_3rdparty) {
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

    model::offset base_offset{0};
    BOOST_REQUIRE_NO_THROW(
      c(make_record_batch(config_key_0, config_value_0, base_offset++)).get());
    BOOST_REQUIRE(
      s.get_compatibility(pps::subject{"subject_0"}, pps::default_to_global::no)
        .get()
      == pps::compatibility_level::backward);

    BOOST_REQUIRE_NO_THROW(
      c(make_record_batch(schema_key_0, schema_value_0, base_offset++)).get());
    auto schema_1 = s.get_subject_schema(
                       pps::subject{"subject_0"},
                       pps::schema_version{1},
                       pps::include_deleted::no)
                      .get();

    BOOST_REQUIRE_EQUAL(schema_1.id, pps::schema_id{1});
    BOOST_REQUIRE_EQUAL(schema_1.version, pps::schema_version{1});

    BOOST_REQUIRE_NO_THROW(
      c(make_record_batch(del_sub_key_0, del_sub_value_0, base_offset++))
        .get());
    BOOST_REQUIRE_EXCEPTION(
      s.get_versions(pps::subject{"subject_0"}, pps::include_deleted::no).get(),
      pps::exception,
      [](const pps::exception& e) {
          return e.code() == pps::error_code::subject_not_found;
      });
}
