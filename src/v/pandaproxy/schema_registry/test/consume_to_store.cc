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
#include "pandaproxy/schema_registry/seq_writer.h"
#include "pandaproxy/schema_registry/sharded_store.h"
#include "pandaproxy/schema_registry/storage.h"
#include "pandaproxy/schema_registry/test/utils.h"

#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

namespace pps = pandaproxy::schema_registry;

const auto subject0 = pps::context_subject::unqualified("subject0");
constexpr pps::topic_key_magic magic0{0};
constexpr pps::topic_key_magic magic1{1};
constexpr pps::topic_key_magic magic2{2};
constexpr pps::schema_version version0{0};
constexpr pps::schema_version version1{1};
constexpr pps::schema_id id0{0};
constexpr pps::schema_id id1{1};

const pps::schema_definition string_def0{
  pps::sanitize_avro_schema_definition(
    {R"({"type":"string"})",
     pps::schema_type::avro,
     {{.name{"ref"}, .sub{subject0}, .version{version0}}},
     {}})
    .value()};
const pps::schema_definition int_def0{
  pps::sanitize_avro_schema_definition(
    {R"({"type": "int"})", pps::schema_type::avro})
    .value()};

inline model::record_batch make_delete_subject_batch(pps::context_subject sub) {
    storage::record_batch_builder rb{
      model::record_batch_type::raft_data, model::offset{0}};

    rb.add_raw_kv(
      to_json_iobuf(
        pps::delete_subject_key{
          .seq{model::offset{0}}, .node{model::node_id{0}}, .sub{sub}}),
      to_json_iobuf(pps::delete_subject_value{.sub{sub}}));
    return std::move(rb).build();
}

inline model::record_batch make_delete_subject_permanently_batch(
  pps::context_subject sub,
  const chunked_vector<pps::schema_version>& versions) {
    storage::record_batch_builder rb{
      model::record_batch_type::raft_data, model::offset{0}};

    std::for_each(versions.cbegin(), versions.cend(), [&](auto version) {
        rb.add_raw_kv(
          to_json_iobuf(
            pps::schema_key{
              .seq{model::offset{0}},
              .node{model::node_id{0}},
              .sub{sub},
              .version{version}}),
          std::nullopt);
    });
    return std::move(rb).build();
}

SEASTAR_THREAD_TEST_CASE(test_consume_to_store) {
    pps::enable_qualified_subjects::set_local(true);
    auto reset_flag = ss::defer(
      [] { pps::enable_qualified_subjects::reset_local(); });

    pps::sharded_store s;
    s.start(pps::is_mutable::yes, ss::default_smp_service_group()).get();
    auto stop_store = ss::defer([&s]() { s.stop().get(); });

    // This transport will not be used by the sequencer
    // (which itself is only instantiated to receive consume_to_store's
    //  offset updates), is just needed for constructor.
    noop_transport dummy_transport;

    ss::sharded<pps::seq_writer> seq;
    seq
      .start(
        model::node_id{0},
        ss::default_smp_service_group(),
        std::ref(dummy_transport),
        std::reference_wrapper(s),
        ss::sharded_parameter(
          [] { return std::make_unique<sequence_state_checker_test>(); }))
      .get();
    auto stop_seq = ss::defer([&seq]() { seq.stop().get(); });

    auto c = pps::consume_to_store(s, seq.local());

    auto sequence = model::offset{0};
    const auto node_id = model::node_id{123};

    auto good_schema_1 = pps::as_record_batch(
      pps::schema_key{sequence, node_id, subject0, version0, magic1},
      pps::schema_value{{subject0, string_def0.share()}, version0, id0});
    BOOST_REQUIRE_NO_THROW(c(good_schema_1.copy()).get());

    auto s_res = s.get_subject_schema(
                    subject0, version0, pps::include_deleted::no)
                   .get();
    BOOST_REQUIRE_EQUAL(s_res.schema.def(), string_def0);

    auto good_schema_ref_1 = pps::as_record_batch(
      pps::schema_key{sequence, node_id, subject0, version1, magic1},
      pps::schema_value{{subject0, string_def0.share()}, version1, id1});
    BOOST_REQUIRE_NO_THROW(c(good_schema_ref_1.copy()).get());

    auto s_ref_res = s.get_subject_schema(
                        subject0, version1, pps::include_deleted::no)
                       .get();
    BOOST_REQUIRE_EQUAL(s_ref_res.schema.def(), string_def0);
    BOOST_REQUIRE_EQUAL(s_ref_res.schema.sub(), subject0);
    BOOST_REQUIRE_EQUAL(s_ref_res.id, id1);
    BOOST_REQUIRE_EQUAL_COLLECTIONS(
      s_ref_res.schema.def().refs().begin(),
      s_ref_res.schema.def().refs().end(),
      string_def0.refs().begin(),
      string_def0.refs().end());

    auto bad_schema_magic = pps::as_record_batch(
      pps::schema_key{sequence, node_id, subject0, version0, magic2},
      pps::schema_value{{subject0, string_def0.share()}, version0, id0});
    BOOST_REQUIRE_THROW(c(bad_schema_magic.copy()).get(), pps::exception);

    BOOST_REQUIRE(
      s.get_compatibility(pps::default_context, pps::default_to_global::yes)
        .get()
      == pps::compatibility_level::backward);
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

template<typename Key>
model::record_batch as_record_batch(Key key) {
    storage::record_batch_builder rb{
      model::record_batch_type::raft_data, model::offset{0}};
    rb.add_raw_kv(to_json_iobuf(std::move(key)), std::nullopt);
    return std::move(rb).build();
}

SEASTAR_THREAD_TEST_CASE(test_consume_to_store_after_compaction) {
    pps::enable_qualified_subjects::set_local(true);
    auto reset_flag = ss::defer(
      [] { pps::enable_qualified_subjects::reset_local(); });

    pps::sharded_store s;
    s.start(pps::is_mutable::no, ss::default_smp_service_group()).get();
    auto stop_store = ss::defer([&s]() { s.stop().get(); });

    // This transport will not be used by the sequencer
    // (which itself is only instantiated to receive consume_to_store's
    //  offset updates), is just needed for constructor.
    noop_transport dummy_transport;

    ss::sharded<pps::seq_writer> seq;
    seq
      .start(
        model::node_id{0},
        ss::default_smp_service_group(),
        std::ref(dummy_transport),
        std::reference_wrapper(s),
        ss::sharded_parameter(
          [] { return std::make_unique<sequence_state_checker_test>(); }))
      .get();
    auto stop_seq = ss::defer([&seq]() { seq.stop().get(); });

    auto c = pps::consume_to_store(s, seq.local());

    auto sequence = model::offset{0};
    const auto node_id = model::node_id{123};

    // Insert the schema at seq 0
    auto good_schema_1 = pps::as_record_batch(
      pps::schema_key{sequence, node_id, subject0, version0, magic1},
      pps::schema_value{{subject0, string_def0.share()}, version0, id0});
    BOOST_REQUIRE_NO_THROW(c(good_schema_1.copy()).get());
    // Roll the segment
    // Soft delete the version (at seq 1)
    // Perm delete the version (at seq 1)
    // Compact that away, so we have a gap
    // Restart
    // Delete seq0, version 0, it now appears as not soft-deleted
    auto perm_delete_schema_1 = as_record_batch(
      pps::schema_key{sequence, node_id, subject0, version0, magic1});
    BOOST_REQUIRE_NO_THROW(c(perm_delete_schema_1.copy()).get());

    BOOST_REQUIRE_EXCEPTION(
      s.get_versions(subject0, pps::include_deleted::yes).get(),
      pps::exception,
      [](pps::exception e) {
          return e.code() == pps::error_code::subject_not_found;
      });
}

SEASTAR_THREAD_TEST_CASE(test_writes_disabled) {
    pps::enable_qualified_subjects::set_local(true);
    auto reset_flag = ss::defer(
      [] { pps::enable_qualified_subjects::reset_local(); });

    pps::sharded_store s;
    s.start(pps::is_mutable::no, ss::default_smp_service_group()).get();
    auto stop_store = ss::defer([&s]() { s.stop().get(); });

    // This transport will not be used by the sequencer
    // (which itself is only instantiated to receive consume_to_store's
    //  offset updates), is just needed for constructor.
    noop_transport dummy_transport;

    ss::sharded<pps::seq_writer> seq;
    seq
      .start(
        model::node_id{0},
        ss::default_smp_service_group(),
        std::ref(dummy_transport),
        std::reference_wrapper(s),
        ss::sharded_parameter([] {
            return std::make_unique<sequence_state_checker_test>(
              pps::sequence_state_checker::writes_disabled_t::yes);
        }))
      .get();
    auto stop_seq = ss::defer([&seq]() { seq.stop().get(); });

    BOOST_REQUIRE_EXCEPTION(
      seq.local()
        .write_mode(
          pps::context_subject{pps::default_context, pps::subject{""}},
          pps::mode::read_only,
          pps::force::no)
        .get(),
      pps::exception,
      [](pps::exception e) {
          return e.code() == pps::error_code::writes_disabled;
      });
}

/// Transport that simulates a write collision during soft delete.
///
/// Every write to the _schemas topic is tagged with the offset the writer
/// expects to land at. If another node writes first, the offset doesn't
/// match — a "write collision" — and the operation retries. Before
/// retrying, read_sync() catches up on the topic, consuming the winning
/// writer's record into the store.
///
/// This transport drives that sequence:
///   1. Initial read_sync(): HWM=1, _loaded_offset already 0 → no-op
///   2. First produce (the delete): returns wrong offset → collision
///   3. Retry read_sync(): HWM=2, consumes the competing delete batch
///   4. Retry attempt: subject already deleted → returns versions including
///      deletes without re-issuing produce
class colliding_transport final : public pps::transport {
public:
    explicit colliding_transport(
      pps::context_subject sub, model::offset collision_offset)
      : _collision_offset(collision_offset) {
        // Build the competing writer's delete batch at the collision offset.
        storage::record_batch_builder rb{
          model::record_batch_type::raft_data, collision_offset};
        rb.add_raw_kv(
          to_json_iobuf(
            pps::delete_subject_key{
              .seq{collision_offset}, .node{model::node_id{99}}, .sub{sub}}),
          to_json_iobuf(pps::delete_subject_value{.sub{sub}}));
        _competing_batch = std::move(rb).build();
    }

    ss::future<> stop() final { return ss::now(); }
    ss::future<cluster::errc> create_topic(
      model::topic_namespace_view,
      int32_t,
      cluster::topic_properties,
      int16_t) final {
        throw std::runtime_error(
          "colliding_transport::create_topic not implemented");
    }

    ss::future<pps::produce_result> produce(model::record_batch) override {
        ++_produce_calls;
        if (_produce_calls == 1) {
            // Collision: return an offset that doesn't match write_at.
            co_return pps::produce_result{
              .base_offset = _collision_offset + model::offset{1}};
        }
        // The retry should never produce — it finds the subject
        // already deleted and returns the version list via
        // include_deleted::yes.
        throw std::runtime_error("unexpected second produce call");
    }

    ss::future<model::offset> get_high_watermark() override {
        if (_produce_calls == 0) {
            // Before collision: only the schema record at offset 0.
            co_return model::offset{1};
        }
        // After collision: schema at 0, competing delete at 1.
        co_return model::offset{2};
    }

    ss::future<> consume_range(
      model::offset start,
      model::offset end,
      ss::noncopyable_function<ss::future<ss::stop_iteration>(
        model::record_batch)> consumer) override {
        if (
          _competing_batch.has_value() && start <= _collision_offset
          && _collision_offset < end) {
            co_await consumer(std::move(*_competing_batch));
            _competing_batch.reset();
        }
    }

    int produce_calls() const { return _produce_calls; }

private:
    std::optional<model::record_batch> _competing_batch;
    model::offset _collision_offset;
    int _produce_calls{0};
};

SEASTAR_THREAD_TEST_CASE(test_delete_subject_write_collision_retry) {
    pps::enable_qualified_subjects::set_local(true);
    auto reset_flag = ss::defer(
      [] { pps::enable_qualified_subjects::reset_local(); });

    // Store setup: insert a schema so there's something to delete.
    pps::sharded_store store;
    store.start(pps::is_mutable::yes, ss::default_smp_service_group()).get();
    auto stop_store = ss::defer([&store]() { store.stop().get(); });

    const auto version = pps::schema_version{1};
    store
      .upsert(
        pps::seq_marker{
          .seq = model::offset{0},
          .node = model::node_id{0},
          .version = version,
          .key_type = pps::seq_marker_key_type::schema},
        pps::subject_schema{subject0, int_def0.share()},
        id0,
        version,
        pps::is_deleted::no)
      .get();

    // The competing delete will land at offset 1 (the next available).
    colliding_transport transport(subject0, model::offset{1});

    ss::sharded<pps::seq_writer> seq;
    seq
      .start(
        model::node_id{0},
        ss::default_smp_service_group(),
        std::ref(transport),
        std::reference_wrapper(store),
        ss::sharded_parameter(
          [] { return std::make_unique<sequence_state_checker_test>(); }))
      .get();
    auto stop_seq = ss::defer([&seq]() { seq.stop().get(); });

    // Advance _loaded_offset past the schema record.
    seq.local().advance_offset(model::offset{0}).get();

    // First produce collides. The retry's read_sync consumes the
    // competing delete batch, finds the subject already soft-deleted,
    // and returns the version list via include_deleted::yes.
    auto versions = seq.local().delete_subject_impermanent(subject0).get();

    BOOST_REQUIRE_EQUAL(versions.size(), 1);
    BOOST_CHECK_EQUAL(versions[0], version);
    BOOST_CHECK_EQUAL(transport.produce_calls(), 1);
}
