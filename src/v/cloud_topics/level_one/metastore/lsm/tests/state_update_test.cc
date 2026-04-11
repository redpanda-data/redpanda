/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/common/object_id.h"
#include "cloud_topics/level_one/metastore/lsm/state_reader.h"
#include "cloud_topics/level_one/metastore/lsm/state_update.h"
#include "cloud_topics/level_one/metastore/lsm/values.h"
#include "cloud_topics/level_one/metastore/lsm/write_batch_row.h"
#include "cloud_topics/level_one/metastore/state.h"
#include "cloud_topics/level_one/metastore/state_update.h"
#include "lsm/io/memory_persistence.h"
#include "lsm/lsm.h"
#include "model/fundamental.h"
#include "model/timestamp.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

using namespace cloud_topics::l1;
using testing::HasSubstr;

namespace {

struct extent_spec {
    model::topic_id_partition tidp;
    kafka::offset base_offset;
    kafka::offset last_offset;
    size_t filepos;
    size_t len;

    extent_spec& pos(size_t first, size_t last) {
        filepos = first;
        len = last - first + 1;
        return *this;
    }
};

struct object_spec {
    object_id oid;
    std::vector<extent_spec> extents;
};

struct term_spec {
    model::topic_id_partition tidp;

    // Pairs of (term_start_offset, term).
    std::vector<std::pair<int64_t, int64_t>> start_offset_term;
};

struct compact_spec {
    model::topic_id_partition tidp;
    int64_t cleaned_at{1000};
    int64_t epoch{0};

    // (base_offset, last_offset, has_tombstones).
    std::vector<std::tuple<int64_t, int64_t, bool>> cleaned;

    // Pairs of (base_offset, last_offset).
    std::vector<std::pair<int64_t, int64_t>> rm_tombstones;
};
using compact_specs = std::vector<compact_spec>;

extent_spec tp(model::topic_id_partition tidp, int64_t base, int64_t last) {
    return extent_spec{
      .tidp = tidp,
      .base_offset = kafka::offset(base),
      .last_offset = kafka::offset(last),
      .filepos = 0,
      .len = 0,
    };
}

term_spec terms(
  model::topic_id_partition tidp,
  std::vector<std::pair<int64_t, int64_t>> start_offset_term) {
    return term_spec{
      .tidp = tidp,
      .start_offset_term = std::move(start_offset_term),
    };
}
using term_specs = std::vector<term_spec>;

template<typename... Extents>
inline object_spec make_object(object_id oid, Extents... extents) {
    return object_spec{
      .oid = oid,
      .extents = {extents...},
    };
}

template<typename... Objects>
chunked_vector<new_object> make_new_objects(Objects... objects) {
    std::vector<object_spec> obj_specs = {objects...};
    chunked_vector<new_object> new_objects;
    for (const auto& obj_spec : obj_specs) {
        size_t total_size = 0;
        for (const auto& ext : obj_spec.extents) {
            total_size += ext.len;
        }
        new_object obj{
          .oid = obj_spec.oid,
          .footer_pos = 0,
          .object_size = total_size,
          .extent_metas = {},
        };
        for (const auto& ext : obj_spec.extents) {
            new_object::metadata meta{
              .base_offset = ext.base_offset,
              .last_offset = ext.last_offset,
              .max_timestamp = model::timestamp(1000),
              .filepos = ext.filepos,
              .len = ext.len,
            };
            obj.extent_metas[ext.tidp.topic_id][ext.tidp.partition] = meta;
        }
        new_objects.push_back(std::move(obj));
    }
    return new_objects;
}

template<typename... Objects>
add_objects_db_update
make_add_objects_update(const term_specs& terms, Objects... objects) {
    term_state_update_t new_terms;
    for (const auto& ts : terms) {
        auto& tp_terms = new_terms[ts.tidp];
        for (const auto& start_offset_term : ts.start_offset_term) {
            tp_terms.emplace_back(
              term_start{
                .term_id = model::term_id(start_offset_term.second),
                .start_offset = kafka::offset(start_offset_term.first),
              });
        }
    }

    auto new_objects = make_new_objects(objects...);
    return add_objects_db_update{
      .new_objects = std::move(new_objects),
      .new_terms = std::move(new_terms),
    };
}

template<typename... Objects>
replace_objects_db_update
make_replace_objects_update(const compact_specs& cs, Objects... objects) {
    chunked_hash_map<
      model::topic_id,
      chunked_hash_map<model::partition_id, compaction_state_update>>
      compaction_updates;
    for (const auto& c : cs) {
        auto& tp = compaction_updates[c.tidp.topic_id][c.tidp.partition];
        tp.cleaned_at = model::timestamp(c.cleaned_at);
        tp.expected_compaction_epoch = partition_state::compaction_epoch_t(
          c.epoch);
        for (const auto& [base, last, has_tombstones] : c.cleaned) {
            tp.new_cleaned_ranges.emplace_back(
              cloud_topics::l1::compaction_state_update::cleaned_range{
                .base_offset = kafka::offset(base),
                .last_offset = kafka::offset(last),
                .has_tombstones = has_tombstones,
              });
        }
        for (const auto& [base, last] : c.rm_tombstones) {
            tp.removed_tombstones_ranges.insert(
              kafka::offset(base), kafka::offset(last));
        }
    }

    auto new_objects = make_new_objects(objects...);
    return replace_objects_db_update{
      .new_objects = std::move(new_objects),
      .compaction_updates = std::move(compaction_updates),
    };
}

model::topic_id_partition make_tidp(int partition) {
    return model::topic_id_partition(
      model::topic_id(
        uuid_t::from_string("12345678-1234-5678-1234-567812345678")),
      model::partition_id(partition));
}
const auto tidp0 = make_tidp(0);

} // namespace

class StateUpdateTest : public ::testing::Test {
protected:
    void SetUp() override {
        db_ = lsm::database::open(
                {.database_epoch = 0},
                lsm::io::persistence{
                  .data = lsm::io::make_memory_data_persistence(),
                  .metadata = lsm::io::make_memory_metadata_persistence(),
                })
                .get();
    }

    void TearDown() override {
        if (db_) {
            db_->close().get();
        }
    }

    object_id make_oid() { return object_id(uuid_t::create()); }

    state_reader make_reader() {
        auto snap = db_->create_snapshot();
        return state_reader(std::move(snap));
    }

    void
    preregister_new_objects(const chunked_vector<new_object>& new_objects) {
        chunked_vector<object_id> oids;
        for (const auto& o : new_objects) {
            oids.push_back(o.oid);
        }
        preregister_objects(std::move(oids), model::timestamp(1000));
    }

    void apply_add_update(add_objects_db_update& update) {
        preregister_new_objects(update.new_objects);
        auto reader = make_reader();
        chunked_vector<write_batch_row> rows;
        auto result = update.build_rows(reader, rows).get();
        ASSERT_TRUE(result.has_value());

        auto wb = db_->create_write_batch();
        auto seqno = next_seqno();
        for (const auto& row : rows) {
            if (row.value.empty()) {
                wb.remove(row.key, seqno);
            } else {
                wb.put(row.key, row.value.copy(), seqno);
            }
        }
        db_->apply(std::move(wb)).get();
    }

    void apply_replace_update(replace_objects_db_update& update) {
        preregister_new_objects(update.new_objects);
        auto reader = make_reader();
        chunked_vector<write_batch_row> rows;
        auto result = update.build_rows(reader, rows).get();
        ASSERT_TRUE(result.has_value());

        auto seqno = next_seqno();
        auto wb = db_->create_write_batch();
        for (const auto& row : rows) {
            if (row.value.empty()) {
                wb.remove(row.key, seqno);
            } else {
                wb.put(row.key, row.value.copy(), seqno);
            }
        }
        db_->apply(std::move(wb)).get();
    }

    void verify_metadata(
      model::topic_id_partition tidp, kafka::offset start, kafka::offset next) {
        auto reader = make_reader();
        auto res = reader.get_metadata(tidp).get();
        ASSERT_TRUE(res.has_value());
        ASSERT_TRUE(res.value().has_value());
        EXPECT_EQ(res.value()->start_offset, start);
        EXPECT_EQ(res.value()->next_offset, next);
    }

    void verify_metadata_size(model::topic_id_partition tidp, size_t size) {
        auto reader = make_reader();
        auto res = reader.get_metadata(tidp).get();
        ASSERT_TRUE(res.has_value());
        ASSERT_TRUE(res.value().has_value());
        EXPECT_EQ(res.value()->size, size);
    }

    void verify_extent_exists(
      model::topic_id_partition tidp,
      kafka::offset base_offset,
      kafka::offset last_offset) {
        auto reader = make_reader();
        auto res = reader.get_extent_ge(tidp, base_offset).get();
        ASSERT_TRUE(res.has_value());
        ASSERT_TRUE(res.value().has_value());
        EXPECT_EQ(res.value()->base_offset, base_offset);
        EXPECT_EQ(res.value()->last_offset, last_offset);
    }

    void verify_extent_missing(
      model::topic_id_partition tidp, kafka::offset offset) {
        auto reader = make_reader();
        auto res = reader.get_extent_ge(tidp, offset).get();
        ASSERT_TRUE(res.has_value());
        if (res.value().has_value()) {
            // A greater Extent exists, but it should not start at this exact
            // offset.
            EXPECT_NE(res.value()->base_offset, offset);
        }
    }

    void verify_object_exists(
      object_id oid, size_t total_data_size, size_t removed_data_size = 0) {
        auto reader = make_reader();
        auto res = reader.get_object(oid).get();
        ASSERT_TRUE(res.has_value());
        ASSERT_TRUE(res.value().has_value());
        EXPECT_EQ(res.value()->total_data_size, total_data_size);
        EXPECT_EQ(res.value()->removed_data_size, removed_data_size);
    }

    void verify_max_term(
      model::topic_id_partition tidp,
      model::term_id term,
      kafka::offset start_offset) {
        auto reader = make_reader();
        auto res = reader.get_max_term(tidp).get();
        ASSERT_TRUE(res.has_value());
        ASSERT_TRUE(res.value().has_value());
        EXPECT_EQ(res.value()->term_id, term);
        EXPECT_EQ(res.value()->start_offset, start_offset);
    }

    void verify_extent_range_exists(
      model::topic_id_partition tidp,
      kafka::offset base,
      kafka::offset last,
      size_t extent_count) {
        auto reader = make_reader();
        auto res = reader.get_extent_range(tidp, base, last).get();
        ASSERT_TRUE(res.has_value());
        ASSERT_TRUE(res.value().has_value());
        auto rows = res.value()->materialize_rows().get();
        EXPECT_EQ(rows.size(), extent_count);
    }

    // Verifies compaction state: cleaned_ranges and
    // cleaned_ranges_with_tombstones. expected_cleaned_ranges: vector of
    // (base_offset, last_offset) expected_tombstone_ranges: vector of
    // (base_offset, last_offset, timestamp)
    void verify_compaction_state(
      model::topic_id_partition tidp,
      const std::vector<std::pair<int64_t, int64_t>>& expected_cleaned_ranges,
      const std::vector<std::tuple<int64_t, int64_t, int64_t>>&
        expected_tombstone_ranges) {
        auto reader = make_reader();
        auto res = reader.get_compaction_metadata(tidp).get();
        ASSERT_TRUE(res.has_value());
        ASSERT_TRUE(res.value().has_value());

        auto cleaned_vec = res.value()->cleaned_ranges.to_vec();
        ASSERT_EQ(cleaned_vec.size(), expected_cleaned_ranges.size());
        for (size_t i = 0; i < expected_cleaned_ranges.size(); ++i) {
            EXPECT_EQ(
              cleaned_vec[i].base_offset,
              kafka::offset(expected_cleaned_ranges[i].first));
            EXPECT_EQ(
              cleaned_vec[i].last_offset,
              kafka::offset(expected_cleaned_ranges[i].second));
        }

        const auto& tombstones = res.value()->cleaned_ranges_with_tombstones;
        ASSERT_EQ(tombstones.size(), expected_tombstone_ranges.size());
        auto tombstone_it = tombstones.begin();
        for (const auto& [base, last, ts] : expected_tombstone_ranges) {
            EXPECT_EQ(tombstone_it->base_offset, kafka::offset(base));
            EXPECT_EQ(tombstone_it->last_offset, kafka::offset(last));
            EXPECT_EQ(
              tombstone_it->cleaned_with_tombstones_at, model::timestamp(ts));
            ++tombstone_it;
        }
    }

protected:
    lsm::sequence_number next_seqno() {
        auto max_applied_opt = db_->max_applied_seqno();
        if (!max_applied_opt) {
            return lsm::sequence_number(1);
        }
        return lsm::sequence_number{max_applied_opt.value()() + 1};
    }

    template<typename... Objects>
    void add_objects(term_specs terms, Objects... objects) {
        auto db_update = make_add_objects_update(std::move(terms), objects...);
        apply_add_update(db_update);
    }

    template<typename... Objects>
    void replace_objects(compact_specs cs, Objects... objects) {
        auto db_update = make_replace_objects_update(std::move(cs), objects...);
        apply_replace_update(db_update);
    }

    void apply_set_start_offset_update(set_start_offset_db_update& update) {
        auto reader = make_reader();
        chunked_vector<write_batch_row> rows;
        auto result = update.build_rows(reader, rows).get();
        ASSERT_TRUE(result.has_value());

        auto seqno = next_seqno();
        auto wb = db_->create_write_batch();
        for (const auto& row : rows) {
            if (row.value.empty()) {
                wb.remove(row.key, seqno);
            } else {
                wb.put(row.key, row.value.copy(), seqno);
            }
        }
        db_->apply(std::move(wb)).get();
    }

    void
    set_start_offset(model::topic_id_partition tidp, kafka::offset offset) {
        auto update = set_start_offset_db_update{
          .tp = tidp,
          .new_start_offset = offset,
        };
        apply_set_start_offset_update(update);
    }

    void apply_remove_topics_update(remove_topics_db_update& update) {
        auto reader = make_reader();
        chunked_vector<write_batch_row> rows;
        auto result = update.build_rows(reader, rows).get();
        ASSERT_TRUE(result.has_value());

        auto seqno = next_seqno();
        auto wb = db_->create_write_batch();
        for (const auto& row : rows) {
            if (row.value.empty()) {
                wb.remove(row.key, seqno);
            } else {
                wb.put(row.key, row.value.copy(), seqno);
            }
        }
        db_->apply(std::move(wb)).get();
    }

    void remove_topics(chunked_vector<model::topic_id> topics) {
        auto update = remove_topics_db_update{
          .topics = std::move(topics),
        };
        apply_remove_topics_update(update);
    }

    void verify_metadata_missing(model::topic_id_partition tidp) {
        auto reader = make_reader();
        auto res = reader.get_metadata(tidp).get();
        ASSERT_TRUE(res.has_value());
        EXPECT_FALSE(res->has_value());
    }

    void verify_compaction_missing(model::topic_id_partition tidp) {
        auto reader = make_reader();
        auto res = reader.get_compaction_metadata(tidp).get();
        ASSERT_TRUE(res.has_value());
        EXPECT_FALSE(res->has_value());
    }

    void apply_remove_objects_update(remove_objects_db_update& update) {
        auto reader = make_reader();
        chunked_vector<write_batch_row> rows;
        auto result = update.build_rows(rows).get();
        ASSERT_TRUE(result.has_value());

        auto seqno = next_seqno();
        auto wb = db_->create_write_batch();
        for (const auto& row : rows) {
            if (row.value.empty()) {
                wb.remove(row.key, seqno);
            } else {
                wb.put(row.key, row.value.copy(), seqno);
            }
        }
        db_->apply(std::move(wb)).get();
    }

    void remove_objects(chunked_vector<object_id> objects) {
        auto update = remove_objects_db_update{
          .objects = std::move(objects),
        };
        apply_remove_objects_update(update);
    }

    void verify_object_missing(object_id oid) {
        auto reader = make_reader();
        auto res = reader.get_object(oid).get();
        ASSERT_TRUE(res.has_value());
        EXPECT_FALSE(res->has_value());
    }

    void apply_preregister_update(preregister_objects_db_update& update) {
        auto reader = make_reader();
        chunked_vector<write_batch_row> rows;
        auto result = update.build_rows(reader, rows).get();
        ASSERT_TRUE(result.has_value());

        auto seqno = next_seqno();
        auto wb = db_->create_write_batch();
        for (const auto& row : rows) {
            if (row.value.empty()) {
                wb.remove(row.key, seqno);
            } else {
                wb.put(row.key, row.value.copy(), seqno);
            }
        }
        db_->apply(std::move(wb)).get();
    }

    void apply_expire_preregistered_update(
      expire_preregistered_objects_db_update& update) {
        auto reader = make_reader();
        chunked_vector<write_batch_row> rows;
        auto result = update.build_rows(reader, rows).get();
        ASSERT_TRUE(result.has_value());

        auto seqno = next_seqno();
        auto wb = db_->create_write_batch();
        for (const auto& row : rows) {
            if (row.value.empty()) {
                wb.remove(row.key, seqno);
            } else {
                wb.put(row.key, row.value.copy(), seqno);
            }
        }
        db_->apply(std::move(wb)).get();
    }

    void verify_object_preregistered(object_id oid) {
        auto reader = make_reader();
        auto res = reader.get_object(oid).get();
        ASSERT_TRUE(res.has_value());
        ASSERT_TRUE(res.value().has_value());
        EXPECT_TRUE(res.value()->is_preregistration);
    }

    void verify_object_not_preregistered(object_id oid) {
        auto reader = make_reader();
        auto res = reader.get_object(oid).get();
        ASSERT_TRUE(res.has_value());
        ASSERT_TRUE(res.value().has_value());
        EXPECT_FALSE(res.value()->is_preregistration);
    }

    void preregister_objects(
      chunked_vector<object_id> oids, model::timestamp registered_at) {
        auto update = preregister_objects_db_update{
          .object_ids = std::move(oids),
          .registered_at = registered_at,
        };
        apply_preregister_update(update);
    }

    void verify_preregistered_object_exists(object_id oid) {
        auto reader = make_reader();
        auto res = reader.get_object(oid).get();
        ASSERT_TRUE(res.has_value());
        ASSERT_TRUE(res->has_value());
        EXPECT_TRUE(res->value().is_preregistration);
    }

    void verify_preregistered_object_missing(object_id oid) {
        auto reader = make_reader();
        auto res = reader.get_object(oid).get();
        ASSERT_TRUE(res.has_value());
        if (res->has_value()) {
            EXPECT_FALSE(res->value().is_preregistration);
        }
    }

    std::optional<lsm::database> db_;
};

TEST_F(StateUpdateTest, TestAddObjectsOnce) {
    auto oid1 = make_oid();
    auto oid2 = make_oid();
    auto oid3 = make_oid();

    add_objects(
      {terms(tidp0, {{0, 1}, {100, 2}, {200, 3}})},
      make_object(oid1, tp(tidp0, 0, 99).pos(0, 1023)),
      make_object(oid2, tp(tidp0, 100, 199).pos(0, 1023)),
      make_object(oid3, tp(tidp0, 200, 299).pos(0, 1023)));

    verify_metadata(tidp0, kafka::offset(0), kafka::offset(300));
    verify_extent_exists(tidp0, kafka::offset(0), kafka::offset(99));
    verify_extent_exists(tidp0, kafka::offset(100), kafka::offset(199));
    verify_extent_exists(tidp0, kafka::offset(200), kafka::offset(299));
    verify_object_exists(oid1, 1024);
    verify_object_exists(oid2, 1024);
    verify_object_exists(oid3, 1024);
    verify_max_term(tidp0, model::term_id(3), kafka::offset(200));
}

TEST_F(StateUpdateTest, TestAddObjectsAgain) {
    add_objects(
      {terms(tidp0, {{0, 1}})},
      make_object(make_oid(), tp(tidp0, 0, 299).pos(0, 3071)));

    // Add objects after already adding objects.
    auto oid1 = make_oid();
    auto oid2 = make_oid();
    add_objects(
      {terms(tidp0, {{300, 1}, {350, 2}})},
      make_object(oid1, tp(tidp0, 300, 399).pos(0, 1023)),
      make_object(oid2, tp(tidp0, 400, 499).pos(0, 1023)));

    verify_metadata(tidp0, kafka::offset(0), kafka::offset(500));
    verify_extent_exists(tidp0, kafka::offset(0), kafka::offset(299));
    verify_extent_exists(tidp0, kafka::offset(300), kafka::offset(399));
    verify_extent_exists(tidp0, kafka::offset(400), kafka::offset(499));
    verify_object_exists(oid1, 1024);
    verify_object_exists(oid2, 1024);
    verify_max_term(tidp0, model::term_id(2), kafka::offset(350));
}

TEST_F(StateUpdateTest, TestAddObjectsRejectsDuplicateObject) {
    auto oid = make_oid();
    add_objects(
      {terms(tidp0, {{0, 0}})},
      make_object(oid, tp(tidp0, 0, 99).pos(0, 1023)));

    auto dupe_update = make_add_objects_update(
      {terms(tidp0, {{0, 0}})},
      make_object(oid, tp(tidp0, 0, 99).pos(0, 1023)));
    auto reader = make_reader();
    chunked_vector<write_batch_row> rows;
    auto result = dupe_update.build_rows(reader, rows).get();
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().e, db_update_errc::invalid_update);
    EXPECT_THAT(
      fmt::format("{}", result.error()), HasSubstr("not pre-registered"));
}

TEST_F(StateUpdateTest, TestAddObjectsRejectsEmptyObjects) {
    auto update = make_add_objects_update({terms(tidp0, {{0, 0}})});
    auto validate_res = update.validate_inputs();
    ASSERT_FALSE(validate_res.has_value());
    EXPECT_EQ(validate_res.error().e, db_update_errc::invalid_input);
    EXPECT_THAT(
      fmt::format("{}", validate_res.error()),
      HasSubstr("No objects requested"));
}

TEST_F(StateUpdateTest, TestAddObjectsRejectsMissingTerms) {
    auto db_update = make_add_objects_update(
      term_specs{}, make_object(make_oid(), tp(tidp0, 0, 99).pos(0, 1023)));
    auto validate_res = db_update.validate_inputs();
    ASSERT_FALSE(validate_res.has_value());
    EXPECT_EQ(validate_res.error().e, db_update_errc::invalid_input);
    EXPECT_THAT(
      fmt::format("{}", validate_res.error()),
      HasSubstr("Missing term info in request"));
}

TEST_F(StateUpdateTest, TestAddObjectsRejectsNonContiguousExtents) {
    // Missing [100, 199].
    auto db_update = make_add_objects_update(
      {terms(tidp0, {{0, 1}})},
      make_object(make_oid(), tp(tidp0, 0, 99).pos(0, 1023)),
      make_object(make_oid(), tp(tidp0, 200, 299).pos(0, 1023)));
    auto validate_res = db_update.validate_inputs();
    ASSERT_FALSE(validate_res.has_value());
    EXPECT_EQ(validate_res.error().e, db_update_errc::invalid_input);
    EXPECT_THAT(
      fmt::format("{}", validate_res.error()), HasSubstr("offset ordering"));
}

TEST_F(StateUpdateTest, TestAddObjectsRejectsExtentTermMismatch) {
    // Extents start at offset 0, but terms start at offset 1.
    auto db_update = make_add_objects_update(
      {terms(tidp0, {{1, 1}})},
      make_object(make_oid(), tp(tidp0, 0, 99).pos(0, 1023)));
    auto validate_res = db_update.validate_inputs();
    ASSERT_FALSE(validate_res.has_value());
    EXPECT_EQ(validate_res.error().e, db_update_errc::invalid_input);
    EXPECT_THAT(
      fmt::format("{}", validate_res.error()),
      HasSubstr("Extent start and term start do not match"));
}

TEST_F(StateUpdateTest, TestAddObjectsRejectsExtentsEndBeforeLastTerm) {
    // Extents cover [0-99], but last term starts at offset 100.
    auto db_update = make_add_objects_update(
      {terms(tidp0, {{0, 1}, {100, 2}})},
      make_object(make_oid(), tp(tidp0, 0, 99).pos(0, 1023)));
    auto validate_res = db_update.validate_inputs();
    ASSERT_FALSE(validate_res.has_value());
    EXPECT_EQ(validate_res.error().e, db_update_errc::invalid_input);
    EXPECT_THAT(
      fmt::format("{}", validate_res.error()),
      HasSubstr("Extents end below a requested new term"));
}

TEST_F(StateUpdateTest, TestAddObjectsRejectsDecreasingTerm) {
    // Terms go backwards: term 2 before term 1.
    auto db_update = make_add_objects_update(
      {terms(tidp0, {{0, 2}, {50, 1}})},
      make_object(make_oid(), tp(tidp0, 0, 99).pos(0, 1023)));
    auto validate_res = db_update.validate_inputs();
    ASSERT_FALSE(validate_res.has_value());
    EXPECT_EQ(validate_res.error().e, db_update_errc::invalid_input);
    EXPECT_THAT(
      fmt::format("{}", validate_res.error()), HasSubstr("Invalid term"));
}

TEST_F(StateUpdateTest, TestAddObjectsRejectsDecreasingTermOffset) {
    // Term offsets go backwards.
    auto db_update = make_add_objects_update(
      {terms(tidp0, {{0, 1}, {75, 2}, {50, 3}})},
      make_object(make_oid(), tp(tidp0, 0, 99).pos(0, 1023)));
    auto validate_res = db_update.validate_inputs();
    ASSERT_FALSE(validate_res.has_value());
    EXPECT_EQ(validate_res.error().e, db_update_errc::invalid_input);
    EXPECT_THAT(
      fmt::format("{}", validate_res.error()), HasSubstr("Invalid term"));
}

TEST_F(StateUpdateTest, TestAddObjectsRejectsNonIncreasingTermId) {
    // Term IDs don't strictly increase: term 1 appears twice.
    auto db_update = make_add_objects_update(
      {terms(tidp0, {{0, 1}, {50, 1}})},
      make_object(make_oid(), tp(tidp0, 0, 99).pos(0, 1023)));
    auto validate_res = db_update.validate_inputs();
    ASSERT_FALSE(validate_res.has_value());
    EXPECT_EQ(validate_res.error().e, db_update_errc::invalid_input);
    EXPECT_THAT(
      fmt::format("{}", validate_res.error()), HasSubstr("Invalid term"));
}

TEST_F(StateUpdateTest, TestAddObjectsWithCorrections) {
    // Set up initial partition with offsets [0-99], expected next is 100.
    add_objects(
      {terms(tidp0, {{0, 1}})},
      make_object(make_oid(), tp(tidp0, 0, 99).pos(0, 1023)));

    // Try to add misaligned objects starting at offset 50 instead of 100.
    auto new_oid = make_oid();
    chunked_vector<object_id> prereg_oids;
    prereg_oids.push_back(new_oid);
    preregister_objects(std::move(prereg_oids), model::timestamp(1000));

    auto misaligned_update = make_add_objects_update(
      {terms(tidp0, {{50, 2}})},
      make_object(new_oid, tp(tidp0, 50, 149).pos(0, 1023)));
    auto reader = make_reader();
    chunked_vector<write_batch_row> rows;
    chunked_hash_map<model::topic_id_partition, kafka::offset> corrections;
    auto result
      = misaligned_update.build_rows(reader, rows, &corrections).get();
    ASSERT_TRUE(result.has_value());

    // There should be one row: object entry (all data marked removed).
    EXPECT_EQ(1, rows.size());
    auto wb = db_->create_write_batch();
    auto seqno = next_seqno();
    for (const auto& row : rows) {
        if (row.value.empty()) {
            wb.remove(row.key, seqno);
        } else {
            wb.put(row.key, row.value.copy(), seqno);
        }
    }
    db_->apply(std::move(wb)).get();

    // Corrections should indicate the partition expected offset 100.
    ASSERT_TRUE(corrections.contains(tidp0));
    EXPECT_EQ(corrections[tidp0], kafka::offset(100));

    // Metadata should remain unchanged.
    verify_metadata(tidp0, kafka::offset(0), kafka::offset(100));
    verify_extent_missing(tidp0, kafka::offset(50));
    verify_extent_exists(tidp0, kafka::offset(0), kafka::offset(99));

    // The misaligned object should be in the metastore with all data marked
    // as removed.
    verify_object_exists(new_oid, 1024, 1024);
}

TEST_F(StateUpdateTest, TestReplaceObjectsBasic) {
    auto old_oid = make_oid();
    add_objects(
      {terms(tidp0, {{0, 1}})},
      make_object(old_oid, tp(tidp0, 0, 99).pos(0, 1023)));
    add_objects(
      {terms(tidp0, {{100, 1}})},
      make_object(make_oid(), tp(tidp0, 100, 199).pos(0, 1023)));
    add_objects(
      {terms(tidp0, {{200, 1}})},
      make_object(make_oid(), tp(tidp0, 200, 299).pos(0, 1023)));

    // Create a new object that replaces the first two extents [0-199].
    auto new_oid = make_oid();
    replace_objects(
      compact_specs{}, make_object(new_oid, tp(tidp0, 0, 199).pos(0, 2047)));

    // Metadata should be unchanged.
    verify_metadata(tidp0, kafka::offset(0), kafka::offset(300));

    // Old extents [0-99] and [100-199] should be gone. New extent [0-199]
    // should exist.
    verify_extent_exists(tidp0, kafka::offset(0), kafka::offset(199));
    verify_extent_missing(tidp0, kafka::offset(100));
    verify_extent_exists(tidp0, kafka::offset(200), kafka::offset(299));

    verify_object_exists(new_oid, 2048);
    verify_object_exists(old_oid, 1024, /*expected_removed_data_size*/ 1024);
}

TEST_F(StateUpdateTest, TestReplaceObjectsRejectsMissingPartition) {
    auto oid = make_oid();
    chunked_vector<object_id> prereg_oids;
    prereg_oids.push_back(oid);
    preregister_objects(std::move(prereg_oids), model::timestamp(1000));

    auto db_update = make_replace_objects_update(
      compact_specs{}, make_object(oid, tp(tidp0, 0, 99).pos(0, 1023)));
    auto reader = make_reader();
    chunked_vector<write_batch_row> rows;
    auto result = db_update.build_rows(reader, rows).get();
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().e, db_update_errc::invalid_update);
    EXPECT_THAT(
      fmt::format("{}", result.error()), HasSubstr("not tracked by state"));
}

TEST_F(StateUpdateTest, TestReplaceObjectsRejectsEmptyObjects) {
    replace_objects_db_update update{
      .new_objects = {},
      .compaction_updates = {},
    };
    auto validate_res = update.validate_inputs();
    ASSERT_FALSE(validate_res.has_value());
    EXPECT_EQ(validate_res.error().e, db_update_errc::invalid_input);
    EXPECT_THAT(
      fmt::format("{}", validate_res.error()),
      HasSubstr("No objects requested"));
}

TEST_F(
  StateUpdateTest, TestReplaceObjectsRejectsCompactionUpdateWithoutExtents) {
    auto tidp1 = make_tidp(1);

    // Compaction update for tidp1, but extents only for tidp0.
    auto db_update = make_replace_objects_update(
      {{compact_spec{
        .tidp = tidp1,
        .cleaned = {{0, 99, false}},
      }}},
      make_object(make_oid(), tp(tidp0, 0, 99).pos(0, 1023)));

    auto validate_res = db_update.validate_inputs();
    ASSERT_FALSE(validate_res.has_value());
    EXPECT_EQ(validate_res.error().e, db_update_errc::invalid_input);
    EXPECT_THAT(
      fmt::format("{}", validate_res.error()),
      HasSubstr("does not refer to partition with extent"));
}

TEST_F(
  StateUpdateTest, TestReplaceObjectsRejectsCleanedRangeExceedsExtentsEnd) {
    // Cleaned range [0-299], but extents only [0-199].
    auto db_update = make_replace_objects_update(
      {{compact_spec{
        .tidp = tidp0,
        .cleaned = {{0, 299, false}},
      }}},
      make_object(make_oid(), tp(tidp0, 0, 199).pos(0, 2047)));

    auto validate_res = db_update.validate_inputs();
    ASSERT_FALSE(validate_res.has_value());
    EXPECT_EQ(validate_res.error().e, db_update_errc::invalid_input);
    EXPECT_THAT(
      fmt::format("{}", validate_res.error()),
      HasSubstr("does not match requested new extents' last_offset"));
}

TEST_F(
  StateUpdateTest, TestReplaceObjectsRejectsCleanedRangeStartsBeforeExtents) {
    // Extents start at 100, but cleaned range starts at 50.
    auto db_update = make_replace_objects_update(
      {{compact_spec{
        .tidp = tidp0,
        .cleaned = {{50, 199, false}},
      }}},
      make_object(make_oid(), tp(tidp0, 100, 199).pos(0, 1023)));

    auto validate_res = db_update.validate_inputs();
    ASSERT_FALSE(validate_res.has_value());
    EXPECT_EQ(validate_res.error().e, db_update_errc::invalid_input);
    EXPECT_THAT(
      fmt::format("{}", validate_res.error()),
      HasSubstr("is not covered by extents"));
}

TEST_F(StateUpdateTest, TestReplaceObjectsRejectsDuplicateObject) {
    auto oid = make_oid();
    add_objects(
      {terms(tidp0, {{0, 1}})},
      make_object(oid, tp(tidp0, 0, 99).pos(0, 1023)));

    // Try to replace using an object ID that already exists.
    auto db_update = make_replace_objects_update(
      compact_specs{}, make_object(oid, tp(tidp0, 0, 99).pos(0, 1023)));
    auto reader = make_reader();
    chunked_vector<write_batch_row> rows;
    auto result = db_update.build_rows(reader, rows).get();
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().e, db_update_errc::invalid_update);
    EXPECT_THAT(
      fmt::format("{}", result.error()), HasSubstr("not pre-registered"));
}

TEST_F(StateUpdateTest, TestReplaceObjectsRejectsOverlappingCleanedRanges) {
    // Set up partition with existing cleaned range with tombstones.
    add_objects(
      {terms(tidp0, {{0, 1}})},
      make_object(make_oid(), tp(tidp0, 0, 99).pos(0, 1023)));

    // Add a cleaned range [0-49].
    replace_objects(
      {{compact_spec{
        .tidp = tidp0,
        .cleaned = {{0, 49, true}},
      }}},
      make_object(make_oid(), tp(tidp0, 0, 99).pos(0, 511)));
    verify_compaction_state(
      tidp0,
      /*expected_cleaned_ranges=*/{{0, 49}},
      /*expected_tombstone_ranges=*/{{0, 49, 1000}});

    // Now try to add overlapping cleaned range [49-99].
    // epoch=1 because the first compaction incremented it from 0 to 1.
    auto overlap_oid = make_oid();
    chunked_vector<object_id> prereg_oids;
    prereg_oids.push_back(overlap_oid);
    preregister_objects(std::move(prereg_oids), model::timestamp(1000));

    auto db_update = make_replace_objects_update(
      {{compact_spec{
        .tidp = tidp0,
        .cleaned_at = 2000,
        .epoch = 1,
        .cleaned = {{49, 99, true}},
      }}},
      make_object(overlap_oid, tp(tidp0, 0, 99).pos(0, 511)));

    auto reader = make_reader();
    chunked_vector<write_batch_row> rows;
    auto result = db_update.build_rows(reader, rows).get();
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().e, db_update_errc::invalid_update);
    EXPECT_THAT(
      fmt::format("{}", result.error()),
      HasSubstr("overlaps with an existing cleaned range"));
}

TEST_F(StateUpdateTest, TestReplaceObjectsRejectsRemovingUntrackedTombstones) {
    // Set up partition without any tombstone tracking.
    add_objects(
      {terms(tidp0, {{0, 1}})},
      make_object(make_oid(), tp(tidp0, 0, 99).pos(0, 1023)));

    // Try to remove tombstones from range [0-49] which doesn't have them.
    auto tomb_oid = make_oid();
    chunked_vector<object_id> prereg_oids;
    prereg_oids.push_back(tomb_oid);
    preregister_objects(std::move(prereg_oids), model::timestamp(1000));

    auto db_update = make_replace_objects_update(
      {{compact_spec{
        .tidp = tidp0,
        .rm_tombstones = {{0, 49}},
      }}},
      make_object(tomb_oid, tp(tidp0, 0, 99).pos(0, 1023)));

    auto reader = make_reader();
    chunked_vector<write_batch_row> rows;
    auto result = db_update.build_rows(reader, rows).get();
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().e, db_update_errc::invalid_update);
    EXPECT_THAT(
      fmt::format("{}", result.error()),
      HasSubstr("is not tracked as having tombstones"));
}

TEST_F(StateUpdateTest, TestReplaceObjectsWithCompactionAndTombstones) {
    // Set up initial partition.
    add_objects(
      {terms(tidp0, {{0, 1}})},
      make_object(make_oid(), tp(tidp0, 0, 99).pos(0, 2047)),
      make_object(make_oid(), tp(tidp0, 100, 199).pos(0, 2047)));

    // First replace with cleaned range with tombstones [0-99].
    auto new_oid1 = make_oid();
    replace_objects(
      {{compact_spec{
        .tidp = tidp0,
        .cleaned_at = 1000,
        .cleaned = {{0, 99, true}},
      }}},
      make_object(new_oid1, tp(tidp0, 0, 99).pos(0, 1023)));

    verify_metadata(tidp0, kafka::offset(0), kafka::offset(200));
    verify_extent_exists(tidp0, kafka::offset(0), kafka::offset(99));
    verify_extent_exists(tidp0, kafka::offset(100), kafka::offset(199));
    verify_object_exists(new_oid1, 1024);
    verify_compaction_state(
      tidp0,
      /*expected_cleaned_ranges=*/{{0, 99}},
      /*expected_tombstone_ranges=*/{{0, 99, 1000}});

    // Second replace: add non-overlapping cleaned range with tombstones
    // [150-199], and remove tombstones from [0-49].
    // epoch=1 because the first compaction incremented it from 0 to 1.
    auto new_oid2 = make_oid();
    replace_objects(
      {{compact_spec{
        .tidp = tidp0,
        .cleaned_at = 2000,
        .epoch = 1,
        .cleaned = {{150, 199, true}},
        .rm_tombstones = {{0, 49}},
      }}},
      make_object(new_oid2, tp(tidp0, 0, 199).pos(0, 1023)));

    verify_metadata(tidp0, kafka::offset(0), kafka::offset(200));
    verify_extent_exists(tidp0, kafka::offset(0), kafka::offset(199));
    verify_extent_missing(tidp0, kafka::offset(100));
    verify_object_exists(new_oid2, 1024);
    verify_compaction_state(
      tidp0,
      /*expected_cleaned_ranges=*/{{0, 99}, {150, 199}},
      /*expected_tombstone_ranges=*/{{50, 99, 1000}, {150, 199, 2000}});
}

TEST_F(StateUpdateTest, TestReplaceObjectsRejectsCleanedRangeNotAtLogStart) {
    add_objects(
      {terms(tidp0, {{0, 1}})},
      make_object(make_oid(), tp(tidp0, 0, 99).pos(0, 1023)),
      make_object(make_oid(), tp(tidp0, 100, 199).pos(0, 1023)));

    verify_metadata(tidp0, kafka::offset(0), kafka::offset(200));

    // Try to replace with cleaned range [100-199], but without replacing down
    // to offset 0. This should be rejected because cleaning requires
    // replacing from the start of the log.
    auto partial_oid = make_oid();
    chunked_vector<object_id> prereg_oids;
    prereg_oids.push_back(partial_oid);
    preregister_objects(std::move(prereg_oids), model::timestamp(1000));

    auto db_update = make_replace_objects_update(
      {{compact_spec{
        .tidp = tidp0,
        .cleaned = {{100, 199, false}},
      }}},
      make_object(partial_oid, tp(tidp0, 100, 199).pos(0, 1023)));

    auto reader = make_reader();
    chunked_vector<write_batch_row> rows;
    auto result = db_update.build_rows(reader, rows).get();
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().e, db_update_errc::invalid_update);
    EXPECT_THAT(
      fmt::format("{}", result.error()),
      HasSubstr("does not replace to the beginning of the log"));

    // Now validate that replacing down to 0 works.
    replace_objects(
      {{compact_spec{
        .tidp = tidp0,
        .cleaned = {{100, 199, false}},
      }}},
      make_object(make_oid(), tp(tidp0, 0, 199).pos(0, 1023)));
    verify_compaction_state(
      tidp0,
      /*expected_cleaned_ranges=*/{{100, 199}},
      /*expected_tombstone_ranges=*/{});
}

TEST_F(StateUpdateTest, TestSetStartOffsetBasic) {
    // Set up partition with three extents.
    auto oid1 = make_oid();
    auto oid2 = make_oid();
    auto oid3 = make_oid();
    add_objects(
      {terms(tidp0, {{0, 1}})},
      make_object(oid1, tp(tidp0, 0, 99).pos(0, 1023)),
      make_object(oid2, tp(tidp0, 100, 199).pos(0, 1023)),
      make_object(oid3, tp(tidp0, 200, 299).pos(0, 1023)));

    verify_metadata(tidp0, kafka::offset(0), kafka::offset(300));

    // Set start offset to 200, which should remove the first two extents.
    set_start_offset(tidp0, kafka::offset(200));

    // Verify metadata is updated.
    verify_metadata(tidp0, kafka::offset(200), kafka::offset(300));

    // Verify first two extents are removed.
    verify_extent_missing(tidp0, kafka::offset(0));
    verify_extent_missing(tidp0, kafka::offset(100));

    // Verify third extent still exists.
    verify_extent_exists(tidp0, kafka::offset(200), kafka::offset(299));

    // Verify object entries have updated removed_data_size.
    verify_object_exists(oid1, 1024, /*removed_data_size=*/1024);
    verify_object_exists(oid2, 1024, /*removed_data_size=*/1024);
    verify_object_exists(oid3, 1024, /*removed_data_size=*/0);
}

TEST_F(StateUpdateTest, TestSetStartOffsetNoOp) {
    // Set up partition.
    auto oid = make_oid();
    add_objects(
      {terms(tidp0, {{0, 1}})},
      make_object(oid, tp(tidp0, 0, 99).pos(0, 1023)));

    verify_metadata(tidp0, kafka::offset(0), kafka::offset(100));

    // Set start offset to 0 (same as current), should be no-op.
    set_start_offset(tidp0, kafka::offset(0));

    // Verify nothing changed.
    verify_metadata(tidp0, kafka::offset(0), kafka::offset(100));
    verify_extent_exists(tidp0, kafka::offset(0), kafka::offset(99));
    verify_object_exists(oid, 1024, /*removed_data_size=*/0);
}

TEST_F(StateUpdateTest, TestSetStartOffsetPartialRemoval) {
    // Set up partition with three extents.
    auto oid1 = make_oid();
    auto oid2 = make_oid();
    auto oid3 = make_oid();
    add_objects(
      {terms(tidp0, {{0, 1}})},
      make_object(oid1, tp(tidp0, 0, 99).pos(0, 1023)),
      make_object(oid2, tp(tidp0, 100, 199).pos(0, 1023)),
      make_object(oid3, tp(tidp0, 200, 299).pos(0, 1023)));

    // Set start offset to 100, which should only remove the first extent.
    set_start_offset(tidp0, kafka::offset(100));

    verify_metadata(tidp0, kafka::offset(100), kafka::offset(300));
    verify_extent_missing(tidp0, kafka::offset(0));
    verify_extent_exists(tidp0, kafka::offset(100), kafka::offset(199));
    verify_extent_exists(tidp0, kafka::offset(200), kafka::offset(299));
    verify_object_exists(oid1, 1024, /*removed_data_size=*/1024);
    verify_object_exists(oid2, 1024, /*removed_data_size=*/0);
    verify_object_exists(oid3, 1024, /*removed_data_size=*/0);
}

TEST_F(StateUpdateTest, TestSetStartOffsetWithinExtent) {
    // Set up partition with extent [0-99].
    auto oid = make_oid();
    add_objects(
      {terms(tidp0, {{0, 1}})},
      make_object(oid, tp(tidp0, 0, 99).pos(0, 1023)));

    // Set start offset to 50 (within the extent).
    // The extent [0-99] should NOT be deleted since its last_offset >= 50.
    set_start_offset(tidp0, kafka::offset(50));

    verify_metadata(tidp0, kafka::offset(50), kafka::offset(100));
    // The extent should still exist since last_offset (99) >= new_start (50).
    verify_extent_exists(tidp0, kafka::offset(0), kafka::offset(99));
    // No data removed since extent wasn't fully deleted.
    verify_object_exists(oid, 1024, /*removed_data_size=*/0);
}

TEST_F(StateUpdateTest, TestSetStartOffsetBelowStartIsNoOp) {
    // Set up partition with start_offset=50.
    add_objects(
      {terms(tidp0, {{0, 1}})},
      make_object(make_oid(), tp(tidp0, 0, 99).pos(0, 1023)));
    set_start_offset(tidp0, kafka::offset(50));

    verify_metadata(tidp0, kafka::offset(50), kafka::offset(100));

    // Try to set start offset below current start.
    auto update = set_start_offset_db_update{
      .tp = tidp0,
      .new_start_offset = kafka::offset(25),
    };
    auto reader = make_reader();
    chunked_vector<write_batch_row> rows;
    bool is_no_op = false;
    auto result = update.build_rows(reader, rows, &is_no_op).get();
    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(is_no_op);
    EXPECT_TRUE(rows.empty());
}

TEST_F(StateUpdateTest, TestSetStartOffsetRejectsAboveNext) {
    // Set up partition with next_offset=100.
    add_objects(
      {terms(tidp0, {{0, 1}})},
      make_object(make_oid(), tp(tidp0, 0, 99).pos(0, 1023)));

    verify_metadata(tidp0, kafka::offset(0), kafka::offset(100));

    // Try to set start offset above next_offset.
    auto update = set_start_offset_db_update{
      .tp = tidp0,
      .new_start_offset = kafka::offset(150),
    };
    auto reader = make_reader();
    chunked_vector<write_batch_row> rows;
    auto result = update.build_rows(reader, rows).get();
    ASSERT_FALSE(result.has_value());
}

TEST_F(StateUpdateTest, TestSetStartOffsetRejectsMissingPartition) {
    // Try to set start offset on a partition that doesn't exist.
    auto update = set_start_offset_db_update{
      .tp = tidp0,
      .new_start_offset = kafka::offset(50),
    };
    auto reader = make_reader();
    chunked_vector<write_batch_row> rows;
    auto result = update.build_rows(reader, rows).get();
    ASSERT_FALSE(result.has_value());
}

TEST_F(StateUpdateTest, TestSetStartOffsetRemovesAllExtents) {
    // Set up partition with extents [0-99].
    auto oid = make_oid();
    add_objects(
      {terms(tidp0, {{0, 1}})},
      make_object(oid, tp(tidp0, 0, 99).pos(0, 1023)));

    // Set start offset to next_offset, which removes all extents.
    set_start_offset(tidp0, kafka::offset(100));

    verify_metadata(tidp0, kafka::offset(100), kafka::offset(100));
    verify_extent_missing(tidp0, kafka::offset(0));
    verify_object_exists(oid, 1024, /*removed_data_size=*/1024);
}

TEST_F(StateUpdateTest, TestSetStartOffsetTruncatesCompactionState) {
    auto oid1 = make_oid();
    add_objects(
      {terms(tidp0, {{0, 1}})},
      make_object(oid1, tp(tidp0, 0, 99).pos(0, 1023)));

    replace_objects(
      {{compact_spec{
        .tidp = tidp0,
        .cleaned = {{20, 80, true}},
      }}},
      make_object(make_oid(), tp(tidp0, 0, 99).pos(0, 511)));

    verify_compaction_state(
      tidp0,
      /*expected_cleaned_ranges=*/{{20, 80}},
      /*expected_tombstone_ranges=*/{{20, 80, 1000}});

    // Set start offset to 50, which should truncate compaction ranges.
    set_start_offset(tidp0, kafka::offset(50));
    verify_metadata(tidp0, kafka::offset(50), kafka::offset(100));
    verify_compaction_state(
      tidp0,
      /*expected_cleaned_ranges=*/{{50, 80}},
      /*expected_tombstone_ranges=*/{{50, 80, 1000}});
}

TEST_F(StateUpdateTest, TestRemoveTopicsBasic) {
    // Set up partition with extents.
    auto oid1 = make_oid();
    auto oid2 = make_oid();
    add_objects(
      {terms(tidp0, {{0, 1}})},
      make_object(oid1, tp(tidp0, 0, 99).pos(0, 1023)),
      make_object(oid2, tp(tidp0, 100, 199).pos(0, 1023)));

    verify_metadata(tidp0, kafka::offset(0), kafka::offset(200));
    verify_extent_exists(tidp0, kafka::offset(0), kafka::offset(99));
    verify_extent_exists(tidp0, kafka::offset(100), kafka::offset(199));

    // Remove the topic.
    chunked_vector<model::topic_id> topics_to_remove;
    topics_to_remove.push_back(tidp0.topic_id);
    remove_topics(std::move(topics_to_remove));

    // Verify metadata, extents, and terms are removed.
    verify_metadata_missing(tidp0);
    verify_extent_missing(tidp0, kafka::offset(0));
    verify_extent_missing(tidp0, kafka::offset(100));
    verify_compaction_missing(tidp0);

    // Verify objects have updated removed_data_size.
    verify_object_exists(oid1, 1024, /*removed_data_size=*/1024);
    verify_object_exists(oid2, 1024, /*removed_data_size=*/1024);
}

TEST_F(StateUpdateTest, TestRemoveTopicsMultiplePartitions) {
    // Set up two partitions for the same topic.
    auto tidp1 = make_tidp(1);
    auto oid1 = make_oid();
    auto oid2 = make_oid();
    add_objects(
      {terms(tidp0, {{0, 1}})},
      make_object(oid1, tp(tidp0, 0, 99).pos(0, 1023)));
    add_objects(
      {terms(tidp1, {{0, 1}})},
      make_object(oid2, tp(tidp1, 0, 99).pos(0, 1023)));

    verify_metadata(tidp0, kafka::offset(0), kafka::offset(100));
    verify_metadata(tidp1, kafka::offset(0), kafka::offset(100));

    // Remove the topic (should remove both partitions).
    chunked_vector<model::topic_id> topics_to_remove;
    topics_to_remove.push_back(tidp0.topic_id);
    remove_topics(std::move(topics_to_remove));

    // Both partitions should be removed.
    verify_metadata_missing(tidp0);
    verify_metadata_missing(tidp1);
    verify_extent_missing(tidp0, kafka::offset(0));
    verify_extent_missing(tidp1, kafka::offset(0));

    // Both objects should have updated removed_data_size.
    verify_object_exists(oid1, 1024, /*removed_data_size=*/1024);
    verify_object_exists(oid2, 1024, /*removed_data_size=*/1024);
}

TEST_F(StateUpdateTest, TestRemoveTopicsWithCompaction) {
    // Set up partition with compaction state.
    auto oid = make_oid();
    add_objects(
      {terms(tidp0, {{0, 1}})},
      make_object(oid, tp(tidp0, 0, 99).pos(0, 1023)));

    // Add a cleaned range.
    replace_objects(
      {{compact_spec{
        .tidp = tidp0,
        .cleaned = {{0, 49, true}},
      }}},
      make_object(make_oid(), tp(tidp0, 0, 99).pos(0, 511)));

    verify_compaction_state(
      tidp0,
      /*expected_cleaned_ranges=*/{{0, 49}},
      /*expected_tombstone_ranges=*/{{0, 49, 1000}});

    // Remove the topic.
    chunked_vector<model::topic_id> topics_to_remove;
    topics_to_remove.push_back(tidp0.topic_id);
    remove_topics(std::move(topics_to_remove));

    // Verify all state is removed.
    verify_metadata_missing(tidp0);
    verify_extent_missing(tidp0, kafka::offset(0));
    verify_compaction_missing(tidp0);
}

TEST_F(StateUpdateTest, TestRemoveTopicsEmpty) {
    // Removing empty topics list should be no-op.
    chunked_vector<model::topic_id> topics_to_remove;
    remove_topics(std::move(topics_to_remove));
    // No crash, no side effects.
}

TEST_F(StateUpdateTest, TestRemoveTopicsNonExistent) {
    // Set up a partition.
    auto oid = make_oid();
    add_objects(
      {terms(tidp0, {{0, 1}})},
      make_object(oid, tp(tidp0, 0, 99).pos(0, 1023)));

    // Try to remove a different topic.
    auto other_topic = model::topic_id(
      uuid_t::from_string("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"));
    chunked_vector<model::topic_id> topics_to_remove;
    topics_to_remove.push_back(other_topic);
    remove_topics(std::move(topics_to_remove));

    // Original topic should still exist.
    verify_metadata(tidp0, kafka::offset(0), kafka::offset(100));
    verify_extent_exists(tidp0, kafka::offset(0), kafka::offset(99));
    verify_object_exists(oid, 1024, /*removed_data_size=*/0);
}

TEST_F(StateUpdateTest, TestRemoveTopicsMultipleTopics) {
    // Set up two different topics.
    auto topic2 = model::topic_id(
      uuid_t::from_string("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"));
    auto tidp_t2 = model::topic_id_partition(topic2, model::partition_id(0));
    auto oid1 = make_oid();
    auto oid2 = make_oid();

    add_objects(
      {terms(tidp0, {{0, 1}})},
      make_object(oid1, tp(tidp0, 0, 99).pos(0, 1023)));
    add_objects(
      {terms(tidp_t2, {{0, 1}})},
      make_object(oid2, tp(tidp_t2, 0, 99).pos(0, 1023)));

    verify_metadata(tidp0, kafka::offset(0), kafka::offset(100));
    verify_metadata(tidp_t2, kafka::offset(0), kafka::offset(100));

    // Remove both topics.
    chunked_vector<model::topic_id> topics_to_remove;
    topics_to_remove.push_back(tidp0.topic_id);
    topics_to_remove.push_back(topic2);
    remove_topics(std::move(topics_to_remove));

    // Both should be removed.
    verify_metadata_missing(tidp0);
    verify_metadata_missing(tidp_t2);
    verify_object_exists(oid1, 1024, /*removed_data_size=*/1024);
    verify_object_exists(oid2, 1024, /*removed_data_size=*/1024);
}

TEST_F(StateUpdateTest, TestRemoveObjectsFullyRemoved) {
    // Set up partition and then remove all extents via set_start_offset.
    auto oid = make_oid();
    add_objects(
      {terms(tidp0, {{0, 1}})},
      make_object(oid, tp(tidp0, 0, 99).pos(0, 1023)));

    verify_object_exists(oid, 1024, /*removed_data_size=*/0);

    // Remove all extents by setting start_offset to next_offset.
    set_start_offset(tidp0, kafka::offset(100));
    verify_object_exists(oid, 1024, /*removed_data_size=*/1024);

    // Now remove the object.
    chunked_vector<object_id> objects_to_remove;
    objects_to_remove.push_back(oid);
    remove_objects(std::move(objects_to_remove));

    // Object should be removed.
    verify_object_missing(oid);
}

TEST_F(StateUpdateTest, TestRemoveObjectsEmpty) {
    // Removing empty objects list should be no-op.
    chunked_vector<object_id> objects_to_remove;
    remove_objects(std::move(objects_to_remove));
    // No crash, no side effects.
}

TEST_F(StateUpdateTest, TestRemoveObjectsNonExistent) {
    // Set up a partition with an object.
    auto oid = make_oid();
    add_objects(
      {terms(tidp0, {{0, 1}})},
      make_object(oid, tp(tidp0, 0, 99).pos(0, 1023)));

    // Try to remove a non-existent object.
    auto other_oid = make_oid();
    chunked_vector<object_id> objects_to_remove;
    objects_to_remove.push_back(other_oid);
    remove_objects(std::move(objects_to_remove));

    // Original object should still exist.
    verify_object_exists(oid, 1024, /*removed_data_size=*/0);
}

TEST_F(StateUpdateTest, TestAddObjectsTracksSizeBasic) {
    auto oid1 = make_oid();
    auto oid2 = make_oid();
    auto oid3 = make_oid();

    // Add three extents with different sizes.
    add_objects(
      {terms(tidp0, {{0, 1}, {100, 2}, {200, 3}})},
      make_object(oid1, tp(tidp0, 0, 99).pos(0, 1023)),    // size = 1024
      make_object(oid2, tp(tidp0, 100, 199).pos(0, 2047)), // size = 2048
      make_object(oid3, tp(tidp0, 200, 299).pos(0, 511))); // size = 512

    // Total size should be 1024 + 2048 + 512 = 3584.
    verify_metadata_size(tidp0, 3584);
}

TEST_F(StateUpdateTest, TestAddObjectsTracksSizeIncrementally) {
    // Add first object.
    add_objects(
      {terms(tidp0, {{0, 1}})},
      make_object(make_oid(), tp(tidp0, 0, 99).pos(0, 999))); // size = 1000
    verify_metadata_size(tidp0, 1000);

    // Add more objects - size should accumulate.
    add_objects(
      {terms(tidp0, {{100, 1}})},
      make_object(make_oid(), tp(tidp0, 100, 199).pos(0, 499))); // size = 500
    verify_metadata_size(tidp0, 1500);

    add_objects(
      {terms(tidp0, {{200, 1}})},
      make_object(make_oid(), tp(tidp0, 200, 299).pos(0, 249))); // size = 250
    verify_metadata_size(tidp0, 1750);
}

TEST_F(StateUpdateTest, TestReplaceObjectsTracksSize) {
    auto old_oid1 = make_oid();
    auto old_oid2 = make_oid();

    // Add initial extents.
    add_objects(
      {terms(tidp0, {{0, 1}})},
      make_object(old_oid1, tp(tidp0, 0, 99).pos(0, 1023))); // size = 1024
    add_objects(
      {terms(tidp0, {{100, 1}})},
      make_object(old_oid2, tp(tidp0, 100, 199).pos(0, 2047))); // size = 2048
    verify_metadata_size(tidp0, 3072);

    // Replace both extents with a single smaller extent.
    auto new_oid = make_oid();
    replace_objects(
      compact_specs{}, make_object(new_oid, tp(tidp0, 0, 199).pos(0, 511)));

    // Size should be updated to the new extent size (512).
    verify_metadata_size(tidp0, 512);
}

TEST_F(StateUpdateTest, TestReplaceObjectsTracksSizeMultiplePartitions) {
    auto tidp1 = make_tidp(1);

    // Add extents to two partitions.
    add_objects(
      {terms(tidp0, {{0, 1}}), terms(tidp1, {{0, 1}})},
      make_object(
        make_oid(),
        tp(tidp0, 0, 99).pos(0, 999),       // size = 1000
        tp(tidp1, 0, 99).pos(1000, 1499))); // size = 500

    verify_metadata_size(tidp0, 1000);
    verify_metadata_size(tidp1, 500);

    // Replace only tidp0's extent with a smaller one.
    replace_objects(
      compact_specs{},
      make_object(make_oid(), tp(tidp0, 0, 99).pos(0, 199))); // size = 200

    // tidp0 should have updated size, tidp1 should be unchanged.
    verify_metadata_size(tidp0, 200);
    verify_metadata_size(tidp1, 500);
}

TEST_F(StateUpdateTest, TestSetStartOffsetTracksSize) {
    auto oid1 = make_oid();
    auto oid2 = make_oid();
    auto oid3 = make_oid();

    // Add three extents.
    add_objects(
      {terms(tidp0, {{0, 1}})},
      make_object(oid1, tp(tidp0, 0, 99).pos(0, 999)),      // size = 1000
      make_object(oid2, tp(tidp0, 100, 199).pos(0, 1999)),  // size = 2000
      make_object(oid3, tp(tidp0, 200, 299).pos(0, 2999))); // size = 3000

    verify_metadata_size(tidp0, 6000);

    // Set start offset to remove the first extent.
    set_start_offset(tidp0, kafka::offset(100));
    verify_metadata_size(tidp0, 5000);

    // Set start offset to remove the second extent as well.
    set_start_offset(tidp0, kafka::offset(200));
    verify_metadata_size(tidp0, 3000);

    // Set start offset to remove all extents.
    set_start_offset(tidp0, kafka::offset(300));
    verify_metadata_size(tidp0, 0);
}

TEST_F(StateUpdateTest, TestRemoveTopicsZerosSize) {
    // Add extents for a partition.
    add_objects(
      {terms(tidp0, {{0, 1}})},
      make_object(make_oid(), tp(tidp0, 0, 99).pos(0, 1023)));
    verify_metadata_size(tidp0, 1024);

    // Remove the topic.
    chunked_vector<model::topic_id> topics_to_remove;
    topics_to_remove.push_back(tidp0.topic_id);
    remove_topics(std::move(topics_to_remove));

    // Metadata should be removed entirely.
    verify_metadata_missing(tidp0);
}

TEST_F(StateUpdateTest, TestPreregisterObjectsBasic) {
    auto oid1 = make_oid();
    auto oid2 = make_oid();

    preregister_objects_db_update update;
    update.object_ids.push_back(oid1);
    update.object_ids.push_back(oid2);
    update.registered_at = model::timestamp::now();

    auto reader = make_reader();
    auto can_apply_res = update.can_apply(reader).get();
    ASSERT_TRUE(can_apply_res.has_value());

    apply_preregister_update(update);

    verify_object_preregistered(oid1);
    verify_object_preregistered(oid2);
}

TEST_F(StateUpdateTest, TestPreregisterObjectsRejectsDuplicate) {
    auto oid1 = make_oid();

    preregister_objects_db_update update1;
    update1.object_ids.push_back(oid1);
    update1.registered_at = model::timestamp::now();
    apply_preregister_update(update1);

    preregister_objects_db_update update2;
    update2.object_ids.push_back(oid1);
    update2.registered_at = model::timestamp::now();
    auto reader = make_reader();
    auto can_apply_res = update2.can_apply(reader).get();
    EXPECT_FALSE(can_apply_res.has_value());
}

TEST_F(StateUpdateTest, TestExpirePreregisteredObjectsClearsFlag) {
    auto oid1 = make_oid();
    auto oid2 = make_oid();

    preregister_objects_db_update prereg;
    prereg.object_ids.push_back(oid1);
    prereg.object_ids.push_back(oid2);
    prereg.registered_at = model::timestamp::now();
    apply_preregister_update(prereg);

    verify_object_preregistered(oid1);
    verify_object_preregistered(oid2);

    expire_preregistered_objects_db_update expire;
    expire.object_ids.push_back(oid1);
    apply_expire_preregistered_update(expire);

    // oid1 should have is_preregistration cleared.
    verify_object_not_preregistered(oid1);
    // oid2 should be unchanged.
    verify_object_preregistered(oid2);
}

TEST_F(StateUpdateTest, TestPreregisterObjects) {
    auto oid1 = make_oid();
    auto oid2 = make_oid();

    chunked_vector<object_id> oids;
    oids.push_back(oid1);
    oids.push_back(oid2);
    preregister_objects(std::move(oids), model::timestamp(12345));

    verify_preregistered_object_exists(oid1);
    verify_preregistered_object_exists(oid2);
}

TEST_F(StateUpdateTest, TestExpirePreregisteredObjects) {
    auto oid1 = make_oid();
    auto oid2 = make_oid();

    chunked_vector<object_id> oids;
    oids.push_back(oid1);
    oids.push_back(oid2);
    preregister_objects(std::move(oids), model::timestamp(12345));

    verify_preregistered_object_exists(oid1);
    verify_preregistered_object_exists(oid2);

    chunked_vector<object_id> expire_oids;
    expire_oids.push_back(oid1);
    expire_oids.push_back(oid2);
    auto expire_update = expire_preregistered_objects_db_update{
      .object_ids = std::move(expire_oids),
    };
    apply_expire_preregistered_update(expire_update);

    // is_preregistration flag should be cleared.
    verify_preregistered_object_missing(oid1);
    verify_preregistered_object_missing(oid2);

    // Objects remain as zero-sized entries, now eligible for GC.
    verify_object_exists(oid1, 0);
    verify_object_exists(oid2, 0);
}

TEST_F(StateUpdateTest, TestAddObjectsRequiresPreregistration) {
    auto oid = make_oid();

    // Attempt to add an object that has NOT been pre-registered.
    auto update = make_add_objects_update(
      {terms(tidp0, {{0, 1}})},
      make_object(oid, tp(tidp0, 0, 99).pos(0, 1023)));
    auto reader = make_reader();
    chunked_vector<write_batch_row> rows;
    auto result = update.build_rows(reader, rows).get();
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().e, db_update_errc::invalid_update);
}

TEST_F(StateUpdateTest, TestAddObjectsWithPreregistration) {
    auto oid = make_oid();

    // Pre-register the object first.
    chunked_vector<object_id> oids;
    oids.push_back(oid);
    preregister_objects(std::move(oids), model::timestamp(12345));
    verify_preregistered_object_exists(oid);

    // Now add the object — call build_rows directly since oid is already
    // preregistered (add_objects() would preregister again, which is invalid).
    {
        auto db_update = make_add_objects_update(
          {terms(tidp0, {{0, 1}})},
          make_object(oid, tp(tidp0, 0, 99).pos(0, 1023)));
        auto reader = make_reader();
        chunked_vector<write_batch_row> rows;
        ASSERT_TRUE(db_update.build_rows(reader, rows).get().has_value());
        auto wb = db_->create_write_batch();
        auto seqno = next_seqno();
        for (const auto& row : rows) {
            if (row.value.empty()) {
                wb.remove(row.key, seqno);
            } else {
                wb.put(row.key, row.value.copy(), seqno);
            }
        }
        db_->apply(std::move(wb)).get();
    }

    verify_metadata(tidp0, kafka::offset(0), kafka::offset(100));
    verify_object_exists(oid, 1024);

    // Preregistered row should be cleaned up.
    verify_preregistered_object_missing(oid);
}

TEST_F(StateUpdateTest, TestDiscoverTruncatedObjectIds) {
    auto oid1 = make_oid();
    auto oid2 = make_oid();
    auto oid3 = make_oid();
    add_objects(
      {terms(tidp0, {{0, 1}})},
      make_object(oid1, tp(tidp0, 0, 99).pos(0, 1023)),
      make_object(oid2, tp(tidp0, 100, 199).pos(0, 1023)),
      make_object(oid3, tp(tidp0, 200, 299).pos(0, 1023)));

    // Discover objects below offset 200: should find oid1 and oid2.
    auto update = set_start_offset_db_update{
      .tp = tidp0,
      .new_start_offset = kafka::offset(200),
    };
    auto reader = make_reader();
    auto result = update.discover_truncated_object_ids(reader).get();
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result.value().size(), 2);
    EXPECT_TRUE(result.value().contains(oid1));
    EXPECT_TRUE(result.value().contains(oid2));
    EXPECT_FALSE(result.value().contains(oid3));
}

TEST_F(StateUpdateTest, TestDiscoverReplacedObjectIds) {
    auto old_oid1 = make_oid();
    auto old_oid2 = make_oid();
    auto old_oid3 = make_oid();
    add_objects(
      {terms(tidp0, {{0, 1}})},
      make_object(old_oid1, tp(tidp0, 0, 99).pos(0, 1023)),
      make_object(old_oid2, tp(tidp0, 100, 199).pos(0, 1023)),
      make_object(old_oid3, tp(tidp0, 200, 299).pos(0, 1023)));

    // Build a replace update that replaces extents [0-199]. Discovery
    // should find old_oid1 and old_oid2 but not old_oid3.
    auto new_oid = make_oid();
    auto db_update = make_replace_objects_update(
      compact_specs{}, make_object(new_oid, tp(tidp0, 0, 199).pos(0, 2047)));

    // Preregister the new object so validate_inputs passes.
    preregister_new_objects(db_update.new_objects);

    auto reader = make_reader();
    auto result = db_update.discover_replaced_object_ids(reader).get();
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result.value().size(), 2);
    EXPECT_TRUE(result.value().contains(old_oid1));
    EXPECT_TRUE(result.value().contains(old_oid2));
    EXPECT_FALSE(result.value().contains(old_oid3));
}

TEST_F(StateUpdateTest, TestDiscoverRemoveTopicsObjectIds) {
    // Use a different topic_id for the second topic.
    auto other_tidp = model::topic_id_partition(
      model::topic_id(
        uuid_t::from_string("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")),
      model::partition_id(0));

    auto oid1 = make_oid();
    auto oid2 = make_oid();
    auto oid3 = make_oid();
    add_objects(
      {terms(tidp0, {{0, 1}})},
      make_object(oid1, tp(tidp0, 0, 99).pos(0, 1023)),
      make_object(oid2, tp(tidp0, 100, 199).pos(0, 1023)));
    add_objects(
      {terms(other_tidp, {{0, 1}})},
      make_object(oid3, tp(other_tidp, 0, 99).pos(0, 1023)));

    // Discover objects for tidp0's topic only — should find oid1 and oid2
    // but not oid3 (different topic).
    auto update = remove_topics_db_update{
      .topics = chunked_vector<model::topic_id>::single(tidp0.topic_id),
    };
    auto reader = make_reader();
    auto result = update.discover_object_ids(reader).get();
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result.value().size(), 2);
    EXPECT_TRUE(result.value().contains(oid1));
    EXPECT_TRUE(result.value().contains(oid2));
    EXPECT_FALSE(result.value().contains(oid3));
}
