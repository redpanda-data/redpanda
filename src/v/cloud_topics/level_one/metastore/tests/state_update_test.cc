/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "absl/strings/numbers.h"
#include "absl/strings/str_split.h"
#include "cloud_topics/level_one/common/object_id.h"
#include "cloud_topics/level_one/metastore/lsm/keys.h"
#include "cloud_topics/level_one/metastore/lsm/state_reader.h"
#include "cloud_topics/level_one/metastore/lsm/state_update.h"
#include "cloud_topics/level_one/metastore/lsm/values.h"
#include "cloud_topics/level_one/metastore/lsm/write_batch_row.h"
#include "cloud_topics/level_one/metastore/state_update.h"
#include "cloud_topics/level_one/metastore/tests/state_utils.h"
#include "gmock/gmock.h"
#include "lsm/io/memory_persistence.h"
#include "lsm/lsm.h"
#include "model/fundamental.h"
#include "serde/rw/rw.h"
#include "utils/uuid.h"

#include <gtest/gtest.h>

using namespace cloud_topics;
using namespace cloud_topics::l1;

namespace {
const object_id oid1 = l1::create_object_id();
const object_id oid2 = l1::create_object_id();
const object_id oid3 = l1::create_object_id();
const object_id oid4 = l1::create_object_id();
const object_id oid5 = l1::create_object_id();
const object_id oid6 = l1::create_object_id();
const std::string_view tidp_a = "deadbeef-aaaa-0000-0000-000000000000/0";
const std::string_view tidp_b = "deadbeef-bbbb-0000-0000-000000000000/0";
const std::string_view tidp_c = "deadbeef-cccc-0000-0000-000000000000/0";

kafka::offset operator""_o(unsigned long long o) {
    return kafka::offset{static_cast<int64_t>(o)};
}

model::timestamp operator""_t(unsigned long long t) {
    return model::timestamp{static_cast<int64_t>(t)};
}
model::term_id operator""_tm(unsigned long long t) {
    return model::term_id{static_cast<int64_t>(t)};
}

class new_obj_builder {
public:
    new_obj_builder(object_id oid, size_t footer_pos, size_t object_size) {
        out.oid = oid;
        out.footer_pos = footer_pos;
        out.object_size = object_size;
    }
    new_obj_builder(const new_obj_builder&) = delete;
    new_obj_builder(new_obj_builder&&) = default;
    new_object build() { return std::move(out); }

    new_obj_builder& add(
      std::string_view tpr_str,
      kafka::offset base_o,
      kafka::offset last_o,
      model::timestamp last_t,
      size_t first_pos,
      size_t last_pos) {
        auto tpr = model::topic_id_partition::from(tpr_str);
        out.extent_metas[tpr.topic_id][tpr.partition] = new_object::metadata{
          .base_offset = base_o,
          .last_offset = last_o,
          .max_timestamp = last_t,
          .filepos = first_pos,
          .len = last_pos - first_pos,
        };
        return *this;
    }

private:
    new_object out;
};

struct add_objects_builder {
public:
    add_objects_builder& add(new_object o) {
        out.new_objects.emplace_back(std::move(o));
        return *this;
    }
    add_objects_builder&
    add_term_start(std::string_view tp_str, model::term_id t, kafka::offset o) {
        auto tp = model::topic_id_partition::from(tp_str);
        out.new_terms[tp].emplace_back(
          term_start{.term_id = t, .start_offset = o});
        return *this;
    }
    add_objects_update build() { return std::move(out); }

private:
    add_objects_update out;
};

struct replace_objects_builder {
public:
    replace_objects_builder& add(new_object o) {
        out.new_objects.emplace_back(std::move(o));
        return *this;
    }
    replace_objects_builder& clean(
      std::string_view tp_str,
      compaction_state_update::cleaned_range r,
      model::timestamp cleaned_at) {
        auto tp = model::topic_id_partition::from(tp_str);
        auto& c_state = out.compaction_updates[tp.topic_id][tp.partition];
        c_state.cleaned_at = cleaned_at;
        c_state.new_cleaned_ranges.push_back(std::move(r));
        return *this;
    }
    replace_objects_builder& clean_tombstones(
      std::string_view tp_str, kafka::offset base, kafka::offset last) {
        auto tp = model::topic_id_partition::from(tp_str);
        auto& c_state = out.compaction_updates[tp.topic_id][tp.partition];
        c_state.removed_tombstones_ranges.insert(base, last);
        return *this;
    }
    replace_objects_builder& set_expected_epoch(
      std::string_view tp_str,
      partition_state::compaction_epoch_t compaction_epoch) {
        auto tp = model::topic_id_partition::from(tp_str);
        auto& c_state = out.compaction_updates[tp.topic_id][tp.partition];
        c_state.expected_compaction_epoch = compaction_epoch;
        return *this;
    }
    replace_objects_update build() { return std::move(out); }

private:
    replace_objects_update out;
};
} // namespace

enum class state_backend { simple, lsm };

class StateUpdateParamTest : public ::testing::TestWithParam<state_backend> {
protected:
    void SetUp() override {
        if (GetParam() == state_backend::lsm) {
            db_ = lsm::database::open(
                    {.database_epoch = 0},
                    lsm::io::persistence{
                      .data = lsm::io::make_memory_data_persistence(),
                      .metadata = lsm::io::make_memory_metadata_persistence(),
                    })
                    .get();
        }
    }

    void TearDown() override {
        if (db_) {
            db_->close().get();
        }
    }

    std::expected<std::monostate, stm_update_error>
    apply_add_objects(add_objects_update update) {
        chunked_vector<object_id> oids;
        for (const auto& o : update.new_objects) {
            oids.push_back(o.oid);
        }
        if (GetParam() == state_backend::simple) {
            auto prereg = preregister_for_simple(oids);
            if (!prereg.has_value()) {
                return prereg;
            }
            return update.apply(state_);
        }
        auto prereg = preregister_for_lsm(oids);
        if (!prereg.has_value()) {
            return prereg;
        }
        add_objects_db_update db_update{
          .new_objects = std::move(update.new_objects),
          .new_terms = std::move(update.new_terms),
        };
        auto reader = state_reader(db_->create_snapshot());
        chunked_vector<write_batch_row> rows;
        auto result = db_update.build_rows(reader, rows).get();
        if (!result.has_value()) {
            return std::unexpected(
              stm_update_error{fmt::format("{}", result.error())});
        }
        apply_rows_to_db(rows);
        return std::monostate{};
    }

    std::expected<std::monostate, stm_update_error>
    apply_replace_objects(replace_objects_update update) {
        chunked_vector<object_id> oids;
        for (const auto& o : update.new_objects) {
            oids.push_back(o.oid);
        }
        if (GetParam() == state_backend::simple) {
            auto prereg = preregister_for_simple(oids);
            if (!prereg.has_value()) {
                return prereg;
            }
            return update.apply(state_);
        }
        auto prereg = preregister_for_lsm(oids);
        if (!prereg.has_value()) {
            return prereg;
        }
        replace_objects_db_update db_update{
          .new_objects = std::move(update.new_objects),
          .compaction_updates = std::move(update.compaction_updates),
        };
        auto reader = state_reader(db_->create_snapshot());
        chunked_vector<write_batch_row> rows;
        auto result = db_update.build_rows(reader, rows).get();
        if (!result.has_value()) {
            return std::unexpected(
              stm_update_error{fmt::format("{}", result.error())});
        }
        apply_rows_to_db(rows);
        return std::monostate{};
    }

    std::expected<std::monostate, stm_update_error>
    can_apply_add_objects(add_objects_update update) {
        if (GetParam() == state_backend::simple) {
            return update.can_apply(state_);
        }
        add_objects_db_update db_update{
          .new_objects = std::move(update.new_objects),
          .new_terms = std::move(update.new_terms),
        };
        auto validate_res = db_update.validate_inputs();
        if (!validate_res.has_value()) {
            return std::unexpected(
              stm_update_error{fmt::format("{}", validate_res.error())});
        }
        auto reader = state_reader(db_->create_snapshot());
        chunked_vector<write_batch_row> rows;
        auto result = db_update.build_rows(reader, rows).get();
        if (!result.has_value()) {
            return std::unexpected(
              stm_update_error{fmt::format("{}", result.error())});
        }
        return std::monostate{};
    }

    std::expected<std::monostate, stm_update_error> apply_set_start_offset(
      const model::topic_id_partition& tp, kafka::offset new_start_offset) {
        if (GetParam() == state_backend::simple) {
            auto update = set_start_offset_update::build(
              state_, tp, new_start_offset);
            if (!update.has_value()) {
                return std::unexpected(
                  stm_update_error{fmt::format("{}", update.error())});
            }
            return update->apply(state_);
        }
        set_start_offset_db_update db_update{
          .tp = tp,
          .new_start_offset = new_start_offset,
        };
        auto reader = state_reader(db_->create_snapshot());
        chunked_vector<write_batch_row> rows;
        auto result = db_update.build_rows(reader, rows).get();
        if (!result.has_value()) {
            return std::unexpected(
              stm_update_error{fmt::format("{}", result.error())});
        }
        apply_rows_to_db(rows);
        return std::monostate{};
    }

    std::expected<std::monostate, stm_update_error>
    apply_remove_topics(std::initializer_list<model::topic_id> topics_list) {
        chunked_vector<model::topic_id> topics;
        for (const auto& t : topics_list) {
            topics.push_back(t);
        }
        if (GetParam() == state_backend::simple) {
            auto update = remove_topics_update::build(
              state_, std::move(topics));
            if (!update.has_value()) {
                return std::unexpected(
                  stm_update_error{fmt::format("{}", update.error())});
            }
            return update->apply(state_);
        }
        remove_topics_db_update db_update{
          .topics = std::move(topics),
        };
        auto reader = state_reader(db_->create_snapshot());
        chunked_vector<write_batch_row> rows;
        auto result = db_update.build_rows(reader, rows).get();
        if (!result.has_value()) {
            return std::unexpected(
              stm_update_error{fmt::format("{}", result.error())});
        }
        apply_rows_to_db(rows);
        return std::monostate{};
    }

    std::expected<std::monostate, stm_update_error>
    apply_remove_objects(std::initializer_list<object_id> objects_list) {
        chunked_vector<object_id> objects;
        for (const auto& o : objects_list) {
            objects.push_back(o);
        }
        if (GetParam() == state_backend::simple) {
            auto update = remove_objects_update::build(
              state_, std::move(objects));
            if (!update.has_value()) {
                return std::unexpected(
                  stm_update_error{fmt::format("{}", update.error())});
            }
            return update->apply(state_);
        }
        remove_objects_db_update db_update{
          .objects = std::move(objects),
        };
        auto reader = state_reader(db_->create_snapshot());
        chunked_vector<write_batch_row> rows;
        auto result = db_update.build_rows(rows).get();
        if (!result.has_value()) {
            return std::unexpected(
              stm_update_error{fmt::format("{}", result.error())});
        }
        apply_rows_to_db(rows);
        return std::monostate{};
    }

    std::expected<std::monostate, stm_update_error> apply_preregister_objects(
      chunked_vector<object_id> ids, model::timestamp ts) {
        if (GetParam() == state_backend::simple) {
            preregister_objects_update update;
            update.object_ids = std::move(ids);
            update.registered_at = ts;
            return update.apply(state_);
        }
        preregister_objects_db_update db_update{
          .object_ids = std::move(ids),
          .registered_at = ts,
        };
        auto reader = state_reader(db_->create_snapshot());
        chunked_vector<write_batch_row> rows;
        auto result = db_update.build_rows(reader, rows).get();
        if (!result.has_value()) {
            return std::unexpected(
              stm_update_error{fmt::format("{}", result.error())});
        }
        apply_rows_to_db(rows);
        return std::monostate{};
    }

    std::expected<std::monostate, stm_update_error>
    apply_expire_preregistered_objects(chunked_vector<object_id> ids) {
        if (GetParam() == state_backend::simple) {
            expire_preregistered_objects_update update;
            update.object_ids = std::move(ids);
            return update.apply(state_);
        }
        expire_preregistered_objects_db_update db_update{
          .object_ids = std::move(ids),
        };
        auto reader = state_reader(db_->create_snapshot());
        chunked_vector<write_batch_row> rows;
        auto result = db_update.build_rows(reader, rows).get();
        if (!result.has_value()) {
            return std::unexpected(
              stm_update_error{fmt::format("{}", result.error())});
        }
        apply_rows_to_db(rows);
        return std::monostate{};
    }

    state& get_state() {
        if (GetParam() == state_backend::lsm) {
            state_ = snapshot_to_state(*db_);
        }
        return state_;
    }

    std::expected<std::monostate, stm_update_error>
    apply_preregister_objects(std::initializer_list<object_id> ids) {
        chunked_vector<object_id> id_vec(ids.begin(), ids.end());
        if (GetParam() == state_backend::simple) {
            return preregister_for_simple(id_vec);
        }
        return preregister_for_lsm(id_vec);
    }

private:
    std::expected<std::monostate, stm_update_error>
    preregister_for_simple(const chunked_vector<object_id>& ids) {
        preregister_objects_update u;
        u.registered_at = model::timestamp::now();
        for (const auto& oid : ids) {
            u.object_ids.push_back(oid);
        }
        if (u.object_ids.empty()) {
            return std::monostate{};
        }
        return u.apply(state_);
    }

    std::expected<std::monostate, stm_update_error>
    preregister_for_lsm(const chunked_vector<object_id>& ids) {
        preregister_objects_db_update update;
        update.registered_at = model::timestamp::now();
        for (const auto& oid : ids) {
            update.object_ids.push_back(oid);
        }
        auto reader = state_reader(db_->create_snapshot());
        chunked_vector<write_batch_row> rows;
        auto result = update.build_rows(reader, rows).get();
        if (!result.has_value()) {
            return std::unexpected(
              stm_update_error{fmt::format("{}", result.error())});
        }
        apply_rows_to_db(rows);
        return std::monostate{};
    }

    void apply_rows_to_db(const chunked_vector<write_batch_row>& rows) {
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

    lsm::sequence_number next_seqno() {
        auto max_applied_opt = db_->max_applied_seqno();
        if (!max_applied_opt) {
            return lsm::sequence_number(1);
        }
        return lsm::sequence_number{max_applied_opt.value()() + 1};
    }

    state state_;
    std::optional<lsm::database> db_;
};

INSTANTIATE_TEST_SUITE_P(
  StateUpdate,
  StateUpdateParamTest,
  testing::Values(state_backend::simple, state_backend::lsm),
  [](const testing::TestParamInfo<state_backend>& info) {
      return info.param == state_backend::simple ? "Simple" : "LSM";
  });

TEST_P(StateUpdateParamTest, TestEmptyAdd) {
    auto empty_update = add_objects_builder().build();
    auto res = can_apply_add_objects(std::move(empty_update));
    EXPECT_FALSE(res.has_value());
}

TEST_P(StateUpdateParamTest, TestAddBasic) {
    {
        auto update = add_objects_builder()
                        .add(new_obj_builder(oid1, 100, 1100)
                               .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                               .add(tidp_b, 0_o, 10_o, 1999_t, 100, 199)
                               .build())
                        .add_term_start(tidp_a, 0_tm, 0_o)
                        .add_term_start(tidp_b, 0_tm, 0_o)
                        .build();
        ASSERT_FALSE(update.new_objects.empty());
        auto res = apply_add_objects(std::move(update));
        EXPECT_TRUE(res.has_value());
        EXPECT_EQ(2, get_state().topic_to_state.size());
    }
    {
        auto update = add_objects_builder()
                        .add(new_obj_builder(oid2, 100, 1100)
                               .add(tidp_c, 0_o, 10_o, 1999_t, 0, 99)
                               .add(tidp_b, 11_o, 20_o, 1999_t, 100, 199)
                               .build())
                        .add_term_start(tidp_c, 0_tm, 0_o)
                        .add_term_start(tidp_b, 0_tm, 11_o)
                        .build();
        ASSERT_FALSE(update.new_objects.empty());
        auto res = apply_add_objects(std::move(update));
        EXPECT_TRUE(res.has_value());
        EXPECT_EQ(3, get_state().topic_to_state.size());
    }
}

TEST_P(StateUpdateParamTest, TestDuplicateAddSingleUpdate) {
    ASSERT_TRUE(apply_preregister_objects({oid1, oid2}).has_value());
    auto update = add_objects_builder()
                    .add(new_obj_builder(oid1, 100, 1100)
                           .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                           .build())
                    .add(new_obj_builder(oid2, 100, 1100)
                           .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                           .build())
                    .add_term_start(tidp_a, 0_tm, 0_o)
                    .add_term_start(tidp_b, 0_tm, 0_o)
                    .build();
    ASSERT_FALSE(update.new_objects.empty());
    auto res = can_apply_add_objects(std::move(update));
    EXPECT_FALSE(res.has_value());
}

TEST_P(StateUpdateParamTest, TestAddRejectsInvertedExtent) {
    // An extent with base_offset > last_offset should be rejected.
    auto update = add_objects_builder()
                    .add(new_obj_builder(oid1, 100, 1100)
                           .add(tidp_a, 10_o, 5_o, 1999_t, 0, 99)
                           .build())
                    .add_term_start(tidp_a, 0_tm, 10_o)
                    .build();
    auto res = can_apply_add_objects(std::move(update));
    EXPECT_FALSE(res.has_value());
}

TEST_P(StateUpdateParamTest, TestStartAfterZero) {
    auto update = add_objects_builder()
                    .add(new_obj_builder(oid1, 100, 1100)
                           .add(tidp_a, 11_o, 20_o, 1999_t, 0, 99)
                           .build())
                    .add_term_start(tidp_a, 0_tm, 11_o)
                    .build();
    auto res = apply_add_objects(std::move(update));
    EXPECT_TRUE(res.has_value());

    auto& s = get_state();
    // The added object should be tracked as removed.
    EXPECT_EQ(0, s.topic_to_state.size());
    EXPECT_EQ(1, s.objects.size());
    auto& added_obj = s.objects.at(oid1);
    EXPECT_EQ(added_obj.removed_data_size, added_obj.total_data_size);
}

TEST_P(StateUpdateParamTest, TestDuplicateObject) {
    auto update = add_objects_builder()
                    .add(new_obj_builder(oid1, 100, 1100)
                           .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                           .add(tidp_b, 0_o, 10_o, 1999_t, 100, 199)
                           .add(tidp_c, 0_o, 10_o, 1999_t, 200, 299)
                           .build())
                    .add_term_start(tidp_a, 0_tm, 0_o)
                    .add_term_start(tidp_b, 0_tm, 0_o)
                    .add_term_start(tidp_c, 0_tm, 0_o)
                    .build();
    ASSERT_FALSE(update.new_objects.empty());
    auto res = apply_add_objects(std::move(update));
    EXPECT_TRUE(res.has_value());
    {
        auto& s = get_state();
        EXPECT_EQ(3, s.topic_to_state.size());
        EXPECT_EQ(1, s.objects.size());
    }

    // Try to apply the exact same add_object -- it should be rejected.
    auto dupe_update = add_objects_builder()
                         .add(new_obj_builder(oid1, 100, 1100)
                                .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                                .add(tidp_b, 0_o, 10_o, 1999_t, 100, 199)
                                .add(tidp_c, 0_o, 10_o, 1999_t, 200, 299)
                                .build())
                         .add_term_start(tidp_a, 0_tm, 0_o)
                         .add_term_start(tidp_b, 0_tm, 0_o)
                         .add_term_start(tidp_c, 0_tm, 0_o)
                         .build();
    auto dupe_res = can_apply_add_objects(std::move(dupe_update));
    EXPECT_FALSE(dupe_res.has_value());
    EXPECT_EQ(1, get_state().objects.size());
}

TEST_P(StateUpdateParamTest, TestDuplicateAddMultipleUpdates) {
    auto update = add_objects_builder()
                    .add(new_obj_builder(oid1, 100, 1100)
                           .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                           .add(tidp_b, 0_o, 10_o, 1999_t, 100, 199)
                           .add(tidp_c, 0_o, 10_o, 1999_t, 200, 299)
                           .build())
                    .add_term_start(tidp_a, 0_tm, 0_o)
                    .add_term_start(tidp_b, 0_tm, 0_o)
                    .add_term_start(tidp_c, 0_tm, 0_o)
                    .build();
    auto res = apply_add_objects(std::move(update));
    EXPECT_TRUE(res.has_value());
    auto& s = get_state();
    EXPECT_EQ(3, s.topic_to_state.size());
    for (const auto& [t, p_states] : s.topic_to_state) {
        EXPECT_EQ(
          1, p_states.pid_to_state.at(model::partition_id{0}).extents.size());
    }
    EXPECT_EQ(1, s.objects.size());

    // Create a second update with a different object but same extents
    // (overlapping with what we just applied).
    auto dupe_update = add_objects_builder()
                         .add(new_obj_builder(oid2, 100, 1100)
                                .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                                .add(tidp_b, 0_o, 10_o, 1999_t, 100, 199)
                                .add(tidp_c, 0_o, 10_o, 1999_t, 200, 299)
                                .build())
                         .add_term_start(tidp_a, 0_tm, 0_o)
                         .add_term_start(tidp_b, 0_tm, 0_o)
                         .add_term_start(tidp_c, 0_tm, 0_o)
                         .build();
    auto dupe_res = apply_add_objects(std::move(dupe_update));
    EXPECT_TRUE(dupe_res.has_value());
    auto& s2 = get_state();
    EXPECT_EQ(3, s2.topic_to_state.size());
    for (const auto& [t, p_states] : s2.topic_to_state) {
        // The tracked extents shouldn't change.
        EXPECT_EQ(
          1, p_states.pid_to_state.at(model::partition_id{0}).extents.size());
    }
    EXPECT_EQ(2, s2.objects.size());
    auto& dupe_obj = s2.objects.at(oid2);
    EXPECT_EQ(dupe_obj.removed_data_size, dupe_obj.total_data_size);
}

TEST_P(StateUpdateParamTest, TestOverlapSomePartitions) {
    {
        auto update = add_objects_builder()
                        .add(new_obj_builder(oid1, 100, 1100)
                               .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                               .add(tidp_b, 0_o, 10_o, 1999_t, 100, 199)
                               .build())
                        .add_term_start(tidp_a, 0_tm, 0_o)
                        .add_term_start(tidp_b, 0_tm, 0_o)
                        .build();
        auto res = apply_add_objects(std::move(update));
        EXPECT_TRUE(res.has_value());
    }
    {
        auto& s = get_state();
        EXPECT_EQ(2, s.topic_to_state.size());
        for (const auto& [t, p_states] : s.topic_to_state) {
            EXPECT_EQ(
              1,
              p_states.pid_to_state.at(model::partition_id{0}).extents.size());
        }
        EXPECT_EQ(1, s.objects.size());
    }

    // Now send a bogus range for one of the partitions, but a correct extent
    // for another.
    {
        auto update = add_objects_builder()
                        .add(new_obj_builder(oid2, 100, 1100)
                               .add(tidp_a, 1337_o, 1337_o, 1999_t, 0, 5)
                               .add(tidp_b, 11_o, 20_o, 1999_t, 100, 199)
                               .build())
                        .add_term_start(tidp_a, 0_tm, 1337_o)
                        .add_term_start(tidp_b, 0_tm, 11_o)
                        .build();
        auto misaligned_res = apply_add_objects(std::move(update));
        EXPECT_TRUE(misaligned_res.has_value());
    }

    auto& s = get_state();
    EXPECT_EQ(2, s.topic_to_state.size());

    // The bad update shouldn't be applied.
    auto p_a = s.partition_state(model::topic_id_partition::from(tidp_a));
    ASSERT_TRUE(p_a.has_value());
    EXPECT_EQ(1, p_a->get().extents.size());

    // But the good update should be there.
    auto p_b = s.partition_state(model::topic_id_partition::from(tidp_b));
    ASSERT_TRUE(p_b.has_value());
    EXPECT_EQ(2, p_b->get().extents.size());

    // The accounting for the object should reflect this.
    EXPECT_EQ(2, s.objects.size());
    auto& dupe_obj = s.objects.at(oid2);
    EXPECT_EQ(dupe_obj.removed_data_size, 5);
    EXPECT_EQ(dupe_obj.total_data_size, 104);
}

namespace {

MATCHER_P3(MatchesRange, oid, base, last, "") {
    return arg.oid == oid && arg.base_offset == base && arg.last_offset == last;
}
MATCHER_P2(MatchesRange, base, last, "") {
    return arg.base_offset == base && arg.last_offset == last;
}
MATCHER_P2(MatchesTermStart, term, offset, "") {
    return arg.term_id == term && arg.start_offset == offset;
}

} // namespace

TEST_P(StateUpdateParamTest, TestReplaceBasic) {
    using testing::ElementsAre;
    auto add = add_objects_builder()
                 .add(new_obj_builder(oid1, 300, 1300)
                        .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                        .add(tidp_b, 0_o, 10_o, 1999_t, 100, 199)
                        .add(tidp_c, 0_o, 10_o, 1999_t, 200, 299)
                        .build())
                 .add(new_obj_builder(oid2, 100, 1100)
                        .add(tidp_a, 11_o, 20_o, 1999_t, 0, 99)
                        .add(tidp_c, 11_o, 20_o, 1999_t, 0, 99)
                        .build())
                 .add_term_start(tidp_a, 0_tm, 0_o)
                 .add_term_start(tidp_b, 0_tm, 0_o)
                 .add_term_start(tidp_c, 0_tm, 0_o)
                 .build();
    auto add_res = apply_add_objects(std::move(add));
    ASSERT_TRUE(add_res.has_value());

    // Fully replace partition a, partially replace c.
    auto replace = replace_objects_builder()
                     .add(new_obj_builder(oid3, 100, 1100)
                            .add(tidp_a, 0_o, 20_o, 1999_t, 0, 99)
                            .add(tidp_c, 0_o, 10_o, 1999_t, 100, 199)
                            .build())
                     .build();

    auto replace_res = apply_replace_objects(std::move(replace));
    ASSERT_TRUE(replace_res.has_value());

    auto& s = get_state();

    // Fully replaced.
    const auto& prt_a
      = s.partition_state(model::topic_id_partition::from(tidp_a))->get();
    EXPECT_THAT(prt_a.extents, ElementsAre(MatchesRange(oid3, 0_o, 20_o)));

    // Not replaced.
    const auto& prt_b
      = s.partition_state(model::topic_id_partition::from(tidp_b))->get();
    EXPECT_THAT(prt_b.extents, ElementsAre(MatchesRange(oid1, 0_o, 10_o)));

    // Partially replaced.
    const auto& prt_c
      = s.partition_state(model::topic_id_partition::from(tidp_c))->get();
    EXPECT_THAT(
      prt_c.extents,
      ElementsAre(
        MatchesRange(oid3, 0_o, 10_o), MatchesRange(oid2, 11_o, 20_o)));

    EXPECT_EQ(s.objects.at(oid1).removed_data_size, 198);
    EXPECT_EQ(s.objects.at(oid2).removed_data_size, 99);
    EXPECT_EQ(s.objects.at(oid3).removed_data_size, 0);
}

TEST_P(StateUpdateParamTest, TestReplaceEmptyState) {
    auto replace = replace_objects_builder()
                     .add(new_obj_builder(oid1, 100, 1100)
                            .add(tidp_a, 0_o, 20_o, 1999_t, 0, 99)
                            .build())
                     .build();

    auto replace_res = apply_replace_objects(std::move(replace));
    EXPECT_FALSE(replace_res.has_value());
}

TEST_P(StateUpdateParamTest, TestReplaceDuplicate) {
    auto add = add_objects_builder()
                 .add(new_obj_builder(oid1, 100, 1100)
                        .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                        .build())
                 .add_term_start(tidp_a, 0_tm, 0_o)
                 .build();
    auto add_res = apply_add_objects(std::move(add));
    ASSERT_TRUE(add_res.has_value());

    auto replace = replace_objects_builder()
                     .add(new_obj_builder(oid1, 100, 1100)
                            .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                            .build())
                     .build();

    auto replace_res = apply_replace_objects(std::move(replace));
    EXPECT_FALSE(replace_res.has_value());
}

TEST_P(StateUpdateParamTest, TestReplaceMisaligned) {
    auto add = add_objects_builder()
                 .add(new_obj_builder(oid1, 100, 1100)
                        .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                        .build())
                 .add_term_start(tidp_a, 0_tm, 0_o)
                 .build();
    auto add_res = apply_add_objects(std::move(add));
    ASSERT_TRUE(add_res.has_value());

    auto replace = replace_objects_builder()
                     .add(new_obj_builder(oid2, 100, 1100)
                            .add(tidp_a, 0_o, 9_o, 1999_t, 0, 99)
                            .build())
                     .build();

    auto replace_res = apply_replace_objects(std::move(replace));
    EXPECT_FALSE(replace_res.has_value());
}

TEST_P(StateUpdateParamTest, TestReplaceBadOrdering) {
    auto add = add_objects_builder()
                 .add(new_obj_builder(oid1, 100, 1100)
                        .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                        .build())
                 .add_term_start(tidp_a, 0_tm, 0_o)
                 .build();
    auto add_res = apply_add_objects(std::move(add));
    ASSERT_TRUE(add_res.has_value());

    // Make the replacement overlap with itself.
    auto replace = replace_objects_builder()
                     .add(new_obj_builder(oid2, 100, 1100)
                            .add(tidp_a, 0_o, 7_o, 1999_t, 0, 99)
                            .build())
                     .add(new_obj_builder(oid3, 100, 1100)
                            .add(tidp_a, 5_o, 10_o, 1999_t, 0, 99)
                            .build())
                     .build();

    auto replace_res = apply_replace_objects(std::move(replace));
    EXPECT_FALSE(replace_res.has_value());
}

TEST_P(StateUpdateParamTest, TestReplaceRejectsInvertedExtent) {
    auto add = add_objects_builder()
                 .add(new_obj_builder(oid1, 100, 1100)
                        .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                        .build())
                 .add_term_start(tidp_a, 0_tm, 0_o)
                 .build();
    auto add_res = apply_add_objects(std::move(add));
    ASSERT_TRUE(add_res.has_value());

    // Replacement with an inverted extent should be rejected.
    auto replace = replace_objects_builder()
                     .add(new_obj_builder(oid2, 100, 1100)
                            .add(tidp_a, 10_o, 0_o, 1999_t, 0, 99)
                            .build())
                     .build();
    auto replace_res = apply_replace_objects(std::move(replace));
    EXPECT_FALSE(replace_res.has_value());
}

TEST_P(StateUpdateParamTest, TestEmptyReplace) {
    auto add = add_objects_builder()
                 .add(new_obj_builder(oid1, 100, 1100)
                        .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                        .build())
                 .add_term_start(tidp_a, 0_tm, 0_o)
                 .build();
    auto add_res = apply_add_objects(std::move(add));
    ASSERT_TRUE(add_res.has_value());

    auto replace = replace_objects_builder().build();

    auto replace_res = apply_replace_objects(std::move(replace));
    EXPECT_FALSE(replace_res.has_value());
}

TEST_P(StateUpdateParamTest, TestReplaceValidNonContiguous) {
    auto add = add_objects_builder()
                 .add(new_obj_builder(oid1, 100, 1100)
                        .add(tidp_a, 0_o, 99_o, 1999_t, 0, 99)
                        .build())
                 .add(new_obj_builder(oid2, 100, 1100)
                        .add(tidp_a, 100_o, 199_o, 1999_t, 0, 99)
                        .build())
                 .add(new_obj_builder(oid3, 100, 1100)
                        .add(tidp_a, 200_o, 299_o, 1999_t, 0, 99)
                        .build())
                 .add_term_start(tidp_a, 0_tm, 0_o)
                 .build();
    auto add_res = apply_add_objects(std::move(add));
    ASSERT_TRUE(add_res.has_value());

    // Attempt to replace oid1 and oid3 while leaving oid2 in place with a
    // non-contiguous update. While the update itself is non-contiguous, the
    // individual objects still align with existing extents, and is therefore
    // valid.
    auto replace = replace_objects_builder()
                     .add(new_obj_builder(oid4, 100, 1100)
                            .add(tidp_a, 0_o, 99_o, 1999_t, 0, 99)
                            .build())
                     .add(new_obj_builder(oid5, 100, 1100)
                            .add(tidp_a, 200_o, 299_o, 1999_t, 0, 99)
                            .build())
                     .build();

    auto replace_res = apply_replace_objects(std::move(replace));
    ASSERT_TRUE(replace_res.has_value());

    auto& s = get_state();
    auto& p = s.partition_state(model::topic_id_partition::from(tidp_a))->get();
    ASSERT_EQ(p.extents.size(), 3);
}

TEST_P(StateUpdateParamTest, TestReplaceValidNonContiguousSplitExtent) {
    auto add = add_objects_builder()
                 .add(new_obj_builder(oid1, 100, 1100)
                        .add(tidp_a, 0_o, 99_o, 1999_t, 0, 99)
                        .build())
                 .add(new_obj_builder(oid2, 100, 1100)
                        .add(tidp_a, 100_o, 199_o, 1999_t, 0, 99)
                        .build())
                 .add(new_obj_builder(oid3, 100, 1100)
                        .add(tidp_a, 200_o, 299_o, 1999_t, 0, 99)
                        .build())
                 .add_term_start(tidp_a, 0_tm, 0_o)
                 .build();
    auto add_res = apply_add_objects(std::move(add));
    ASSERT_TRUE(add_res.has_value());

    // Attempt to replace oid1 and oid3 while leaving oid2 in place with a
    // non-contiguous update whose objects align with existing extents.
    // The update should see oid3 split into two new extents (for a total of 4
    // extents).
    auto replace = replace_objects_builder()
                     .add(new_obj_builder(oid4, 100, 1100)
                            .add(tidp_a, 0_o, 99_o, 1999_t, 0, 99)
                            .build())
                     .add(new_obj_builder(oid5, 100, 1100)
                            .add(tidp_a, 200_o, 249_o, 1999_t, 0, 99)
                            .build())
                     .add(new_obj_builder(oid6, 100, 1100)
                            .add(tidp_a, 250_o, 299_o, 1999_t, 0, 99)
                            .build())
                     .build();

    auto replace_res = apply_replace_objects(std::move(replace));
    ASSERT_TRUE(replace_res.has_value());

    auto& s = get_state();
    auto& p = s.partition_state(model::topic_id_partition::from(tidp_a))->get();
    ASSERT_EQ(p.extents.size(), 4);
}

TEST_P(StateUpdateParamTest, TestReplaceInvalidNonContiguousBadOffsets) {
    auto add = add_objects_builder()
                 .add(new_obj_builder(oid1, 100, 1100)
                        .add(tidp_a, 0_o, 99_o, 1999_t, 0, 99)
                        .build())
                 .add(new_obj_builder(oid2, 100, 1100)
                        .add(tidp_a, 100_o, 199_o, 1999_t, 0, 99)
                        .build())
                 .add(new_obj_builder(oid3, 100, 1100)
                        .add(tidp_a, 200_o, 299_o, 1999_t, 0, 99)
                        .build())
                 .add_term_start(tidp_a, 0_tm, 0_o)
                 .build();
    auto add_res = apply_add_objects(std::move(add));
    ASSERT_TRUE(add_res.has_value());

    // Attempt to replace oid1 and oid3 while leaving oid2 in place with a
    // invalid non-contiguous update with bad offsets.
    auto replace = replace_objects_builder()
                     .add(new_obj_builder(oid4, 100, 1100)
                            .add(tidp_a, 0_o, 99_o, 1999_t, 0, 99)
                            .build())
                     .add(new_obj_builder(oid5, 100, 1100)
                            .add(tidp_a, 200_o, 249_o, 1999_t, 0, 99)
                            .build())
                     .add(new_obj_builder(oid6, 100, 1100)
                            .add(tidp_a, 239_o, 299_o, 1999_t, 0, 99)
                            .build())
                     .build();

    auto replace_res = apply_replace_objects(std::move(replace));
    EXPECT_FALSE(replace_res.has_value());

    auto& s = get_state();
    auto& p = s.partition_state(model::topic_id_partition::from(tidp_a))->get();
    ASSERT_EQ(p.extents.size(), 3);
}

TEST_P(StateUpdateParamTest, TestReplaceInvalidNonContiguousDoesNotSpan) {
    auto add = add_objects_builder()
                 .add(new_obj_builder(oid1, 100, 1100)
                        .add(tidp_a, 0_o, 99_o, 1999_t, 0, 99)
                        .build())
                 .add(new_obj_builder(oid2, 100, 1100)
                        .add(tidp_a, 100_o, 199_o, 1999_t, 0, 99)
                        .build())
                 .add(new_obj_builder(oid3, 100, 1100)
                        .add(tidp_a, 200_o, 299_o, 1999_t, 0, 99)
                        .build())
                 .add_term_start(tidp_a, 0_tm, 0_o)
                 .build();
    auto add_res = apply_add_objects(std::move(add));
    ASSERT_TRUE(add_res.has_value());

    // Attempt to replace oid1 and oid3 while leaving oid2 in place with a
    // invalid non-contiguous update that doesn't exactly span existing extents.
    auto replace = replace_objects_builder()
                     .add(new_obj_builder(oid4, 100, 1100)
                            .add(tidp_a, 0_o, 99_o, 1999_t, 0, 99)
                            .build())
                     .add(new_obj_builder(oid5, 100, 1100)
                            .add(tidp_a, 200_o, 249_o, 1999_t, 0, 99)
                            .build())
                     .add(new_obj_builder(oid6, 100, 1100)
                            .add(tidp_a, 250_o, 298_o, 1999_t, 0, 99)
                            .build())
                     .build();

    auto replace_res = apply_replace_objects(std::move(replace));
    EXPECT_FALSE(replace_res.has_value());

    auto& s = get_state();
    auto& p = s.partition_state(model::topic_id_partition::from(tidp_a))->get();
    ASSERT_EQ(p.extents.size(), 3);
}

TEST_P(StateUpdateParamTest, TestReplaceSingleExtentBeforeNewStartOffset) {
    auto add = add_objects_builder()
                 .add(new_obj_builder(oid1, 100, 1100)
                        .add(tidp_a, 0_o, 99_o, 1999_t, 0, 99)
                        .build())
                 .add(new_obj_builder(oid2, 100, 1100)
                        .add(tidp_a, 100_o, 199_o, 1999_t, 0, 99)
                        .build())
                 .add(new_obj_builder(oid3, 100, 1100)
                        .add(tidp_a, 200_o, 299_o, 1999_t, 0, 99)
                        .build())
                 .add_term_start(tidp_a, 0_tm, 0_o)
                 .build();
    auto add_res = apply_add_objects(std::move(add));
    ASSERT_TRUE(add_res.has_value());

    auto tp = model::topic_id_partition::from(tidp_a);
    auto set_start_res = apply_set_start_offset(tp, 150_o);
    ASSERT_TRUE(set_start_res.has_value());

    // Attempt to replace with a list of valid objects, with one extent
    // containing offsets before the truncation point (the new start offset).
    // The metastore should be able to apply the update by ignoring the first
    // extent.
    auto replace = replace_objects_builder()
                     .add(new_obj_builder(oid4, 100, 1100)
                            .add(tidp_a, 0_o, 99_o, 1999_t, 0, 99)
                            .build())
                     .add(new_obj_builder(oid5, 100, 1100)
                            .add(tidp_a, 100_o, 199_o, 1999_t, 0, 99)
                            .build())
                     .add(new_obj_builder(oid6, 100, 1100)
                            .add(tidp_a, 200_o, 299_o, 1999_t, 0, 99)
                            .build())
                     .build();

    auto replace_res = apply_replace_objects(std::move(replace));
    ASSERT_TRUE(replace_res.has_value());
}

TEST_P(
  StateUpdateParamTest, TestReplaceWithAlignedExtentsBeforeNewStartOffset) {
    auto add = add_objects_builder()
                 .add(new_obj_builder(oid1, 100, 1100)
                        .add(tidp_a, 0_o, 30_o, 1999_t, 0, 99)
                        .build())
                 .add(new_obj_builder(oid2, 100, 1100)
                        .add(tidp_a, 31_o, 60_o, 1999_t, 0, 99)
                        .build())
                 .add(new_obj_builder(oid3, 100, 1100)
                        .add(tidp_a, 61_o, 100_o, 1999_t, 0, 99)
                        .build())
                 .add_term_start(tidp_a, 0_tm, 0_o)
                 .build();
    auto add_res = apply_add_objects(std::move(add));
    ASSERT_TRUE(add_res.has_value());

    auto tp = model::topic_id_partition::from(tidp_a);
    auto set_start_res = apply_set_start_offset(tp, 61_o);
    ASSERT_TRUE(set_start_res.has_value());

    // Attempt to replace with a list of valid objects built before the
    // truncation occurred. The metastore should be able to accept and prune the
    // extents below the start offset.
    auto replace = replace_objects_builder()
                     .add(new_obj_builder(oid4, 100, 1100)
                            .add(tidp_a, 0_o, 30_o, 1999_t, 0, 99)
                            .build())
                     .add(new_obj_builder(oid5, 100, 1100)
                            .add(tidp_a, 31_o, 60_o, 1999_t, 0, 99)
                            .build())
                     .add(new_obj_builder(oid6, 100, 1100)
                            .add(tidp_a, 61_o, 100_o, 1999_t, 0, 99)
                            .build())
                     .build();

    auto replace_res = apply_replace_objects(std::move(replace));
    ASSERT_TRUE(replace_res.has_value());

    auto& s = get_state();
    auto p_state = s.partition_state(tp);
    ASSERT_TRUE(p_state.has_value());
    EXPECT_EQ(p_state->get().extents.size(), 1);
}

TEST_P(StateUpdateParamTest, TestReplaceAllExtentsBeforeNewStartOffset) {
    auto add = add_objects_builder()
                 .add(new_obj_builder(oid1, 100, 1100)
                        .add(tidp_a, 0_o, 99_o, 1999_t, 0, 99)
                        .build())
                 .add(new_obj_builder(oid2, 100, 1100)
                        .add(tidp_a, 100_o, 199_o, 1999_t, 0, 99)
                        .build())
                 .add(new_obj_builder(oid3, 100, 1100)
                        .add(tidp_a, 200_o, 299_o, 1999_t, 0, 99)
                        .build())
                 .add_term_start(tidp_a, 0_tm, 0_o)
                 .build();
    auto add_res = apply_add_objects(std::move(add));
    ASSERT_TRUE(add_res.has_value());

    auto tp = model::topic_id_partition::from(tidp_a);
    auto set_start_res = apply_set_start_offset(tp, 300_o);
    ASSERT_TRUE(set_start_res.has_value());

    // Attempt to replace with a list of valid objects, with all extents
    // containing offsets before the truncation point (the new start offset).
    // The metastore should be able to recognize this is a no-op.
    auto replace = replace_objects_builder()
                     .add(new_obj_builder(oid4, 100, 1100)
                            .add(tidp_a, 0_o, 99_o, 1999_t, 0, 99)
                            .build())
                     .add(new_obj_builder(oid5, 100, 1100)
                            .add(tidp_a, 100_o, 199_o, 1999_t, 0, 99)
                            .build())
                     .add(new_obj_builder(oid6, 100, 1100)
                            .add(tidp_a, 200_o, 299_o, 1999_t, 0, 99)
                            .build())
                     .build();

    auto replace_res = apply_replace_objects(std::move(replace));
    ASSERT_FALSE(replace_res.has_value());
}

TEST_P(StateUpdateParamTest, TestReplaceMultipleExtentsBeforeNewStartOffset) {
    auto add = add_objects_builder()
                 .add(new_obj_builder(oid1, 100, 1100)
                        .add(tidp_a, 0_o, 100_o, 1999_t, 0, 99)
                        .build())
                 .add_term_start(tidp_a, 0_tm, 0_o)
                 .build();
    auto add_res = apply_add_objects(std::move(add));
    ASSERT_TRUE(add_res.has_value());

    auto tp = model::topic_id_partition::from(tidp_a);
    auto set_start_res = apply_set_start_offset(tp, 75_o);
    ASSERT_TRUE(set_start_res.has_value());

    // Attempt to replace with a list of valid objects, with some extents
    // containing offsets before the truncation point (the new start offset).
    // If we fail to consider the fact that the extent {0,100} is still
    // present in the existing state, removing the first two extents would
    // result in a misaligned update and a failure to replace.
    auto replace = replace_objects_builder()
                     .add(new_obj_builder(oid2, 100, 1100)
                            .add(tidp_a, 0_o, 30_o, 1999_t, 0, 99)
                            .build())
                     .add(new_obj_builder(oid3, 100, 1100)
                            .add(tidp_a, 31_o, 60_o, 1999_t, 0, 99)
                            .build())
                     .add(new_obj_builder(oid4, 100, 1100)
                            .add(tidp_a, 61_o, 100_o, 1999_t, 0, 99)
                            .build())
                     .build();

    auto replace_res = apply_replace_objects(std::move(replace));
    ASSERT_TRUE(replace_res.has_value());

    auto& s = get_state();
    auto p_state = s.partition_state(tp);
    ASSERT_TRUE(p_state.has_value());
    EXPECT_EQ(p_state->get().extents.size(), 1);
}

TEST_P(
  StateUpdateParamTest, TestReplaceMisalignedButContiguousWithNewStartOffset) {
    auto add = add_objects_builder()
                 .add(new_obj_builder(oid1, 100, 1100)
                        .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                        .build())
                 .add(new_obj_builder(oid2, 100, 1100)
                        .add(tidp_a, 11_o, 20_o, 1999_t, 0, 99)
                        .build())
                 .add(new_obj_builder(oid3, 100, 1100)
                        .add(tidp_a, 21_o, 30_o, 1999_t, 0, 99)
                        .build())
                 .add_term_start(tidp_a, 0_tm, 0_o)
                 .build();

    auto add_res = apply_add_objects(std::move(add));
    ASSERT_TRUE(add_res.has_value());

    auto tp = model::topic_id_partition::from(tidp_a);
    auto set_start_res = apply_set_start_offset(tp, 15_o);
    ASSERT_TRUE(set_start_res.has_value());

    // Attempt to replace with a list of extents that are misaligned to the
    // existing extents ([11,20],[21,30]), but form a valid, contiguous update
    // when considering the new start offset.
    auto replace = replace_objects_builder()
                     .add(new_obj_builder(oid4, 100, 1100)
                            .add(tidp_a, 0_o, 15_o, 1999_t, 0, 99)
                            .build())
                     .add(new_obj_builder(oid5, 100, 1100)
                            .add(tidp_a, 16_o, 30_o, 1999_t, 0, 99)
                            .build())
                     .build();

    auto replace_res = apply_replace_objects(std::move(replace));
    ASSERT_TRUE(replace_res.has_value());

    auto& s = get_state();
    auto p_state = s.partition_state(tp);
    ASSERT_TRUE(p_state.has_value());
    EXPECT_EQ(p_state->get().extents.size(), 2);
}

TEST_P(StateUpdateParamTest, TestReplaceWithCompaction) {
    using testing::ElementsAre;
    using range = struct compaction_state_update::cleaned_range;
    auto add = add_objects_builder()
                 .add(new_obj_builder(oid1, 300, 1300)
                        .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                        .add(tidp_b, 0_o, 10_o, 1999_t, 100, 199)
                        .add(tidp_c, 0_o, 10_o, 1999_t, 200, 299)
                        .build())
                 .add_term_start(tidp_a, 0_tm, 0_o)
                 .add_term_start(tidp_b, 0_tm, 0_o)
                 .add_term_start(tidp_c, 0_tm, 0_o)
                 .build();
    auto add_res = apply_add_objects(std::move(add));
    ASSERT_TRUE(add_res.has_value());

    // Fully replace partition a and clean part of it.
    auto replace
      = replace_objects_builder()
          .add(new_obj_builder(oid2, 100, 1100)
                 .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                 .build())
          .clean(
            tidp_a,
            range{
              .base_offset = 5_o, .last_offset = 10_o, .has_tombstones = true},
            1999_t)
          .set_expected_epoch(tidp_a, partition_state::compaction_epoch_t{0})
          .build();

    auto replace_res = apply_replace_objects(std::move(replace));
    ASSERT_TRUE(replace_res.has_value());

    // Compact an extent, marking [5, 10] cleaned with tombstones.
    {
        auto& s = get_state();
        const auto& prt_a
          = s.partition_state(model::topic_id_partition::from(tidp_a))->get();
        ASSERT_TRUE(prt_a.compaction_state.has_value());
        EXPECT_THAT(prt_a.extents, ElementsAre(MatchesRange(oid2, 0_o, 10_o)));
        EXPECT_THAT(
          prt_a.compaction_state->cleaned_ranges.to_vec(),
          ElementsAre(MatchesRange(5_o, 10_o)));
        EXPECT_THAT(
          prt_a.compaction_state->cleaned_ranges_with_tombstones,
          ElementsAre(MatchesRange(5_o, 10_o)));
        EXPECT_EQ(
          prt_a.compaction_epoch, partition_state::compaction_epoch_t{1});
    }

    // Compact an extent, marking [3, 4] cleaned with tombstones.
    replace
      = replace_objects_builder()
          .add(new_obj_builder(oid3, 100, 1100)
                 .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                 .build())
          .clean(
            tidp_a,
            range{
              .base_offset = 3_o, .last_offset = 4_o, .has_tombstones = true},
            1999_t)
          .set_expected_epoch(tidp_a, partition_state::compaction_epoch_t{1})
          .build();
    replace_res = apply_replace_objects(std::move(replace));
    ASSERT_TRUE(replace_res.has_value());

    {
        auto& s = get_state();
        const auto& prt_a
          = s.partition_state(model::topic_id_partition::from(tidp_a))->get();
        EXPECT_THAT(prt_a.extents, ElementsAre(MatchesRange(oid3, 0_o, 10_o)));
        EXPECT_THAT(
          prt_a.compaction_state->cleaned_ranges.to_vec(),
          ElementsAre(MatchesRange(3_o, 10_o)));
        EXPECT_THAT(
          prt_a.compaction_state->cleaned_ranges_with_tombstones,
          ElementsAre(MatchesRange(3_o, 4_o), MatchesRange(5_o, 10_o)));
        EXPECT_EQ(
          prt_a.compaction_epoch, partition_state::compaction_epoch_t{2});
    }

    // Now mark [3, 8] as having removed tombstones.
    replace = replace_objects_builder()
                .add(new_obj_builder(oid4, 100, 1100)
                       .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                       .build())
                .clean_tombstones(tidp_a, 3_o, 8_o)
                .set_expected_epoch(
                  tidp_a, partition_state::compaction_epoch_t{2})
                .build();
    replace_res = apply_replace_objects(std::move(replace));
    ASSERT_TRUE(replace_res.has_value()) << replace_res.error();

    auto& s = get_state();
    const auto& prt_a
      = s.partition_state(model::topic_id_partition::from(tidp_a))->get();
    EXPECT_THAT(prt_a.extents, ElementsAre(MatchesRange(oid4, 0_o, 10_o)));
    EXPECT_THAT(
      prt_a.compaction_state->cleaned_ranges.to_vec(),
      ElementsAre(MatchesRange(3_o, 10_o)));
    EXPECT_THAT(
      prt_a.compaction_state->cleaned_ranges_with_tombstones,
      ElementsAre(MatchesRange(9_o, 10_o)));
    EXPECT_EQ(prt_a.compaction_epoch, partition_state::compaction_epoch_t{3});
}

TEST_P(StateUpdateParamTest, TestCompactionMissingExtent) {
    using range = struct compaction_state_update::cleaned_range;
    auto add = add_objects_builder()
                 .add(new_obj_builder(oid1, 300, 1300)
                        .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                        .add(tidp_b, 0_o, 10_o, 1999_t, 100, 199)
                        .build())
                 .add_term_start(tidp_a, 0_tm, 0_o)
                 .add_term_start(tidp_b, 0_tm, 0_o)
                 .build();
    auto add_res = apply_add_objects(std::move(add));
    ASSERT_TRUE(add_res.has_value());

    // Add a clean range for tidp_a but only supply an extent with tidp_b.
    auto replace
      = replace_objects_builder()
          .add(new_obj_builder(oid2, 100, 1100)
                 .add(tidp_b, 0_o, 10_o, 1999_t, 0, 99)
                 .build())
          .clean(
            tidp_a,
            range{
              .base_offset = 5_o, .last_offset = 10_o, .has_tombstones = true},
            1999_t)
          .set_expected_epoch(tidp_a, partition_state::compaction_epoch_t{0})
          .build();
    auto replace_res = apply_replace_objects(std::move(replace));
    EXPECT_FALSE(replace_res.has_value());
}

TEST_P(StateUpdateParamTest, TestCompactionDoesntReplaceExtents) {
    using range = struct compaction_state_update::cleaned_range;
    auto add = add_objects_builder()
                 .add(new_obj_builder(oid1, 300, 1300)
                        .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                        .add(tidp_b, 0_o, 10_o, 1999_t, 100, 199)
                        .build())
                 .add_term_start(tidp_a, 0_tm, 0_o)
                 .add_term_start(tidp_b, 0_tm, 0_o)
                 .build();
    auto add_res = apply_add_objects(std::move(add));
    ASSERT_TRUE(add_res.has_value());

    auto replace
      = replace_objects_builder()
          .add(new_obj_builder(oid3, 100, 1100)
                 .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                 .build())
          .clean(
            tidp_a,
            range{
              .base_offset = 5_o, .last_offset = 11_o, .has_tombstones = true},
            1999_t)
          .set_expected_epoch(tidp_a, partition_state::compaction_epoch_t{0})
          .build();
    auto replace_res = apply_replace_objects(std::move(replace));
    EXPECT_FALSE(replace_res.has_value());
}

TEST_P(StateUpdateParamTest, TestCompactionDoesntReplaceExtentsStart) {
    using range = struct compaction_state_update::cleaned_range;
    auto add = add_objects_builder()
                 .add(new_obj_builder(oid1, 300, 1300)
                        .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                        .add(tidp_b, 0_o, 10_o, 1999_t, 100, 199)
                        .build())
                 .add_term_start(tidp_a, 0_tm, 0_o)
                 .add_term_start(tidp_b, 0_tm, 0_o)
                 .build();
    auto add_res = apply_add_objects(std::move(add));
    ASSERT_TRUE(add_res.has_value());
    add = add_objects_builder()
            .add(new_obj_builder(oid2, 300, 1300)
                   .add(tidp_a, 11_o, 20_o, 1999_t, 0, 99)
                   .build())
            .add_term_start(tidp_a, 0_tm, 11_o)
            .build();
    add_res = apply_add_objects(std::move(add));
    ASSERT_TRUE(add_res.has_value());

    // Add a replacement extent and claim that it cleans a larger offset range.
    auto replace
      = replace_objects_builder()
          .add(new_obj_builder(oid3, 100, 1100)
                 .add(tidp_a, 11_o, 20_o, 1999_t, 0, 99)
                 .build())
          .clean(
            tidp_a,
            range{
              .base_offset = 0_o, .last_offset = 20_o, .has_tombstones = true},
            1999_t)
          .set_expected_epoch(tidp_a, partition_state::compaction_epoch_t{0})
          .build();
    auto replace_res = apply_replace_objects(std::move(replace));
    EXPECT_FALSE(replace_res.has_value());
}

TEST_P(StateUpdateParamTest, TestCompactionDoesntReplaceLogStart) {
    using range = struct compaction_state_update::cleaned_range;
    auto add = add_objects_builder()
                 .add(new_obj_builder(oid1, 300, 1300)
                        .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                        .add(tidp_b, 0_o, 10_o, 1999_t, 100, 199)
                        .build())
                 .add_term_start(tidp_a, 0_tm, 0_o)
                 .add_term_start(tidp_b, 0_tm, 0_o)
                 .build();
    auto add_res = apply_add_objects(std::move(add));
    ASSERT_TRUE(add_res.has_value());
    add = add_objects_builder()
            .add(new_obj_builder(oid2, 300, 1300)
                   .add(tidp_a, 11_o, 20_o, 1999_t, 0, 99)
                   .build())
            .add_term_start(tidp_a, 0_tm, 11_o)
            .build();
    add_res = apply_add_objects(std::move(add));
    ASSERT_TRUE(add_res.has_value());

    // Add a replacement extent and claim that it makes a larger range clean.
    auto replace
      = replace_objects_builder()
          .add(new_obj_builder(oid3, 100, 1100)
                 .add(tidp_a, 11_o, 20_o, 1999_t, 0, 99)
                 .build())
          .clean(
            tidp_a,
            range{
              .base_offset = 11_o, .last_offset = 20_o, .has_tombstones = true},
            1999_t)
          .set_expected_epoch(tidp_a, partition_state::compaction_epoch_t{0})
          .build();
    auto replace_res = apply_replace_objects(std::move(replace));
    EXPECT_FALSE(replace_res.has_value());
}

TEST_P(StateUpdateParamTest, TestOverlappingTombstones) {
    using range = struct compaction_state_update::cleaned_range;
    auto add = add_objects_builder()
                 .add(new_obj_builder(oid1, 300, 1300)
                        .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                        .add(tidp_b, 0_o, 10_o, 1999_t, 100, 199)
                        .build())
                 .add_term_start(tidp_a, 0_tm, 0_o)
                 .add_term_start(tidp_b, 0_tm, 0_o)
                 .build();
    auto add_res = apply_add_objects(std::move(add));
    ASSERT_TRUE(add_res.has_value());

    auto replace
      = replace_objects_builder()
          .add(new_obj_builder(oid2, 100, 1100)
                 .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                 .build())
          .clean(
            tidp_a,
            range{
              .base_offset = 5_o, .last_offset = 10_o, .has_tombstones = true},
            1999_t)
          .set_expected_epoch(tidp_a, partition_state::compaction_epoch_t{0})
          .build();
    auto replace_res = apply_replace_objects(std::move(replace));
    ASSERT_TRUE(replace_res.has_value());

    replace
      = replace_objects_builder()
          .add(new_obj_builder(oid3, 100, 1100)
                 .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                 .build())
          .clean(
            tidp_a,
            range{
              .base_offset = 10_o, .last_offset = 10_o, .has_tombstones = true},
            1999_t)
          .set_expected_epoch(tidp_a, partition_state::compaction_epoch_t{1})
          .build();
    replace_res = apply_replace_objects(std::move(replace));
    EXPECT_FALSE(replace_res.has_value());
}

TEST_P(StateUpdateParamTest, TestRemoveNonExistingTombstones) {
    auto add = add_objects_builder()
                 .add(new_obj_builder(oid1, 300, 1300)
                        .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                        .add(tidp_b, 0_o, 10_o, 1999_t, 100, 199)
                        .build())
                 .add_term_start(tidp_a, 0_tm, 0_o)
                 .add_term_start(tidp_b, 0_tm, 0_o)
                 .build();
    auto add_res = apply_add_objects(std::move(add));
    ASSERT_TRUE(add_res.has_value());

    auto replace = replace_objects_builder()
                     .add(new_obj_builder(oid2, 100, 1100)
                            .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                            .build())
                     .clean_tombstones(tidp_a, 5_o, 10_o)
                     .set_expected_epoch(
                       tidp_a, partition_state::compaction_epoch_t{0})
                     .build();
    auto replace_res = apply_replace_objects(std::move(replace));
    EXPECT_FALSE(replace_res.has_value());
}

TEST_P(StateUpdateParamTest, TestAddIncreasingTerms) {
    auto update = add_objects_builder()
                    .add(new_obj_builder(oid1, 100, 1100)
                           .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                           .build())
                    .add_term_start(tidp_a, 1_tm, 0_o)
                    .add_term_start(tidp_a, 2_tm, 1_o)
                    .build();
    auto res = apply_add_objects(std::move(update));
    EXPECT_TRUE(res.has_value());
    EXPECT_EQ(1, get_state().topic_to_state.size());

    update = add_objects_builder()
               .add(new_obj_builder(oid2, 100, 1100)
                      .add(tidp_a, 11_o, 20_o, 1999_t, 0, 99)
                      .build())
               .add_term_start(tidp_a, 3_tm, 11_o)
               .add_term_start(tidp_a, 4_tm, 12_o)
               .build();
    res = apply_add_objects(std::move(update));
    EXPECT_TRUE(res.has_value());

    auto& s = get_state();
    auto p_state = s.partition_state(model::topic_id_partition::from(tidp_a));
    ASSERT_TRUE(p_state.has_value());
    EXPECT_EQ(2, p_state->get().extents.size());
    EXPECT_EQ(4, p_state->get().term_starts.size());
    EXPECT_THAT(
      p_state->get().term_starts,
      testing::ElementsAre(
        MatchesTermStart(1_tm, 0_o),
        MatchesTermStart(2_tm, 1_o),
        MatchesTermStart(3_tm, 11_o),
        MatchesTermStart(4_tm, 12_o)));
}

TEST_P(StateUpdateParamTest, TestAddSameSubsequentTerm) {
    auto update = add_objects_builder()
                    .add(new_obj_builder(oid1, 100, 1100)
                           .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                           .build())
                    .add_term_start(tidp_a, 1_tm, 0_o)
                    .add_term_start(tidp_a, 2_tm, 1_o)
                    .build();
    auto res = apply_add_objects(std::move(update));
    EXPECT_TRUE(res.has_value());
    {
        auto& s = get_state();
        EXPECT_EQ(1, s.topic_to_state.size());
        auto p_state = s.partition_state(
          model::topic_id_partition::from(tidp_a));
        ASSERT_TRUE(p_state.has_value());
        EXPECT_EQ(1, p_state->get().extents.size());
        EXPECT_EQ(2, p_state->get().term_starts.size());
        EXPECT_THAT(
          p_state->get().term_starts,
          testing::ElementsAre(
            MatchesTermStart(1_tm, 0_o), MatchesTermStart(2_tm, 1_o)));
    }

    // The start of term 2 shouldn't be changed, but term 3 should be added.
    update = add_objects_builder()
               .add(new_obj_builder(oid2, 100, 1100)
                      .add(tidp_a, 11_o, 20_o, 1999_t, 0, 99)
                      .build())
               .add_term_start(tidp_a, 2_tm, 11_o)
               .add_term_start(tidp_a, 3_tm, 12_o)
               .build();
    res = apply_add_objects(std::move(update));
    EXPECT_TRUE(res.has_value());

    auto& s = get_state();
    auto p_state = s.partition_state(model::topic_id_partition::from(tidp_a));
    ASSERT_TRUE(p_state.has_value());
    EXPECT_EQ(2, p_state->get().extents.size());
    EXPECT_EQ(3, p_state->get().term_starts.size());
    EXPECT_THAT(
      p_state->get().term_starts,
      testing::ElementsAre(
        MatchesTermStart(1_tm, 0_o),
        MatchesTermStart(2_tm, 1_o),
        MatchesTermStart(3_tm, 12_o)));
}

TEST_P(StateUpdateParamTest, TestAddNoTerms) {
    ASSERT_TRUE(apply_preregister_objects({oid1}).has_value());
    auto update = add_objects_builder()
                    .add(new_obj_builder(oid1, 100, 1100)
                           .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                           .build())
                    .build();

    auto res = can_apply_add_objects(std::move(update));
    EXPECT_FALSE(res.has_value());
}

TEST_P(StateUpdateParamTest, TestAddMissingTermsForPartition) {
    ASSERT_TRUE(apply_preregister_objects({oid1}).has_value());
    auto update = add_objects_builder()
                    .add(new_obj_builder(oid1, 100, 1100)
                           .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                           .add(tidp_b, 0_o, 10_o, 1999_t, 100, 199)
                           .build())
                    .add_term_start(tidp_a, 0_tm, 0_o)
                    .build();

    auto res = can_apply_add_objects(std::move(update));
    EXPECT_FALSE(res.has_value());
}

TEST_P(StateUpdateParamTest, TestAddDecreasingTermInUpdate) {
    auto update = add_objects_builder()
                    .add(new_obj_builder(oid1, 100, 1100)
                           .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                           .build())
                    .add_term_start(tidp_a, 2_tm, 0_o)
                    .add_term_start(tidp_a, 1_tm, 1_o)
                    .build();
    auto res = apply_add_objects(std::move(update));
    EXPECT_FALSE(res.has_value());
}

TEST_P(StateUpdateParamTest, TestAddDecreasingTerm) {
    auto update = add_objects_builder()
                    .add(new_obj_builder(oid1, 100, 1100)
                           .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                           .build())
                    .add_term_start(tidp_a, 2_tm, 0_o)
                    .build();
    auto res = apply_add_objects(std::move(update));
    EXPECT_TRUE(res.has_value());

    ASSERT_TRUE(apply_preregister_objects({oid2}).has_value());
    update = add_objects_builder()
               .add(new_obj_builder(oid2, 100, 1100)
                      .add(tidp_a, 11_o, 20_o, 1999_t, 0, 99)
                      .build())
               .add_term_start(tidp_a, 1_tm, 11_o)
               .build();
    res = can_apply_add_objects(std::move(update));
    EXPECT_FALSE(res.has_value());
}

TEST_P(StateUpdateParamTest, TestRejectBogusTermWithBogusExtent) {
    auto update = add_objects_builder()
                    .add(new_obj_builder(oid1, 100, 1100)
                           .add(tidp_a, 10_o, 10_o, 1999_t, 0, 99)
                           .build())
                    .add_term_start(tidp_a, 2_tm, 10_o)
                    .add_term_start(tidp_a, 1_tm, 10_o)
                    .build();
    // Invalid terms (same offset for different terms) should be rejected
    // regardless of whether the extent is misaligned.
    auto res = apply_add_objects(std::move(update));
    EXPECT_FALSE(res.has_value());
}

TEST_P(StateUpdateParamTest, TestTermsWithNoExtent) {
    ASSERT_TRUE(apply_preregister_objects({oid1}).has_value());
    auto update = add_objects_builder()
                    .add(new_obj_builder(oid1, 100, 1100)
                           .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                           .build())
                    .add_term_start(tidp_a, 0_tm, 0_o)
                    // Add some terms for a missing partition.
                    .add_term_start(tidp_b, 0_tm, 0_o)
                    .build();
    auto res = can_apply_add_objects(std::move(update));
    EXPECT_FALSE(res.has_value());
}

TEST_P(StateUpdateParamTest, TestAddMismatchedStartOffset) {
    auto update = add_objects_builder()
                    .add(new_obj_builder(oid1, 100, 1100)
                           .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                           .build())
                    .add_term_start(tidp_a, 1_tm, 0_o)
                    .build();
    auto res = apply_add_objects(std::move(update));
    EXPECT_TRUE(res.has_value());

    // Add an update where the term's start offset doesn't match the extent.
    ASSERT_TRUE(apply_preregister_objects({oid2}).has_value());
    update = add_objects_builder()
               .add(new_obj_builder(oid2, 100, 1100)
                      .add(tidp_a, 11_o, 20_o, 1999_t, 0, 99)
                      .build())
               .add_term_start(tidp_a, 2_tm, 0_o)
               .build();
    res = can_apply_add_objects(std::move(update));
    EXPECT_FALSE(res.has_value());
}

TEST_P(StateUpdateParamTest, TestAddExtentEndsBelowLastTermStart) {
    auto update = add_objects_builder()
                    .add(new_obj_builder(oid1, 100, 1100)
                           .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                           .build())
                    .add_term_start(tidp_a, 1_tm, 0_o)
                    // We can add at the last offset.
                    .add_term_start(tidp_a, 2_tm, 10_o)
                    .build();
    auto res = apply_add_objects(std::move(update));
    EXPECT_TRUE(res.has_value());

    update = add_objects_builder()
               .add(new_obj_builder(oid2, 100, 1100)
                      .add(tidp_a, 11_o, 20_o, 1999_t, 0, 99)
                      .build())
               .add_term_start(tidp_a, 3_tm, 11_o)
               // We cannot past the last offset.
               .add_term_start(tidp_a, 4_tm, 21_o)
               .build();
    res = apply_add_objects(std::move(update));
    EXPECT_FALSE(res.has_value());
}

TEST_P(StateUpdateParamTest, TestSetStartOffsetAlignedWithExtent) {
    // Add some extents
    auto add_update = add_objects_builder()
                        .add(new_obj_builder(oid1, 100, 1100)
                               .add(tidp_a, 0_o, 10_o, 2000_t, 0, 99)
                               .build())
                        .add(new_obj_builder(oid2, 100, 1100)
                               .add(tidp_a, 11_o, 20_o, 2000_t, 0, 99)
                               .build())
                        .add(new_obj_builder(oid3, 100, 1100)
                               .add(tidp_a, 21_o, 30_o, 2000_t, 0, 99)
                               .build())
                        .add_term_start(tidp_a, 0_tm, 0_o)
                        .build();
    auto res = apply_add_objects(std::move(add_update));
    ASSERT_TRUE(res.has_value());

    auto tp = model::topic_id_partition::from(tidp_a);
    auto p_state = get_state().partition_state(tp);
    ASSERT_TRUE(p_state.has_value());
    EXPECT_EQ(3, p_state->get().extents.size());

    // Set start offset to be aligned with the second extent (starts at 11)
    auto apply_res = apply_set_start_offset(tp, 11_o);
    ASSERT_TRUE(apply_res.has_value());

    // Verify that the state has been updated correctly
    p_state = get_state().partition_state(tp);
    ASSERT_TRUE(p_state.has_value());
    EXPECT_EQ(11_o, p_state->get().start_offset);
    EXPECT_EQ(31_o, p_state->get().next_offset);

    // The first extent should be fully removed from accounting
    // Only extents 2 and 3 should remain
    EXPECT_EQ(2, p_state->get().extents.size());
}

TEST_P(StateUpdateParamTest, TestSetStartOffsetNotAlignedWithExtent) {
    // Add some extents
    auto add_update = add_objects_builder()
                        .add(new_obj_builder(oid1, 100, 1100)
                               .add(tidp_a, 0_o, 10_o, 2000_t, 0, 99)
                               .build())
                        .add(new_obj_builder(oid2, 100, 1100)
                               .add(tidp_a, 11_o, 20_o, 2000_t, 0, 99)
                               .build())
                        .add(new_obj_builder(oid3, 100, 1100)
                               .add(tidp_a, 21_o, 30_o, 2000_t, 0, 99)
                               .build())
                        .add_term_start(tidp_a, 0_tm, 0_o)
                        .build();
    auto res = apply_add_objects(std::move(add_update));
    ASSERT_TRUE(res.has_value());

    auto tp = model::topic_id_partition::from(tidp_a);
    auto p_state = get_state().partition_state(tp);
    ASSERT_TRUE(p_state.has_value());
    EXPECT_EQ(3, p_state->get().extents.size());

    // Set start offset to be not aligned with any extent (offset 15 is in the
    // middle of second extent)
    auto apply_res = apply_set_start_offset(tp, 15_o);
    ASSERT_TRUE(apply_res.has_value());

    // Verify that the state has been updated correctly
    p_state = get_state().partition_state(tp);
    ASSERT_TRUE(p_state.has_value());
    EXPECT_EQ(15_o, p_state->get().start_offset);
    EXPECT_EQ(31_o, p_state->get().next_offset);

    // The first extent should be fully removed, but the second and third should
    // remain even though start is not aligned with the second extent's boundary
    EXPECT_EQ(2, p_state->get().extents.size());
}

TEST_P(StateUpdateParamTest, TestSetStartOffsetEmptyWithTerms) {
    // Add extents with various terms
    auto add_update = add_objects_builder()
                        .add(new_obj_builder(oid1, 100, 1100)
                               .add(tidp_a, 0_o, 9_o, 2000_t, 0, 99)
                               .build())
                        .add_term_start(tidp_a, 1_tm, 0_o)
                        .add_term_start(tidp_a, 2_tm, 3_o)
                        .add_term_start(tidp_a, 5_tm, 7_o)
                        .build();
    auto res = apply_add_objects(std::move(add_update));
    ASSERT_TRUE(res.has_value());

    auto tp = model::topic_id_partition::from(tidp_a);
    auto p_state = get_state().partition_state(tp);
    ASSERT_TRUE(p_state.has_value());
    EXPECT_EQ(1, p_state->get().extents.size());
    EXPECT_EQ(3, p_state->get().term_starts.size());

    // Set start offset beyond the end of all extents to make log empty.
    auto apply_res = apply_set_start_offset(tp, 10_o);
    ASSERT_TRUE(apply_res.has_value());

    p_state = get_state().partition_state(tp);
    ASSERT_TRUE(p_state.has_value());
    EXPECT_EQ(10_o, p_state->get().start_offset);
    EXPECT_EQ(10_o, p_state->get().next_offset);

    // All extents should be removed since start is beyond them.
    EXPECT_EQ(0, p_state->get().extents.size());

    // One term information should still be preserved to be able to serve term
    // queries at the next offset.
    EXPECT_EQ(1, p_state->get().term_starts.size());
}

TEST_P(StateUpdateParamTest, TestSetStartOffsetWithCompactionState) {
    using testing::ElementsAre;
    using range = struct compaction_state_update::cleaned_range;

    // Add an extent and then compact it
    auto add_update = add_objects_builder()
                        .add(new_obj_builder(oid1, 100, 1100)
                               .add(tidp_a, 0_o, 20_o, 2000_t, 0, 99)
                               .build())
                        .add_term_start(tidp_a, 0_tm, 0_o)
                        .build();
    auto res = apply_add_objects(std::move(add_update));
    ASSERT_TRUE(res.has_value());

    // Compact part of the extent (clean offsets [5, 15])
    auto replace_update
      = replace_objects_builder()
          .add(new_obj_builder(oid2, 100, 1100)
                 .add(tidp_a, 0_o, 20_o, 2000_t, 0, 99)
                 .build())
          .clean(
            tidp_a,
            range{
              .base_offset = 5_o, .last_offset = 15_o, .has_tombstones = true},
            3000_t)
          .set_expected_epoch(tidp_a, partition_state::compaction_epoch_t{0})
          .build();
    auto replace_res = apply_replace_objects(std::move(replace_update));
    ASSERT_TRUE(replace_res.has_value());

    auto tp = model::topic_id_partition::from(tidp_a);
    auto p_state = get_state().partition_state(tp);
    ASSERT_TRUE(p_state.has_value());
    ASSERT_TRUE(p_state->get().compaction_state.has_value());

    // Verify initial compaction state
    EXPECT_THAT(
      p_state->get().compaction_state->cleaned_ranges.to_vec(),
      ElementsAre(MatchesRange(5_o, 15_o)));
    EXPECT_THAT(
      p_state->get().compaction_state->cleaned_ranges_with_tombstones,
      ElementsAre(MatchesRange(5_o, 15_o)));
    EXPECT_EQ(
      p_state->get().compaction_epoch, partition_state::compaction_epoch_t{1});

    // Set start offset to fall within the cleaned range (offset 10)
    auto apply_res = apply_set_start_offset(tp, 10_o);
    ASSERT_TRUE(apply_res.has_value());

    // Verify that the state has been updated correctly
    p_state = get_state().partition_state(tp);
    ASSERT_TRUE(p_state.has_value());
    EXPECT_EQ(10_o, p_state->get().start_offset);
    EXPECT_EQ(21_o, p_state->get().next_offset);

    // Check that compaction state reflects the new start
    // The cleaned ranges should be adjusted to reflect the new start
    ASSERT_TRUE(p_state->get().compaction_state.has_value());
    EXPECT_THAT(
      p_state->get().compaction_state->cleaned_ranges.to_vec(),
      ElementsAre(MatchesRange(10_o, 15_o)));
    EXPECT_THAT(
      p_state->get().compaction_state->cleaned_ranges_with_tombstones,
      ElementsAre(MatchesRange(10_o, 15_o)));
    EXPECT_EQ(
      p_state->get().compaction_epoch, partition_state::compaction_epoch_t{1});
}

TEST_P(StateUpdateParamTest, TestRemoveObjectsBasic) {
    auto add = add_objects_builder()
                 .add(new_obj_builder(oid1, 100, 1100)
                        .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                        .build())
                 .add(new_obj_builder(oid2, 100, 1100)
                        .add(tidp_b, 0_o, 10_o, 1999_t, 0, 99)
                        .build())
                 .add_term_start(tidp_a, 0_tm, 0_o)
                 .add_term_start(tidp_b, 0_tm, 0_o)
                 .build();
    ASSERT_TRUE(apply_add_objects(std::move(add)).has_value());
    EXPECT_EQ(2, get_state().objects.size());

    // Replace objects to mark originals as unreferenced.
    auto replace = replace_objects_builder()
                     .add(new_obj_builder(oid3, 100, 1100)
                            .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                            .build())
                     .add(new_obj_builder(oid4, 100, 1100)
                            .add(tidp_b, 0_o, 10_o, 1999_t, 0, 99)
                            .build())
                     .build();
    ASSERT_TRUE(apply_replace_objects(std::move(replace)).has_value());
    EXPECT_EQ(4, get_state().objects.size());

    // Remove the unreferenced objects.
    ASSERT_TRUE(apply_remove_objects({oid1, oid2}).has_value());

    EXPECT_FALSE(get_state().objects.contains(oid1));
    EXPECT_FALSE(get_state().objects.contains(oid2));
    EXPECT_TRUE(get_state().objects.contains(oid3));
    EXPECT_TRUE(get_state().objects.contains(oid4));
    EXPECT_EQ(2, get_state().objects.size());

    // Move the start offset such that one of the partition's objects are fully
    // unreferenced.
    auto tp = model::topic_id_partition::from(tidp_a);
    ASSERT_TRUE(apply_set_start_offset(tp, 11_o).has_value());

    // Remove the unreferenced objects.
    ASSERT_TRUE(apply_remove_objects({oid3}).has_value());

    EXPECT_FALSE(get_state().objects.contains(oid3));
    EXPECT_TRUE(get_state().objects.contains(oid4));
    EXPECT_EQ(1, get_state().objects.size());
}

TEST_P(StateUpdateParamTest, TestRemoveObjectsWithReferences) {
    auto add = add_objects_builder()
                 .add(new_obj_builder(oid1, 200, 1100)
                        .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                        .add(tidp_b, 0_o, 10_o, 1999_t, 100, 199)
                        .build())
                 .add_term_start(tidp_a, 0_tm, 0_o)
                 .add_term_start(tidp_b, 0_tm, 0_o)
                 .build();
    ASSERT_TRUE(apply_add_objects(std::move(add)).has_value());
    EXPECT_EQ(1, get_state().objects.size());

    // Move the start offset such that one of the partitions is gone, but the
    // object is still referenced by the other.
    auto tp = model::topic_id_partition::from(tidp_a);
    ASSERT_TRUE(apply_set_start_offset(tp, 11_o).has_value());

    auto remove_res = apply_remove_objects({oid1});
    if (GetParam() == state_backend::lsm) {
        // The LSM state update is more permissive (burden is on callers to do
        // this safely).
        ASSERT_TRUE(remove_res.has_value());
        EXPECT_EQ(0, get_state().objects.size());
        EXPECT_FALSE(get_state().objects.contains(oid1));
    } else {
        ASSERT_FALSE(remove_res.has_value());
        EXPECT_THAT(
          remove_res.error()(), testing::ContainsRegex("is still referenced"));

        EXPECT_EQ(1, get_state().objects.size());
        EXPECT_TRUE(get_state().objects.contains(oid1));
    }
}

TEST_P(StateUpdateParamTest, TestRemoveMissingObjects) {
    auto add = add_objects_builder()
                 .add(new_obj_builder(oid1, 100, 1100)
                        .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                        .build())
                 .add_term_start(tidp_a, 0_tm, 0_o)
                 .build();
    ASSERT_TRUE(apply_add_objects(std::move(add)).has_value());
    EXPECT_EQ(1, get_state().objects.size());

    // Remove references to oid1.
    auto tp = model::topic_id_partition::from(tidp_a);
    ASSERT_TRUE(apply_set_start_offset(tp, 11_o).has_value());

    // Remove it, and objects that don't exist.
    ASSERT_TRUE(apply_remove_objects({oid1, oid2, oid3, oid4}).has_value());

    // The operation should have succeeded.
    EXPECT_EQ(0, get_state().objects.size());
}

TEST_P(StateUpdateParamTest, TestRemoveTopicsBasic) {
    auto add = add_objects_builder()
                 .add(new_obj_builder(oid1, 200, 1100)
                        .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                        .add(tidp_b, 0_o, 10_o, 1999_t, 100, 199)
                        .build())
                 .add_term_start(tidp_a, 0_tm, 0_o)
                 .add_term_start(tidp_b, 0_tm, 0_o)
                 .build();
    ASSERT_TRUE(apply_add_objects(std::move(add)).has_value());
    {
        auto& s = get_state();
        EXPECT_EQ(2, s.topic_to_state.size());
        EXPECT_EQ(1, s.objects.size());
        auto& obj = s.objects.at(oid1);
        EXPECT_EQ(198, obj.total_data_size);
        EXPECT_EQ(0, obj.removed_data_size);
    }

    // Remove just one topic.
    auto tp_a = model::topic_id_partition::from(tidp_a);
    ASSERT_TRUE(apply_remove_topics({tp_a.topic_id}).has_value());

    // Just the other topic should remain in the state.
    auto& s = get_state();
    EXPECT_EQ(1, s.topic_to_state.size());
    EXPECT_FALSE(s.topic_to_state.contains(tp_a.topic_id));
    auto tp_b = model::topic_id_partition::from(tidp_b);
    EXPECT_TRUE(s.topic_to_state.contains(tp_b.topic_id));

    // Validate object accounting.
    EXPECT_EQ(1, s.objects.size());
    EXPECT_EQ(198, s.objects.at(oid1).total_data_size);
    EXPECT_EQ(99, s.objects.at(oid1).removed_data_size);

    if (GetParam() == state_backend::simple) {
        // We should be unable to remove object until the other topic is
        // removed. Note, the LSM backend is more permissive.
        auto remove_obj_res = apply_remove_objects({oid1});
        EXPECT_FALSE(remove_obj_res.has_value());
        EXPECT_THAT(
          remove_obj_res.error()(),
          testing::ContainsRegex("is still referenced"));
        EXPECT_EQ(1, get_state().objects.size());
    }

    // Now remove the other topic and try again.
    ASSERT_TRUE(apply_remove_topics({tp_b.topic_id}).has_value());

    ASSERT_TRUE(apply_remove_objects({oid1}).has_value());
    EXPECT_EQ(0, get_state().objects.size());
}

TEST_P(StateUpdateParamTest, TestRemoveMultipleTopics) {
    auto add = add_objects_builder()
                 .add(new_obj_builder(oid1, 200, 1100)
                        .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                        .add(tidp_b, 0_o, 10_o, 1999_t, 100, 199)
                        .build())
                 .add(new_obj_builder(oid2, 200, 1100)
                        .add(tidp_b, 11_o, 20_o, 1999_t, 0, 99)
                        .add(tidp_c, 0_o, 10_o, 1999_t, 100, 199)
                        .build())
                 .add(new_obj_builder(oid3, 100, 1100)
                        .add(tidp_a, 11_o, 20_o, 1999_t, 0, 99)
                        .build())
                 .add(new_obj_builder(oid4, 100, 1100)
                        .add(tidp_c, 11_o, 20_o, 1999_t, 0, 99)
                        .build())
                 .add_term_start(tidp_a, 0_tm, 0_o)
                 .add_term_start(tidp_b, 0_tm, 0_o)
                 .add_term_start(tidp_c, 0_tm, 0_o)
                 .build();
    ASSERT_TRUE(apply_add_objects(std::move(add)).has_value());
    {
        auto& s = get_state();
        EXPECT_EQ(3, s.topic_to_state.size());
        EXPECT_EQ(4, s.objects.size());
    }

    // Remove all the topics in a single update.
    auto tp_a = model::topic_id_partition::from(tidp_a);
    auto tp_b = model::topic_id_partition::from(tidp_b);
    auto tp_c = model::topic_id_partition::from(tidp_c);
    ASSERT_TRUE(
      apply_remove_topics({tp_a.topic_id, tp_b.topic_id, tp_c.topic_id})
        .has_value());

    // Sanity check, the objects should still be there -- only the topics are
    // removed.
    auto& s = get_state();
    EXPECT_EQ(0, s.topic_to_state.size());
    EXPECT_EQ(4, s.objects.size());

    // All the objects should be removable now.
    ASSERT_TRUE(apply_remove_objects({oid1, oid2, oid3, oid4}).has_value());
    EXPECT_EQ(0, get_state().objects.size());
}

TEST_P(StateUpdateParamTest, TestRemoveMissingTopic) {
    // Add just one topic.
    auto add = add_objects_builder()
                 .add(new_obj_builder(oid1, 100, 1100)
                        .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                        .build())
                 .add_term_start(tidp_a, 0_tm, 0_o)
                 .build();
    ASSERT_TRUE(apply_add_objects(std::move(add)).has_value());
    {
        auto& s = get_state();
        EXPECT_EQ(1, s.topic_to_state.size());
        EXPECT_EQ(1, s.objects.size());
    }

    // Remove topics that don't exist (and one that does).
    auto tp_a = model::topic_id_partition::from(tidp_a);
    auto tp_b = model::topic_id_partition::from(tidp_b);
    auto tp_c = model::topic_id_partition::from(tidp_c);
    ASSERT_TRUE(
      apply_remove_topics({tp_a.topic_id, tp_b.topic_id, tp_c.topic_id})
        .has_value());

    // Should succeed gracefully despite having other topics.
    {
        auto& s = get_state();
        EXPECT_EQ(0, s.topic_to_state.size());
        EXPECT_EQ(1, s.objects.size());

        // Object accounting should only reflect the removed topic A.
        EXPECT_EQ(99, s.objects.at(oid1).total_data_size);
        EXPECT_EQ(99, s.objects.at(oid1).removed_data_size);
    }

    // Do the same with only non-existent topics. This is a no-op.
    ASSERT_TRUE(
      apply_remove_topics({tp_b.topic_id, tp_c.topic_id}).has_value());
    auto& s = get_state();
    EXPECT_EQ(0, s.topic_to_state.size());
    EXPECT_EQ(1, s.objects.size());
}

TEST_P(StateUpdateParamTest, TestCompactionValidatesEpoch) {
    using range = struct compaction_state_update::cleaned_range;
    auto add = add_objects_builder()
                 .add(new_obj_builder(oid1, 300, 1300)
                        .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                        .build())
                 .add_term_start(tidp_a, 0_tm, 0_o)
                 .build();
    auto add_res = apply_add_objects(std::move(add));
    ASSERT_TRUE(add_res.has_value());

    {
        // Attempt to fully compact partition A with an invalid compaction
        // epoch.
        auto replace = replace_objects_builder()
                         .add(new_obj_builder(oid2, 100, 1100)
                                .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                                .build())
                         .clean(
                           tidp_a,
                           range{
                             .base_offset = 0_o,
                             .last_offset = 10_o,
                             .has_tombstones = true},
                           1999_t)
                         .set_expected_epoch(
                           tidp_a, partition_state::compaction_epoch_t{999})
                         .build();

        auto replace_res = apply_replace_objects(std::move(replace));
        ASSERT_FALSE(replace_res.has_value());
    }

    {
        // Fix the expected epoch and see state increment its internal
        // compaction_epoch.
        auto replace = replace_objects_builder()
                         .add(new_obj_builder(oid3, 100, 1100)
                                .add(tidp_a, 0_o, 10_o, 1999_t, 0, 99)
                                .build())
                         .clean(
                           tidp_a,
                           range{
                             .base_offset = 0_o,
                             .last_offset = 10_o,
                             .has_tombstones = true},
                           1999_t)
                         .set_expected_epoch(
                           tidp_a, partition_state::compaction_epoch_t{0})
                         .build();

        auto replace_res = apply_replace_objects(std::move(replace));
        ASSERT_TRUE(replace_res.has_value());
        auto& s = get_state();
        auto tp = model::topic_id_partition::from(tidp_a);
        auto p_state = s.partition_state(tp);
        ASSERT_TRUE(p_state.has_value());
        ASSERT_TRUE(p_state->get().compaction_state.has_value());
        ASSERT_EQ(
          p_state->get().compaction_epoch,
          partition_state::compaction_epoch_t{1});
    }
}

// --- preregistration tests ---

TEST_P(StateUpdateParamTest, TestPreregisterObjectsInsertsThenApply) {
    auto now = model::timestamp::now();
    chunked_vector<object_id> ids;
    ids.push_back(oid1);
    ids.push_back(oid2);

    auto res = apply_preregister_objects(std::move(ids), now);
    ASSERT_TRUE(res.has_value());

    auto& s = get_state();
    EXPECT_EQ(s.objects.size(), 2u);
    ASSERT_TRUE(s.objects.contains(oid1));
    ASSERT_TRUE(s.objects.contains(oid2));
    EXPECT_TRUE(s.objects.at(oid1).is_preregistration);
    EXPECT_EQ(s.objects.at(oid1).last_updated, now);
}

TEST_P(StateUpdateParamTest, TestExpirePreregisteredObjectsClearsFlag) {
    auto now = model::timestamp{1000};
    chunked_vector<object_id> prereg_ids;
    prereg_ids.push_back(oid1);
    prereg_ids.push_back(oid2);
    ASSERT_TRUE(
      apply_preregister_objects(std::move(prereg_ids), now).has_value());

    chunked_vector<object_id> expire_ids;
    expire_ids.push_back(oid1);
    ASSERT_TRUE(
      apply_expire_preregistered_objects(std::move(expire_ids)).has_value());

    auto& s = get_state();
    // oid1 should have is_preregistration cleared (zero-sized, GC-eligible)
    ASSERT_TRUE(s.objects.contains(oid1));
    EXPECT_FALSE(s.objects.at(oid1).is_preregistration);
    EXPECT_EQ(s.objects.at(oid1).total_data_size, 0u);

    // oid2 untouched
    ASSERT_TRUE(s.objects.contains(oid2));
    EXPECT_TRUE(s.objects.at(oid2).is_preregistration);
}

TEST(AddObjectsUpdateTest, RejectsUnregisteredObject) {
    state s;
    // oid1 is NOT in objects (not preregistered).
    // add_objects_update::build() should fail.
    chunked_vector<new_object> objs;
    new_object obj;
    obj.oid = oid1;
    obj.footer_pos = 0;
    obj.object_size = 100;
    objs.push_back(std::move(obj));

    auto result = add_objects_update::build(s, std::move(objs), {});
    EXPECT_FALSE(result.has_value());
}
