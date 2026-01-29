/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_topics/level_one/common/object_id.h"
#include "cloud_topics/level_one/metastore/metastore.h"

namespace cloud_topics::l1::test_utils {

using term_map_t = metastore::term_offset_map_t;
class terms_builder {
public:
    terms_builder&
    add(std::string_view tp_str, model::term_id t, kafka::offset o) {
        auto tp = model::topic_id_partition::from(tp_str);
        out[tp].emplace_back(
          metastore::term_offset{.term = t, .first_offset = o});
        return *this;
    }
    term_map_t build() { return std::move(out); }

private:
    term_map_t out;
};

using om_list_t = chunked_vector<metastore::object_metadata>;
using cmap_t = metastore::compaction_map_t;
class om_builder {
public:
    om_builder(object_id oid, size_t footer_pos, size_t object_size) {
        out.oid = oid;
        out.footer_pos = footer_pos;
        out.object_size = object_size;
    }
    om_builder& add(
      std::string_view tpr_str,
      kafka::offset base_o,
      kafka::offset last_o,
      model::timestamp last_t,
      size_t first_pos,
      size_t last_pos) {
        out.ntp_metas.emplace_back(
          metastore::object_metadata::ntp_metadata{
            .tidp = model::topic_id_partition::from(tpr_str),
            .base_offset = base_o,
            .last_offset = last_o,
            .max_timestamp = last_t,
            .pos = first_pos,
            .size = last_pos - first_pos,
          });
        return *this;
    }
    metastore::object_metadata build() { return std::move(out); }

private:
    metastore::object_metadata out;
};
class cm_builder {
public:
    cm_builder& clean(
      std::string_view tpr_str,
      kafka::offset base,
      kafka::offset last,
      std::optional<model::timestamp> with_tombstones_ts = std::nullopt) {
        auto tp = model::topic_id_partition::from(tpr_str);
        auto& cmp_meta = out[tp];
        cmp_meta.new_cleaned_ranges.push_back(
          metastore::compaction_update::cleaned_range{
            .base_offset = base,
            .last_offset = last,
            .has_tombstones = with_tombstones_ts.has_value(),
          });
        if (with_tombstones_ts) {
            cmp_meta.cleaned_at = *with_tombstones_ts;
        }
        return *this;
    }
    cm_builder& remove_tombstones(
      std::string_view tpr_str, kafka::offset base, kafka::offset last) {
        auto tp = model::topic_id_partition::from(tpr_str);
        auto& cmp_meta = out[tp];
        cmp_meta.removed_tombstones_ranges.insert(base, last);
        cmp_meta.cleaned_at = model::timestamp::now();
        return *this;
    }
    cm_builder& set_expected_epoch(
      std::string_view tpr_str, metastore::compaction_epoch epoch) {
        auto tp = model::topic_id_partition::from(tpr_str);
        auto& cmp_meta = out[tp];
        cmp_meta.expected_compaction_epoch = epoch;
        return *this;
    }
    cmap_t build() { return std::move(out); }

private:
    cmap_t out;
};

} // namespace cloud_topics::l1::test_utils
