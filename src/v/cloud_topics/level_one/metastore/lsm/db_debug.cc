/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/level_one/metastore/lsm/db_debug.h"

#include "cloud_topics/level_one/metastore/lsm/keys.h"
#include "cloud_topics/level_one/metastore/lsm/values.h"
#include "cloud_topics/logger.h"
#include "serde/rw/rw.h"

#include <seastar/core/coroutine.hh>

namespace cloud_topics::l1 {

ss::future<> dump_partition_state(lsm::snapshot& snap) {
    if (!cd_log.is_enabled(ss::log_level::debug)) {
        co_return;
    }
    auto iter = co_await snap.create_iterator();
    co_await iter.seek_to_first();
    while (iter.valid()) {
        auto key_str = iter.key();
        auto val_buf = iter.value();
        bool is_tombstone = val_buf.empty();

        if (auto mk = metadata_row_key::decode(key_str)) {
            if (is_tombstone) {
                vlog(cd_log.debug, "metadata {}: <tombstone>", mk->tidp);
            } else {
                auto val = serde::from_iobuf<metadata_row_value>(
                  std::move(val_buf));
                vlog(
                  cd_log.debug,
                  "metadata {}: start={}, next={}, size={}, "
                  "compaction_epoch={}",
                  mk->tidp,
                  val.start_offset,
                  val.next_offset,
                  val.size,
                  val.compaction_epoch);
            }
        } else if (auto ek = extent_row_key::decode(key_str)) {
            if (is_tombstone) {
                vlog(
                  cd_log.debug,
                  "  extent {}: base={} <tombstone>",
                  ek->tidp,
                  ek->base_offset);
            } else {
                auto val = serde::from_iobuf<extent_row_value>(
                  std::move(val_buf));
                vlog(
                  cd_log.debug,
                  "  extent {}: base={}, last={}, ts={}, filepos={}, "
                  "len={}, oid={}",
                  ek->tidp,
                  ek->base_offset,
                  val.last_offset,
                  val.max_timestamp,
                  val.filepos,
                  val.len,
                  val.oid);
            }
        } else if (auto tk = term_row_key::decode(key_str)) {
            if (is_tombstone) {
                vlog(
                  cd_log.debug,
                  "  term {}: term={} <tombstone>",
                  tk->tidp,
                  tk->term);
            } else {
                auto val = serde::from_iobuf<term_row_value>(
                  std::move(val_buf));
                vlog(
                  cd_log.debug,
                  "  term {}: term={}, start_offset={}",
                  tk->tidp,
                  tk->term,
                  val.term_start_offset);
            }
        } else if (auto ck = compaction_row_key::decode(key_str)) {
            if (is_tombstone) {
                vlog(cd_log.debug, "  compaction {}: <tombstone>", ck->tidp);
            } else {
                auto val = serde::from_iobuf<compaction_row_value>(
                  std::move(val_buf));
                vlog(
                  cd_log.debug,
                  "  compaction {}: cleaned_ranges={}, "
                  "cleaned_ranges_with_tombstones={}",
                  ck->tidp,
                  val.state.cleaned_ranges,
                  val.state.cleaned_ranges_with_tombstones.size());
            }
        } else if (auto ok = object_row_key::decode(key_str)) {
            if (is_tombstone) {
                vlog(cd_log.debug, "  object {}: <tombstone>", ok->oid);
            } else {
                auto val = serde::from_iobuf<object_row_value>(
                  std::move(val_buf));
                vlog(
                  cd_log.debug,
                  "  object {}: total_data_size={}, "
                  "removed_data_size={}, footer_pos={}, object_size={}",
                  ok->oid,
                  val.object.total_data_size,
                  val.object.removed_data_size,
                  val.object.footer_pos,
                  val.object.object_size);
            }
        } else {
            vlog(
              cd_log.debug,
              "unknown key: {} (len={}, value_len={})",
              key_str,
              key_str.size(),
              val_buf.size_bytes());
        }

        co_await iter.next();
    }
}

} // namespace cloud_topics::l1
