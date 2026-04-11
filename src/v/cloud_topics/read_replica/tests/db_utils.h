/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "bytes/iobuf.h"
#include "cloud_io/remote.h"
#include "cloud_storage_clients/types.h"
#include "cloud_topics/level_one/metastore/domain_uuid.h"
#include "cloud_topics/level_one/metastore/lsm/state_reader.h"
#include "cloud_topics/level_one/metastore/lsm/state_update.h"
#include "cloud_topics/level_one/metastore/lsm/write_batch_row.h"
#include "container/chunked_hash_map.h"
#include "lsm/io/cloud_persistence.h"
#include "lsm/lsm.h"
#include "model/fundamental.h"
#include "ssx/time.h"

#include <seastar/core/future.hh>

#include <filesystem>
#include <optional>

using namespace std::chrono_literals;

namespace cloud_topics::read_replica::test_utils {

// Opens or retrieves a cached writer database for the given domain.
inline ss::future<lsm::database*> get_or_create_writer_db(
  l1::domain_uuid domain,
  const std::filesystem::path& staging_base,
  cloud_io::remote* remote,
  const cloud_storage_clients::bucket_name& bucket,
  chunked_hash_map<l1::domain_uuid, std::unique_ptr<lsm::database>>& dbs) {
    lsm::internal::database_epoch epoch = lsm::internal::database_epoch{1};
    auto it = dbs.find(domain);
    if (it != dbs.end()) {
        co_return it->second.get();
    }

    auto cloud_prefix = l1::domain_cloud_prefix(domain);
    cloud_storage_clients::object_key domain_prefix{cloud_prefix};

    auto data_persist = co_await lsm::io::open_cloud_data_persistence(
      staging_base, remote, bucket, domain_prefix, ss::sstring(domain()));
    auto meta_persist = co_await lsm::io::open_cloud_metadata_persistence(
      remote, bucket, domain_prefix);

    lsm::io::persistence io{
      .data = std::move(data_persist),
      .metadata = std::move(meta_persist),
    };

    auto db = co_await lsm::database::open(
      lsm::options{
        .database_epoch = epoch,
        .readonly = false,
      },
      std::move(io));

    auto [db_it, inserted] = dbs.emplace(
      domain, std::make_unique<lsm::database>(std::move(db)));
    co_return db_it->second.get();
}

// Writes L1 metastore data with semantic offset tracking. Creates one
// new_object with one extent and term state. Offsets are expected to be
// contiguous with previous writes.
inline ss::future<lsm::sequence_number> write_metastore_data(
  lsm::database* db,
  const model::topic_id_partition& tidp,
  kafka::offset base_offset,
  kafka::offset last_offset,
  model::term_id term,
  std::optional<ss::lowres_clock::duration> flush_timeout = std::nullopt) {
    l1::new_object new_obj;
    new_obj.oid = l1::object_id{uuid_t::create()};
    new_obj.footer_pos = 1000;
    new_obj.object_size = 2000;

    // Build the objects.
    l1::new_object::metadata extent_meta{
      .base_offset = base_offset,
      .last_offset = last_offset,
      .max_timestamp = model::timestamp::now(),
      .filepos = 0,
      .len = 1000,
    };
    new_obj.extent_metas[tidp.topic_id].emplace(tidp.partition, extent_meta);

    // Build the terms.
    l1::term_state_update_t term_updates;
    term_updates[tidp].push_back(
      l1::term_start{
        .term_id = term,
        .start_offset = base_offset,
      });

    // Pre-register the object so add_objects can reference it.
    l1::preregister_objects_db_update prereg;
    prereg.object_ids.push_back(new_obj.oid);
    prereg.registered_at = model::timestamp::now();
    {
        auto snap = db->create_snapshot();
        l1::state_reader reader(std::move(snap));
        chunked_vector<l1::write_batch_row> rows;
        auto result = co_await prereg.build_rows(reader, rows);
        if (!result.has_value()) {
            throw std::runtime_error(
              fmt::format(
                "Failed to build preregister rows: {}", result.error()));
        }
        auto current_seqno = db->max_applied_seqno().value_or(
          lsm::sequence_number{0});
        auto wb = db->create_write_batch();
        for (auto& row : rows) {
            current_seqno = lsm::sequence_number{current_seqno() + 1};
            wb.put(row.key, std::move(row.value), current_seqno);
        }
        co_await db->apply(std::move(wb));
    }

    l1::add_objects_db_update update;
    update.new_objects.push_back(std::move(new_obj));
    update.new_terms = std::move(term_updates);

    // Build the rows for our update.
    auto snap = db->create_snapshot();
    l1::state_reader reader(std::move(snap));
    chunked_vector<l1::write_batch_row> rows;
    auto build_result = co_await update.build_rows(reader, rows);
    if (!build_result.has_value()) {
        throw std::runtime_error(
          fmt::format("Failed to build rows: {}", build_result.error()));
    }
    // Write the rows to the DB and flush to ensure the update lands in
    // object storage.
    auto current_seqno = db->max_applied_seqno().value_or(
      lsm::sequence_number{0});
    auto wb = db->create_write_batch();
    for (auto& row : rows) {
        current_seqno = lsm::sequence_number{current_seqno() + 1};
        wb.put(row.key, std::move(row.value), current_seqno);
    }
    co_await db->apply(std::move(wb));
    auto timeout = flush_timeout.value_or(30s);
    auto deadline = ssx::instant::from_chrono(
      ss::lowres_clock::now() + timeout);
    co_await db->flush(deadline);
    co_return current_seqno;
}

} // namespace cloud_topics::read_replica::test_utils
