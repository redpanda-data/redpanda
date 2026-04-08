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

#include "bytes/iobuf.h"
#include "cloud_topics/level_one/metastore/domain_uuid.h"
#include "cloud_topics/level_one/metastore/lsm/write_batch_row.h"
#include "lsm/lsm.h"
#include "model/fundamental.h"
#include "serde/envelope.h"

#include <seastar/core/future.hh>

#include <deque>

namespace cloud_topics::l1 {

struct volatile_row
  : public serde::
      envelope<volatile_row, serde::version<0>, serde::compat_version<0>> {
    friend bool operator==(const volatile_row&, const volatile_row&) = default;
    auto serde_fields() { return std::tie(seqno, row); }
    lsm::sequence_number seqno;
    write_batch_row row;

    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(it, "{{seqno: {}, row: {}}}", seqno, row);
    }
};

// State that backs a replicated database on object storage. This comprises of:
// - A UUID that uniquely identifies the database state, to ensure we only
//   accept operations meant for the correct database. This gets updated when
//   the state is ever restored from a manifest.
// - Deltas for the offset and term to be applied to seqno and epochs to ensure
//   monotonicity for the underlying database. These may be non-zero after
//   restoring a manifest.
// - A manifest that has been uploaded to object storage from which all
//   replicas will be able to open a database.
// - A list of writes that need to be replayed on top of the manifest in order
//   to be caught up. This list can be thought of as a write-ahead log for the
//   database.
struct lsm_state
  : public serde::
      envelope<lsm_state, serde::version<0>, serde::compat_version<0>> {
    friend bool operator==(const lsm_state&, const lsm_state&) = default;
    auto serde_fields() {
        return std::tie(
          domain_uuid,
          seqno_delta,
          db_epoch_delta,
          volatile_buffer,
          persisted_manifest);
    }
    lsm_state share();
    ss::future<> serde_async_write(iobuf& out);

    // Conversion between Redpanda space and LSM DB space.
    model::term_id to_term(lsm::internal::database_epoch) const;
    lsm::internal::database_epoch to_epoch(model::term_id) const;
    model::offset to_offset(lsm::sequence_number) const;
    lsm::sequence_number to_seqno(model::offset) const;

    // The unique identifier for this LSM state. This should be used as the
    // basis for where the database writes its data and metadata.
    //
    // Must be set before any writes are accepted. May be reset if recovering
    // from an existing manifest that is written with a different domain path
    // (e.g.  when recovering state from cloud).
    domain_uuid domain_uuid{};

    // Difference between Raft offset/term and database seqno/epoch. These will
    // be non-zero upon recovery from object storage, since the restored
    // seqno/epoch will not start at zero in that case. These may also be
    // negative, in case recovery was performed on a non-empty Raft log.
    //
    // seqno = offset + seqno_delta
    // epoch = term + epoch_delta
    //
    // TODO: use named types to enforce correct arithmetic rules.
    int64_t seqno_delta{0};
    int64_t db_epoch_delta{0};

    // Rows that aren't persisted to object storage yet but should be applied
    // to the database. When opening a database from the persisted manifest,
    // these rows must be applied to the opened database to catch it up.
    std::deque<volatile_row> volatile_buffer;

    // State that is persisted to object storage. This state contains write
    // operations up to a given sequence number; operations from below that
    // sequence number can be removed from the volatile buffer and no longer
    // need to be replayed to the database when opening the database from this
    // state.
    //
    // TODO: we store the serialized manifest because it allows us to copy
    // synchronously without adding locks. If we want to maintain the manifest
    // as raw proto, we will need to add a synchronous copy to protobuf, or add
    // a lock around the manifest to safely async copy it.
    struct serialized_manifest
      : public serde::envelope<
          serialized_manifest,
          serde::version<0>,
          serde::compat_version<0>> {
        auto serde_fields() {
            return std::tie(buf, last_seqno, database_epoch);
        }
        friend bool operator==(
          const serialized_manifest&, const serialized_manifest&) = default;
        serialized_manifest share();
        lsm::sequence_number get_last_seqno() const { return last_seqno; }
        lsm::internal::database_epoch get_database_epoch() const {
            return database_epoch;
        }

        iobuf buf;
        lsm::sequence_number last_seqno;
        lsm::internal::database_epoch database_epoch;
    };
    std::optional<serialized_manifest> persisted_manifest;
};

struct lsm_stm_snapshot
  : public serde::
      envelope<lsm_stm_snapshot, serde::version<0>, serde::compat_version<0>> {
    auto serde_fields() { return std::tie(state); }
    lsm_state state;
    ss::future<> serde_async_write(iobuf& out);
};

} // namespace cloud_topics::l1
