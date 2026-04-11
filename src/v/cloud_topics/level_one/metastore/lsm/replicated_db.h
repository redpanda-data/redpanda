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

#include "cloud_io/remote.h"
#include "cloud_storage_clients/types.h"
#include "cloud_topics/level_one/metastore/lsm/stm.h"
#include "cloud_topics/level_one/metastore/lsm/write_batch_row.h"
#include "container/chunked_vector.h"
#include "lsm/io/persistence.h"
#include "lsm/lsm.h"
#include "lsm/proto/manifest.proto.h"
#include "model/fundamental.h"
#include "utils/detailed_error.h"

#include <seastar/core/scheduling.hh>

#include <expected>
#include <filesystem>

namespace cloud_topics::l1 {

class stm;

// Leader-only interface for interacting with a replicated LSM database.
//
// There is no concurrency control applied by this class at the row level.
// Callers are expected to coordinate (e.g. via locking) to ensure updates to
// the same rows are performed in the desired order.
class replicated_database {
public:
    enum class errc {
        io_error,
        replication_error,
        update_rejected,
        not_leader,
        shutting_down,
    };
    using error = detailed_error<errc>;

    // Opens a replicated database for the leader of this term.
    //
    // This method:
    // 1. Syncs the STM to ensure we have the latest state, assuming this is
    //    the first time opening the database as leader in the current term
    // 2. Creates cloud persistence for data and metadata
    // 3. Opens the LSM database using the persisted manifest from the STM
    // 4. Applies any volatile_buffer writes to the database, catching up the
    //    database to the state as of the end of the previous leadership
    //
    // It is expected that this is called by the leader once in a given term,
    // before any LSM state updates are replicated in this term.
    static ss::future<
      std::expected<std::unique_ptr<replicated_database>, error>>
    open(
      model::term_id expected_term,
      stm* s,
      const std::filesystem::path& staging_directory,
      cloud_io::remote* remote,
      const cloud_storage_clients::bucket_name& bucket,
      ss::abort_source& as,
      ss::scheduling_group sg);

    replicated_database(replicated_database&&) = delete;
    ~replicated_database() = default;
    void start();
    ss::future<std::expected<void, error>> close();
    bool needs_reopen() const;

    // Builds a write batch for the given rows, replicates it to the STM, and
    // upon success, applies it to the local database.
    //
    // If a replication error is returned, it's possible that the replication
    // call timed out but the write was still replicated to the STM. To ensure
    // the update actually happened, callers should either retry, or step down
    // as leader to ensure the next leader picks up any potentially timed out
    // writes.
    ss::future<std::expected<void, error>>
    write(chunked_vector<write_batch_row> rows);

    // Resets the underlying STM to the given manifest.
    // NOTE: once this is called, this database instance must not be used.
    ss::future<std::expected<void, error>>
    reset(domain_uuid, std::optional<lsm::proto::manifest> manifest);

    domain_uuid get_domain_uuid() const;
    ss::future<std::expected<void, error>>
      flush(std::optional<ss::lowres_clock::duration> = std::nullopt);

    lsm::database& db() { return db_; }

private:
    replicated_database(
      model::term_id term,
      domain_uuid domain_uuid,
      stm* s,
      lsm::database db,
      ss::abort_source& as,
      ss::scheduling_group sg)
      : term_(term)
      , expected_domain_uuid_(domain_uuid)
      , stm_(s)
      , db_(std::move(db))
      , as_(as)
      , sg_(sg) {}

    ss::future<> apply_loop();

    // All replication happens with this term as the invariant.
    const model::term_id term_;

    // The domain UUID that this database is managing. If this diverges from
    // the domain UUID in the STM, this replicated_database is no longer
    // usable and callers should open a new one.
    const domain_uuid expected_domain_uuid_;

    std::optional<model::offset> applied_offset_;

    // Indicates that there are rows in the volatile buffer that have yet to be
    // applied to the database.
    ssx::condition_variable needs_apply_cv_;

    // Indicates that some rows were just applied to the database.
    ssx::condition_variable finished_apply_cv_;

    // STM for replication and state access.
    stm* stm_;

    // The underlying LSM database.
    lsm::database db_;

    ss::gate gate_;
    ss::abort_source& as_;
    ss::scheduling_group sg_;
};

} // namespace cloud_topics::l1

template<>
struct fmt::formatter<cloud_topics::l1::replicated_database::errc> final
  : fmt::formatter<std::string_view> {
    template<typename FormatContext>
    auto format(
      const cloud_topics::l1::replicated_database::errc& k,
      FormatContext& ctx) const {
        switch (k) {
            using enum cloud_topics::l1::replicated_database::errc;
        case io_error:
            return formatter<string_view>::format("io_error", ctx);
        case replication_error:
            return formatter<string_view>::format("replication_error", ctx);
        case update_rejected:
            return formatter<string_view>::format("update_rejected", ctx);
        case not_leader:
            return formatter<string_view>::format("not_leader", ctx);
        case shutting_down:
            return formatter<string_view>::format("shutting_down", ctx);
        }
    }
};
