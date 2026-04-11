/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/metastore/lsm/replicated_db.h"

#include "cloud_topics/level_one/metastore/domain_uuid.h"
#include "cloud_topics/level_one/metastore/lsm/lsm_update.h"
#include "cloud_topics/level_one/metastore/lsm/replicated_persistence.h"
#include "cloud_topics/level_one/metastore/lsm/stm.h"
#include "cloud_topics/logger.h"
#include "config/configuration.h"
#include "lsm/io/cloud_persistence.h"
#include "lsm/io/persistence.h"
#include "lsm/proto/manifest.proto.h"
#include "model/batch_builder.h"
#include "model/record.h"
#include "serde/rw/scalar.h"
#include "ssx/clock.h"
#include "ssx/future-util.h"

#include <seastar/core/coroutine.hh>

namespace cloud_topics::l1 {

namespace {
replicated_database::errc map_stm_error(stm::errc e) {
    switch (e) {
    case stm::errc::not_leader:
        return replicated_database::errc::not_leader;
    case stm::errc::raft_error:
        return replicated_database::errc::replication_error;
    case stm::errc::shutting_down:
        return replicated_database::errc::shutting_down;
    }
}

replicated_database::error
wrap_failed_future(std::exception_ptr ex, std::string_view context) {
    if (ssx::is_shutdown_exception(ex)) {
        return replicated_database::error(
          replicated_database::errc::shutting_down);
    }
    return replicated_database::error(
      replicated_database::errc::io_error, "{}: {}", context, ex);
}
} // namespace

ss::future<std::expected<
  std::unique_ptr<replicated_database>,
  replicated_database::error>>
replicated_database::open(
  model::term_id expected_term,
  stm* s,
  const std::filesystem::path& staging_directory,
  cloud_io::remote* remote,
  const cloud_storage_clients::bucket_name& bucket,
  ss::abort_source& as,
  ss::scheduling_group sg) {
    auto term_result = co_await s->sync(std::chrono::seconds(30));
    if (!term_result.has_value()) {
        co_return std::unexpected(
          error(map_stm_error(term_result.error()), "Failed to sync"));
    }
    auto term = term_result.value();
    if (term != expected_term) {
        co_return std::unexpected(error(
          errc::not_leader,
          "Synced term != expected term: {} vs {}",
          term,
          expected_term));
    }
    auto epoch = s->state().to_epoch(term);

    vlog(
      cd_log.info,
      "Opening replicated LSM database for term {} with DB epoch {}",
      term,
      epoch);
    if (s->state().domain_uuid().is_nil()) {
        auto new_domain_uuid = domain_uuid(uuid_t::create());
        vlog(
          cd_log.info,
          "Replicating new domain UUID {} in term {}",
          new_domain_uuid,
          term);
        auto update = set_domain_uuid_update::build(
          s->state(), new_domain_uuid);
        model::batch_builder builder;
        builder.set_batch_type(model::record_batch_type::l1_stm);
        builder.add_record(
          {.key = serde::to_iobuf(lsm_update_key::set_domain_uuid),
           .value = serde::to_iobuf(update.value())});
        auto batch = co_await std::move(builder).build();
        auto replicate_result = co_await s->replicate_and_wait(
          term, std::move(batch), as);

        if (!replicate_result.has_value()) {
            co_return std::unexpected(error(
              map_stm_error(replicate_result.error()),
              "Failed to replicate set_domain_uuid batch"));
        }
        if (s->state().domain_uuid().is_nil()) {
            co_return std::unexpected(errc::replication_error);
        }
    }
    auto domain_uuid = s->state().domain_uuid;
    cloud_storage_clients::object_key domain_prefix{
      domain_cloud_prefix(domain_uuid)};

    auto data_persist_fut = co_await ss::coroutine::as_future(
      lsm::io::open_cloud_data_persistence(
        staging_directory,
        remote,
        bucket,
        domain_prefix,
        ss::sstring(domain_uuid())));
    if (data_persist_fut.failed()) {
        co_return std::unexpected(wrap_failed_future(
          data_persist_fut.get_exception(), "Failed to open data persistence"));
    }
    auto meta_persist_fut = co_await ss::coroutine::as_future(
      open_replicated_metadata_persistence(
        s, remote, bucket, domain_uuid, domain_prefix));
    if (meta_persist_fut.failed()) {
        co_return std::unexpected(wrap_failed_future(
          meta_persist_fut.get_exception(),
          "Failed to open metadata persistence"));
    }
    lsm::io::persistence io{
      .data = std::move(data_persist_fut.get()),
      .metadata = std::move(meta_persist_fut.get()),
    };

    // Open the LSM database using the persisted manifest from the STM.
    auto db_fut = co_await ss::coroutine::as_future(
      lsm::database::open(
        lsm::options{
          .database_epoch = epoch(),
          .compaction_scheduling_group = sg,
          .file_deletion_delay = absl::FromChrono(
            config::shard_local_cfg()
              .cloud_topics_long_term_file_deletion_delay()),
        },
        std::move(io)));
    if (db_fut.failed()) {
        co_return std::unexpected(wrap_failed_future(
          db_fut.get_exception(), "Failed to open LSM database"));
    }
    auto db = db_fut.get();

    // Replay the writes in the volatile_buffer as writes to the database.
    // These are writes that were replicated but not yet persisted to the
    // manifest.
    auto max_persisted_seqno = db.max_persisted_seqno();
    if (!s->state().volatile_buffer.empty()) {
        vlog(
          cd_log.info,
          "Applying {} volatile writes to LSM database",
          s->state().volatile_buffer.size());

        auto wb = db.create_write_batch();
        size_t num_written = 0;
        for (const auto& row : s->state().volatile_buffer) {
            auto seqno = row.seqno;
            if (seqno <= max_persisted_seqno) {
                continue;
            }
            if (row.row.value.empty()) {
                wb.remove(row.row.key, seqno);
            } else {
                wb.put(row.row.key, row.row.value.copy(), seqno);
            }
            vlog(
              cd_log.trace,
              "Replaying at seqno: {}, key: {}",
              seqno,
              row.row.key);
            ++num_written;
        }
        if (num_written > 0) {
            auto write_fut = co_await ss::coroutine::as_future(
              db.apply(std::move(wb)));
            if (write_fut.failed()) {
                co_return std::unexpected(wrap_failed_future(
                  write_fut.get_exception(),
                  "Failed to apply volatile writes"));
            }
        }
    }
    auto ret = std::unique_ptr<replicated_database>(
      new replicated_database(term, domain_uuid, s, std::move(db), as, sg));
    ret->start();
    co_return std::move(ret);
}

void replicated_database::start() {
    ssx::spawn_with_gate(gate_, [this] {
        return ss::with_scheduling_group(sg_, [this] { return apply_loop(); });
    });
}

ss::future<std::expected<void, replicated_database::error>>
replicated_database::close() {
    auto gate_fut = gate_.close();
    needs_apply_cv_.broken();
    finished_apply_cv_.broken();
    auto fut = co_await ss::coroutine::as_future(db_.close());
    if (fut.failed()) {
        co_return std::unexpected(
          wrap_failed_future(fut.get_exception(), "Error closing database"));
    }
    co_await std::move(gate_fut);
    co_return std::expected<void, error>{};
}

bool replicated_database::needs_reopen() const {
    return !stm_->raft()->is_leader() || term_ != stm_->raft()->confirmed_term()
           || get_domain_uuid() != expected_domain_uuid_;
}

ss::future<std::expected<void, replicated_database::error>>
replicated_database::write(chunked_vector<write_batch_row> rows) {
    if (rows.empty()) {
        co_return std::expected<void, error>{};
    }

    auto update = apply_write_batch_update::build(
      stm_->state(), expected_domain_uuid_, std::move(rows));

    if (!update.has_value()) {
        co_return std::unexpected(error(
          errc::update_rejected,
          "Failed to build write batch update: {}",
          update.error()));
    }

    model::batch_builder builder;
    builder.set_batch_type(model::record_batch_type::l1_stm);
    builder.add_record(
      {.key = serde::to_iobuf(lsm_update_key::apply_write_batch),
       .value = serde::to_iobuf(update.value().share())});
    auto batch = co_await std::move(builder).build();

    auto replicate_result = co_await stm_->replicate_and_wait(
      term_, std::move(batch), as_);

    if (!replicate_result.has_value()) {
        co_return std::unexpected(error(
          map_stm_error(replicate_result.error()),
          "Failed to replicate write batch"));
    }
    // NOTE: at this point, since we waited for STM apply after replication,
    // the write should have been added to the volatile buffer.
    needs_apply_cv_.signal();
    auto lsm_apply_timeout
      = config::shard_local_cfg().cloud_topics_metastore_lsm_apply_timeout_ms();
    auto deadline = ss::lowres_clock::now() + lsm_apply_timeout;
    auto wait_fut = co_await ss::coroutine::as_future(finished_apply_cv_.wait(
      deadline, as_, [this, o = replicate_result.value()] {
          return applied_offset_.has_value() && applied_offset_.value() >= o;
      }));
    if (wait_fut.failed()) {
        auto ex = wait_fut.get_exception();
        if (ssx::is_shutdown_exception(ex)) {
            co_return std::unexpected(error(errc::shutting_down));
        }
        co_return std::unexpected(
          error(errc::replication_error, "Wait for apply timed out {}", ex));
    }
    co_return std::expected<void, error>{};
}

ss::future<std::expected<void, replicated_database::error>>
replicated_database::reset(
  domain_uuid uuid, std::optional<lsm::proto::manifest> manifest) {
    if (uuid == get_domain_uuid()) {
        co_return std::expected<void, error>{};
    }
    std::optional<lsm_state::serialized_manifest> serialized_man;
    if (manifest) {
        serialized_man = lsm_state::serialized_manifest{
          .buf = co_await manifest->to_proto(),
          .last_seqno = lsm::sequence_number(manifest->get_last_seqno()),
          .database_epoch = lsm::internal::database_epoch(
            manifest->get_database_epoch()),
        };
    }
    auto update = reset_manifest_update::build(
      stm_->state(), uuid, std::move(serialized_man));

    if (!update.has_value()) {
        co_return std::unexpected(error(
          errc::update_rejected, "Failed to build reset_manifest update"));
    }

    model::batch_builder builder;
    builder.set_batch_type(model::record_batch_type::l1_stm);
    builder.add_record(
      {.key = serde::to_iobuf(lsm_update_key::reset_manifest),
       .value = serde::to_iobuf(std::move(update.value()))});
    auto batch = co_await std::move(builder).build();

    auto replicate_result = co_await stm_->replicate_and_wait(
      term_, std::move(batch), as_);

    if (!replicate_result.has_value()) {
        co_return std::unexpected(error(
          map_stm_error(replicate_result.error()),
          "Failed to replicate manifest reset"));
    }
    if (uuid != get_domain_uuid()) {
        co_return std::unexpected(error(
          errc::replication_error,
          "Domain UUID doesn't match after replication: {} vs expected {}",
          get_domain_uuid(),
          uuid));
    }

    co_return std::expected<void, error>{};
}

domain_uuid replicated_database::get_domain_uuid() const {
    return stm_->state().domain_uuid;
}

ss::future<std::expected<void, replicated_database::error>>
replicated_database::flush(std::optional<ss::lowres_clock::duration> timeout) {
    auto deadline = timeout ? ssx::lowres_steady_clock().now()
                                + ssx::duration::from_chrono(*timeout)
                            : ssx::instant::infinite_future();
    auto flush_fut = co_await ss::coroutine::as_future(db_.flush(deadline));
    if (flush_fut.failed()) {
        co_return std::unexpected(wrap_failed_future(
          flush_fut.get_exception(), "Failed to flush to database"));
    }
    co_return std::expected<void, error>{};
}

ss::future<> replicated_database::apply_loop() {
    auto log_exit = ss::defer([this] {
        vlog(
          cd_log.debug,
          "Exiting apply loop of domain UUID {} in term {}",
          expected_domain_uuid_,
          term_);
    });
    while (!as_.abort_requested() && !gate_.is_closed()) {
        auto max_applied = db_.max_applied_seqno();
        auto& state = stm_->state();
        auto wait_fut = co_await ss::coroutine::as_future(
          needs_apply_cv_.wait(as_, [&state, max_applied] {
              return !state.volatile_buffer.empty() &&
                  (!max_applied.has_value() || state.volatile_buffer.back().seqno > max_applied.value());
          }));
        if (wait_fut.failed()) {
            auto ex = wait_fut.get_exception();
            vlog(
              cd_log.debug,
              "Shutting down apply loop for term {}: {}",
              term_,
              ex);
            co_return;
        }
        auto first_to_apply = lsm::sequence_number(0);
        if (max_applied.has_value()) {
            first_to_apply = lsm::sequence_number(max_applied.value()() + 1);
        }
        auto first_gt_applied_it = std::lower_bound(
          state.volatile_buffer.begin(),
          state.volatile_buffer.end(),
          first_to_apply,
          [](const volatile_row& r, lsm::sequence_number s) {
              return r.seqno < s;
          });
        if (first_gt_applied_it == state.volatile_buffer.end()) {
            // Conservative sanity check: the above "needs apply" condition
            // should ensure that there are rows in the volatile buffer above
            // max_applied, but there isn't harm in just continuing and
            // reevaluating.
            continue;
        }

        // TODO: apply more than one at a time? Just be careful of reactor
        // stalls, memory consumed by the write batch, and ensuring that all
        // rows in a given seqno are applied together.
        lsm::write_batch wb = db_.create_write_batch();
        const auto seqno_to_apply = first_gt_applied_it->seqno;
        auto offset = state.to_offset(seqno_to_apply);
        for (auto it = first_gt_applied_it; it != state.volatile_buffer.end();
             ++it) {
            auto seqno = it->seqno;
            if (seqno_to_apply != seqno) {
                // We've finished collecting the rows of seqno_to_apply, go and
                // apply it.
                break;
            }
            const auto& row = it->row;
            vlog(
              cd_log.trace, "Applying at seqno: {}, key: {}", seqno, row.key);
            if (row.value.empty()) {
                wb.remove(row.key, seqno);
            } else {
                wb.put(row.key, row.value.copy(), seqno);
            }
        }
        auto write_fut = co_await ss::coroutine::as_future(
          db_.apply(std::move(wb)));
        if (write_fut.failed()) {
            auto ex = write_fut.get_exception();
            vlog(
              cd_log.error,
              "Failed to apply batch {} to database: {}",
              offset,
              ex);
            continue;
        }
        vlog(cd_log.trace, "Applied write batch at seqno: {}", seqno_to_apply);
        applied_offset_ = offset;
        finished_apply_cv_.broadcast();
    }
}

} // namespace cloud_topics::l1
