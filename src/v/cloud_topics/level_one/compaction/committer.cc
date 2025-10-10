/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/compaction/committer.h"

#include "cloud_topics/level_one/common/object_id.h"
#include "cloud_topics/level_one/compaction/committing_policy.h"
#include "cloud_topics/level_one/compaction/logger.h"
#include "cloud_topics/level_one/compaction/meta.h"
#include "ssx/future-util.h"

namespace cloud_topics::l1 {

compaction_committer::compaction_committer(
  std::unique_ptr<committing_policy> policy, io* io, metastore* metastore)
  : _policy(std::move(policy))
  , _io(io)
  , _metastore(metastore) {}

ss::future<> compaction_committer::start() {
    start_committing_loop();
    co_return;
}

ss::future<> compaction_committer::stop() {
    _as.request_abort();
    _sem.broken();
    auto close_fut = _gate.close();
    auto updates = std::exchange(_updates, {});
    for (auto& update : updates) {
        co_await remove_staging_files(std::move(update));
    }
    co_await std::move(close_fut);
}

void compaction_committer::start_committing_loop() {
    ssx::repeat_until_gate_closed_or_aborted(_gate, _as, [this] {
        return committing_loop().handle_exception(
          [](const std::exception_ptr& e) {
              auto log_level = ssx::is_shutdown_exception(e)
                                 ? ss::log_level::debug
                                 : ss::log_level::warn;
              vlogl(
                compaction_log,
                log_level,
                "Encountered exception in committing loop: {}",
                e);
          });
    });
}

bool compaction_committer::is_active() const {
    return !_gate.is_closed() && !_as.abort_requested() && !_stopped;
}

ss::future<> compaction_committer::committing_loop() {
    while (is_active()) {
        constexpr std::chrono::seconds poll_frequency(10);
        try {
            co_await _sem.wait(
              poll_frequency, std::max(_sem.current(), size_t(1)));
        } catch (const ss::semaphore_timed_out&) {
            // Fall through
        }

        if (_updates.empty()) {
            continue;
        }

        if (_policy->should_commit()) {
            auto updates = std::exchange(_updates, {});
            co_await commit_some(std::move(updates));
        }
    }
}

void compaction_committer::push_update(object_output_t update) {
    _updates.push_back(std::move(update));
    auto update_response = _policy->on_update(_updates.back());
    if (update_response == committing_policy::update_response::preempt) {
        _sem.signal();
    }
}

ss::future<std::expected<
  compaction_committer::built_update_context,
  compaction_committer::error>>
compaction_committer::build_and_put_update(inflight_update_context update) {
    auto metadata_builder_res = co_await _metastore->object_builder();
    if (!metadata_builder_res.has_value()) {
        vlog(
          compaction_log.error,
          "Could not create object metadata builder: {}",
          metadata_builder_res.error());
        co_return std::unexpected(
          error{
            .t = error::type::build_or_put_failure,
            .msg = fmt::format("{}", metadata_builder_res.error())});
    }

    auto metadata_builder = std::move(metadata_builder_res).value();

    metastore::compaction_map_t compact_map;
    compact_map.emplace(update.tidp, std::move(update.compact_update));
    for (auto& file_and_md_info : update.staging_file_refs_and_md_infos) {
        auto oid_res = metadata_builder->get_or_create_object_for(
          file_and_md_info.ntp_md.tidp);
        if (!oid_res.has_value()) {
            vlog(
              compaction_log.error,
              "Could not get object: {}",
              oid_res.error());
            co_return std::unexpected(
              error{
                .t = error::type::build_or_put_failure,
                .msg = fmt::format("{}", oid_res.error())});
        }
        auto oid = std::move(oid_res).value();

        auto add_res = metadata_builder->add(
          oid, std::move(file_and_md_info.ntp_md));

        if (!add_res.has_value()) {
            vlog(
              compaction_log.error,
              "Could not add metadata to object: {}",
              add_res.error());
            co_return std::unexpected(
              error{
                .t = error::type::build_or_put_failure,
                .msg = fmt::format("{}", add_res.error())});
        }

        auto res = metadata_builder->finish(
          oid,
          file_and_md_info.info.footer_offset,
          file_and_md_info.info.size_bytes);
        if (!res.has_value()) {
            vlog(
              compaction_log.error,
              "Failed to finish metadata for object {}: {}",
              oid,
              res.error());
            co_return std::unexpected(
              error{
                .t = error::type::build_or_put_failure,
                .msg = fmt::format("{}", res.error())});
        }

        auto put_res = co_await _io->put_object(
          oid, file_and_md_info.staging_file_ref, &_as);
        if (!put_res.has_value()) {
            vlog(
              compaction_log.error,
              "Failed to put object {}: {}",
              oid,
              static_cast<int>(put_res.error()));
            co_return std::unexpected(
              error{
                .t = error::type::build_or_put_failure,
                .msg = fmt::format("{}", static_cast<int>(put_res.error()))});
        }
    }

    co_return built_update_context{
      .metadata_builder = std::move(metadata_builder),
      .compact_map = std::move(compact_map)};
}

ss::future<std::expected<void, compaction_committer::error>>
compaction_committer::try_build_and_commit_update(
  inflight_update_context update) {
    auto update_ctx_res = co_await build_and_put_update(std::move(update));
    if (!update_ctx_res.has_value()) {
        vlog(
          compaction_log.error,
          "Failed to build and put compaction update: {}",
          update_ctx_res.error());
        co_return std::unexpected(update_ctx_res.error());
    }

    auto update_ctx = std::move(update_ctx_res).value();

    auto commit_res = co_await _metastore->compact_objects(
      *update_ctx.metadata_builder, std::move(update_ctx.compact_map));

    if (!commit_res.has_value()) {
        vlog(
          compaction_log.error,
          "Failed to commit compaction update to the metastore: {}",
          commit_res.error());
        co_return std::unexpected(
          error{
            .t = error::type::commit_failure,
            .msg = fmt::format("{}", commit_res.error())});
    }

    co_return std::expected<void, compaction_committer::error>{};
}

ss::future<> compaction_committer::commit_some(updates_t updates) {
    // TODO: Here, we may also want to make decisions about how to group
    // together partitions/updates in L1. We could, for example, do a best
    // effort isolation of partition data in L1 objects. Building a
    // metadata_builder per update is silly, but we also may have to implement
    // new primitives for concatenating together L1 staging files.
    //
    // Ultimately this is a similar function to `reconciler::build_object()` and
    // may be worth abstracting out somehow, though perhaps with a different
    // heuristic for batching L1 updates here.
    for (auto& update : updates) {
        if (!is_active()) {
            // A shutdown has been triggered, we still need to clean up the
            // extracted list of staging files.
            co_await remove_staging_files(std::move(update));
            continue;
        }

        auto update_str = fmt::format("compaction update {}", update);
        vlog(compaction_log.debug, "Attempting to commit {}", update_str);

        auto update_ctx = inflight_update_context{
          .tidp = update.tidp,
          .staging_file_refs_and_md_infos = to_ref(
            update.staging_files_and_md_infos),
          .compact_update = std::move(update.compact_update)};
        auto res = co_await try_build_and_commit_update(std::move(update_ctx));

        if (!res.has_value()) {
            vlog(
              compaction_log.error,
              "Failed to commit {}: {}",
              update_str,
              res.error());
        } else {
            vlog(compaction_log.debug, "Successfully committed {}", update_str);
        }

        co_await remove_staging_files(std::move(update));
    }

    co_return;
}

ss::future<>
compaction_committer::remove_staging_files(object_output_t update) {
    static constexpr size_t max_concurrent_removal = 1024;
    co_await ss::max_concurrent_for_each(
      update.staging_files_and_md_infos,
      max_concurrent_removal,
      [](auto& file_and_md_info) {
          return file_and_md_info.staging_file->remove();
      });
}

} // namespace cloud_topics::l1
