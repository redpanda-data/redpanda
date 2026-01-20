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
#include "cloud_topics/level_one/metastore/retry.h"
#include "model/timeout_clock.h"
#include "ssx/future-util.h"
#include "ssx/when_all.h"

#include <exception>

namespace cloud_topics::l1 {

compaction_committer::compaction_job::compaction_job(
  compaction_job_id id,
  model::topic_id_partition tidp,
  std::unique_ptr<metastore::object_metadata_builder> metadata_builder,
  compaction_committer* committer,
  io* io,
  metastore* metastore,
  committing_policy* policy,
  ss::gate::holder holder)
  : _id(id)
  , _tp(tidp)
  , _metadata_builder(std::move(metadata_builder))
  , _upload_sem(0, fmt::format("upload_sem_{}", id))
  , _committer(committer)
  , _io(io)
  , _metastore(metastore)
  , _policy(policy)
  , _holder(holder) {
    start_upload_loop();
}

void compaction_committer::compaction_job::add_l1_object(
  file_and_md_info file_and_info) {
    vassert(
      _state == compaction_job::state::in_progress,
      "Cannot add l1 object to finalized job.");

    vlog(
      compaction_log.trace,
      "Pushing file of size {}, offsets ({}~{}) for compaction of tidp {}, job "
      "{}",
      file_and_info.info.size_bytes,
      file_and_info.ntp_md.base_offset,
      file_and_info.ntp_md.last_offset,
      file_and_info.ntp_md.tidp,
      _id);

    auto update_response = _policy->on_update(file_and_info);

    _staging_file_and_md_infos.push_back(std::move(file_and_info));

    if (update_response == committing_policy::update_response::preempt) {
        _upload_sem.signal();
    }
}

ss::future<> compaction_committer::compaction_job::finalize(
  chunked_vector<metastore::compaction_update::cleaned_range>
    new_cleaned_ranges,
  offset_interval_set removed_tombstone_ranges,
  metastore::compaction_epoch expected_compaction_epoch) {
    co_await do_finalize(
      std::move(new_cleaned_ranges),
      std::move(removed_tombstone_ranges),
      expected_compaction_epoch);

    if (!_inflight_uploads.empty()) {
        // Await and discard unresolved inflight futures, if any.
        auto res = co_await ss::coroutine::as_future(
          do_await_inflight_uploads());
        res.ignore_ready_future();
    }

    if (!_staging_file_and_md_infos.empty()) {
        // Remove leftover staging files, if any.
        co_await remove_staging_files();
    }

    _committer->finalize_job(_id);
}

void compaction_committer::compaction_job::cancel_job() {
    _as.request_abort();
    _upload_sem.broken();
    _last_upload_scheduled.broken();
}

ss::future<> compaction_committer::compaction_job::remove_staging_files() {
    static constexpr size_t max_concurrent_removal = 1024;
    co_await ss::max_concurrent_for_each(
      _staging_file_and_md_infos,
      max_concurrent_removal,
      [](auto& file_and_md_info) {
          return file_and_md_info.staging_file->remove();
      });
}

bool compaction_committer::compaction_job::all_uploads_inflight() const {
    return _state == compaction_job::state::finalized
           && _staging_file_and_md_infos.empty();
}

ss::future<> compaction_committer::compaction_job::upload_loop() {
    while (!_as.abort_requested() && !all_uploads_inflight()) {
        constexpr std::chrono::seconds poll_frequency(10);
        try {
            co_await _upload_sem.wait(
              poll_frequency, std::max(_upload_sem.current(), size_t(1)));
        } catch (const ss::semaphore_timed_out&) {
            // Fall through
        }

        if (!_staging_file_and_md_infos.empty()) {
            if (_policy->should_commit()) {
                upload_some();
            }
        }
    }

    _last_upload_scheduled.signal();
}

void compaction_committer::compaction_job::start_upload_loop() {
    ssx::background = upload_loop().handle_exception(
      [this](const std::exception_ptr& e) {
          auto log_level = ssx::is_shutdown_exception(e) ? ss::log_level::debug
                                                         : ss::log_level::warn;
          vlogl(
            compaction_log,
            log_level,
            "Encountered exception in upload loop for job {}: {}",
            _id,
            e);
      });
}

ss::future<compaction_committer::compaction_job::expected_t>
compaction_committer::compaction_job::do_upload(
  staging_file* file,
  object_builder::object_info info,
  metastore::object_metadata::ntp_metadata ntp_md) {
    auto& metadata_builder = _metadata_builder;
    auto oid_res = metadata_builder->create_object_for(_tp);
    if (!oid_res.has_value()) {
        co_return std::unexpected(
          error{
            .t = error::type::builder_failure,
            .msg = fmt::format("{}", oid_res.error())});
    }

    auto oid = std::move(oid_res).value();

    auto put_res = co_await _io->put_object(oid, file, &_as);

    if (!put_res.has_value()) {
        std::ignore = metadata_builder->remove_pending_object(oid);
        co_return std::unexpected(
          error{
            .t = error::type::io_failure,
            .msg = fmt::format(
              "Failed to put object {}: {}",
              oid,
              static_cast<int>(put_res.error()))});
    }

    vlog(
      compaction_log.trace,
      "Uploaded file {} ({}~{}) as part of compaction of tidp {}, job {}",
      oid,
      ntp_md.base_offset,
      ntp_md.last_offset,
      _tp,
      _id);

    auto add_res = metadata_builder->add(oid, std::move(ntp_md));
    if (!add_res.has_value()) {
        std::ignore = metadata_builder->remove_pending_object(oid);
        co_return std::unexpected(
          error{
            .t = error::type::builder_failure,
            .msg = fmt::format("{}", add_res.error())});
    }

    auto finish_res = metadata_builder->finish(
      oid, info.footer_offset, info.size_bytes);
    if (!finish_res.has_value()) {
        std::ignore = metadata_builder->remove_pending_object(oid);
        co_return std::unexpected(
          error{
            .t = error::type::builder_failure,
            .msg = fmt::format("{}", finish_res.error())});
    }

    co_return expected_t{};
}

ss::future<compaction_committer::compaction_job::expected_t>
compaction_committer::compaction_job::upload_file(
  file_and_md_info file_and_info) {
    auto file = std::move(file_and_info.staging_file);
    auto res = co_await do_upload(
      file.get(),
      std::move(file_and_info.info),
      std::move(file_and_info.ntp_md));
    co_await file->remove();
    co_return res;
}

void compaction_committer::compaction_job::upload_some() {
    auto updates = std::exchange(_staging_file_and_md_infos, {});
    while (!updates.empty()) {
        auto update = std::move(updates.front());
        updates.pop_front();
        auto inflight_upload = upload_file(std::move(update));
        _inflight_uploads.push_back(std::move(inflight_upload));
    }
}

ss::future<chunked_vector<compaction_committer::compaction_job::expected_t>>
compaction_committer::compaction_job::do_await_inflight_uploads() {
    auto inflight_uploads = std::exchange(_inflight_uploads, {});
    return ssx::when_all_succeed<chunked_vector<expected_t>>(
      std::move(inflight_uploads));
}

ss::future<std::optional<ss::sstring>>
compaction_committer::compaction_job::await_inflight_uploads() {
    co_await _last_upload_scheduled.wait();

    auto res = co_await do_await_inflight_uploads();

    auto failed = res | std::views::filter([](const expected_t& res) {
                      return !res.has_value();
                  });

    if (!failed.empty()) {
        co_return fmt::format(
          "{}",
          fmt::join(
            failed | std::views::transform([&](const expected_t& res) {
                return res.error();
            }),
            ", "));
    }

    co_return std::nullopt;
}

ss::future<std::expected<void, metastore::errc>>
compaction_committer::compaction_job::do_compact_objects(
  metastore::compaction_map_t compact_map) {
    co_return co_await l1::retry_metastore_op_with_default_rtc(
      [this, &compact_map]() {
          return _metastore->compact_objects(*_metadata_builder, compact_map);
      },
      _as);
}

ss::future<>
compaction_committer::compaction_job::compact_objects_without_update(
  metastore::compaction_epoch expected_compaction_epoch) {
    auto compaction_update = metastore::compaction_update{
      .new_cleaned_ranges = {},
      .removed_tombstones_ranges = {},
      .cleaned_at = model::timestamp::missing(),
      .expected_compaction_epoch = expected_compaction_epoch};

    metastore::compaction_map_t compact_map;
    compact_map.emplace(_tp, std::move(compaction_update));
    auto replace_res = co_await do_compact_objects(std::move(compact_map));
    if (replace_res.has_value()) {
        vlog(
          compaction_log.info,
          "Finalized compaction of tidp {}, job id {} without a compaction "
          "metadata update",
          _tp,
          _id);
    } else {
        vlog(
          compaction_log.warn,
          "Could not commit object update during compaction of tidp {}, job id "
          "{}: {}.",
          _tp,
          _id,
          replace_res.error());
    }
}

ss::future<> compaction_committer::compaction_job::compact_objects_with_update(
  chunked_vector<metastore::compaction_update::cleaned_range>
    new_cleaned_ranges,
  offset_interval_set removed_tombstone_ranges,
  metastore::compaction_epoch expected_compaction_epoch) {
    auto compaction_update = metastore::compaction_update{
      .new_cleaned_ranges = std::move(new_cleaned_ranges),
      .removed_tombstones_ranges = std::move(removed_tombstone_ranges),
      .cleaned_at = model::timestamp::now(),
      .expected_compaction_epoch = expected_compaction_epoch};

    auto compaction_update_str = fmt::format("{}", compaction_update);
    metastore::compaction_map_t compact_map;
    compact_map.emplace(_tp, std::move(compaction_update));
    auto commit_res = co_await do_compact_objects(std::move(compact_map));

    if (commit_res.has_value()) {
        vlog(
          compaction_log.info,
          "Finalized compaction of tidp {}, job id {} with compaction metadata "
          "update {}",
          _tp,
          _id,
          compaction_update_str);
    } else {
        vlog(
          compaction_log.warn,
          "Could not commit metadata update {} for compaction of tidp {}, job "
          "id {}: {}. Retrying object update without metadata.",
          compaction_update_str,
          _tp,
          _id,
          commit_res.error());
        // We couldn't commit the metastore update, but we should at least try
        // to replace the objects so as not to discard our hard IO work.
        co_return co_await compact_objects_without_update(
          expected_compaction_epoch);
    }
}

ss::future<> compaction_committer::compaction_job::do_finalize(
  chunked_vector<metastore::compaction_update::cleaned_range>
    new_cleaned_ranges,
  offset_interval_set removed_tombstone_ranges,
  metastore::compaction_epoch expected_compaction_epoch) {
    vlog(
      compaction_log.debug,
      "Marking job {} for tidp {} as finalized",
      _id,
      _tp);
    _state = compaction_job::state::finalized;
    _upload_sem.signal();

    vlog(
      compaction_log.debug,
      "Awaiting completion of uploads for job {}, tidp {}",
      _id,
      _tp);

    auto fut = co_await ss::coroutine::as_future(await_inflight_uploads());

    if (_metadata_builder->is_empty()) {
        // There is no update to push.
        vlog(
          compaction_log.debug,
          "No built or uploaded objects for job {}, tidp {}.",
          _id,
          _tp);

        fut.ignore_ready_future();
        co_return;
    }

    if (fut.failed()) {
        auto e = fut.get_exception();
        auto is_shutdown_exception = ssx::is_shutdown_exception(e);
        auto lvl = is_shutdown_exception ? ss::log_level::debug
                                         : ss::log_level::warn;
        vlogl(
          compaction_log,
          lvl,
          "Failed to put all compaction objects for job {} to object storage: "
          "{}",
          _id,
          e);

        if (is_shutdown_exception) {
            co_return;
        }

        // If an error was encountered during committing, don't attempt to
        // make a compaction update for `compact_objects` with the
        // `metastore`- just replace the objects.
        co_return co_await compact_objects_without_update(
          expected_compaction_epoch);
    }

    auto res = std::move(fut).get();
    if (res.has_value()) {
        vlog(
          compaction_log.warn,
          "Failed to put all compaction objects for job {} to object "
          "storage: {}",
          _id,
          res.value());
        co_return co_await compact_objects_without_update(
          expected_compaction_epoch);
    }

    co_return co_await compact_objects_with_update(
      std::move(new_cleaned_ranges),
      std::move(removed_tombstone_ranges),
      expected_compaction_epoch);
}

compaction_committer::compaction_committer(
  std::unique_ptr<committing_policy> policy, io* io, metastore* metastore)
  : _policy(std::move(policy))
  , _io(io)
  , _metastore(metastore) {}

ss::future<> compaction_committer::start() { co_return; }

ss::future<> compaction_committer::stop() {
    vlog(
      compaction_log.info,
      "Stopping compaction committer with {} active jobs.",
      _compaction_jobs.size());
    _as.request_abort();
    auto close_fut = _gate.close();
    cancel_active_jobs();
    co_await std::move(close_fut);
}

ss::future<compaction_committer::compaction_job*>
compaction_committer::begin_compaction_job(model::topic_id_partition tidp) {
    auto id = create_compaction_job_id();

    auto metadata_builder_res
      = co_await l1::retry_metastore_op_with_default_rtc(
        [this]() { return _metastore->object_builder(); }, _as);
    if (!metadata_builder_res.has_value()) {
        vlog(
          compaction_log.warn,
          "Could not create object metadata builder for compaction of tidp {}: "
          "{}. Aborting.",
          tidp,
          metadata_builder_res.error());
        throw std::runtime_error("Couldn't begin compaction job");
    }
    auto metadata_builder = std::move(metadata_builder_res).value();

    auto [it, emplaced] = _compaction_jobs.emplace(
      id,
      std::make_unique<compaction_job>(
        id,
        tidp,
        std::move(metadata_builder),
        this,
        _io,
        _metastore,
        _policy.get(),
        _gate.hold()));
    vassert(emplaced, "Expected emplace() to succeed. Concurrency issue?");

    vlog(
      compaction_log.debug,
      "Started compaction job for tidp {} under id {}",
      tidp,
      id);

    co_return it->second.get();
}

void compaction_committer::finalize_job(compaction_job_id id) {
    // Now safe to extract/remove the underlying job state from
    // `_compaction_jobs`.
    auto job_ptr_opt = _compaction_jobs.extract(id);
    vassert(job_ptr_opt.has_value(), "concurrency issue?");
    auto job_ptr = std::move(job_ptr_opt.value().second);
}

void compaction_committer::cancel_active_jobs() {
    for (auto& [_, job] : _compaction_jobs) {
        job->cancel_job();
    }
}

} // namespace cloud_topics::l1
