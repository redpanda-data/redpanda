/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/read_replica/state_refresh_loop.h"

#include "bytes/iobuf.h"
#include "cloud_topics/level_one/metastore/manifest_io.h"
#include "cloud_topics/logger.h"
#include "cloud_topics/read_replica/snapshot_manager.h"
#include "cloud_topics/read_replica/snapshot_metastore.h"
#include "cloud_topics/read_replica/stm.h"
#include "config/configuration.h"
#include "hashing/murmur.h"
#include "serde/rw/rw.h"
#include "ssx/sleep_abortable.h"

#include <seastar/coroutine/as_future.hh>

namespace cloud_topics::read_replica {

namespace {
state_refresh_loop::errc translate_stm_errc(stm::errc e) {
    using enum stm::errc;
    switch (e) {
    case not_leader:
        return state_refresh_loop::errc::not_leader;
    case raft_error:
        return state_refresh_loop::errc::replication_error;
    case shutting_down:
        return state_refresh_loop::errc::io_error;
    }
}

ss::future<std::expected<update_metadata_update, state_refresh_loop::error>>
build_update(
  const l1::domain_uuid& domain,
  snapshot_handle& snap,
  const model::topic_id_partition& tidp) {
    auto& metastore = *snap.metastore;
    auto offsets_res = co_await metastore.get_offsets(tidp);
    if (!offsets_res.has_value()) {
        using enum l1::metastore::errc;
        switch (offsets_res.error()) {
        case missing_ntp:
        case out_of_range:
            // The partition isn't in the metastore (yet). Treat it as empty.
            co_return update_metadata_update{
              .domain = domain,
              .seqno = snap.seqno,
              .start_offset = kafka::offset{0},
              .next_offset = kafka::offset{0},
              .latest_term = model::term_id{0},
            };
        case invalid_request:
        case transport_error:
            co_return std::unexpected(
              state_refresh_loop::error(
                state_refresh_loop::errc::io_error,
                "Failed to get offsets: {}",
                offsets_res.error()));
        }
    }
    auto term_res = co_await metastore.get_term_for_offset(
      tidp, offsets_res->next_offset);
    if (!term_res.has_value()) {
        co_return std::unexpected(
          state_refresh_loop::error(
            state_refresh_loop::errc::io_error,
            "Failed to get term: {}",
            term_res.error()));
    }
    co_return update_metadata_update{
      .domain = domain,
      .seqno = snap.seqno,
      .start_offset = offsets_res->start_offset,
      .next_offset = offsets_res->next_offset,
      .latest_term = term_res.value(),
    };
}

} // namespace

void state_refresh_loop::log_error(
  std::string_view prefix, state_refresh_loop::error e) {
    ss::log_level lvl{};
    switch (e.e) {
    case errc::metastore_manifest_error:
        lvl = ss::log_level::error;
        break;
    case errc::io_error:
    case errc::replication_error:
        lvl = ss::log_level::warn;
        break;
    case errc::not_leader:
        lvl = ss::log_level::debug;
        break;
    }
    vlogl(logger_, lvl, "{}{}", prefix, e);
}

state_refresh_loop::state_refresh_loop(
  model::term_id expected_term,
  model::topic_id_partition tidp,
  ss::shared_ptr<stm> stm,
  snapshot_manager* snapshot_mgr,
  cloud_storage_clients::bucket_name bucket,
  cloud_storage::remote_label remote_label,
  cloud_io::remote& remote)
  : expected_term_(expected_term)
  , tidp_(tidp)
  , stm_(std::move(stm))
  , snapshot_mgr_(snapshot_mgr)
  , bucket_(std::move(bucket))
  , remote_label_(remote_label)
  , remote_(remote)
  , metastore_manifest_io_(std::make_unique<l1::manifest_io>(remote_, bucket_))
  , logger_(
      cd_log,
      fmt::format(
        "tidp: {}, expected_term: {}, bucket: {}, remote label: {}",
        tidp,
        expected_term_,
        bucket,
        remote_label_)) {}

state_refresh_loop::~state_refresh_loop() = default;

void state_refresh_loop::start() {
    ssx::spawn_with_gate(gate_, [this] { return run_loop(); });
}

ss::future<> state_refresh_loop::stop_and_wait() {
    as_.request_abort();
    co_await gate_.close();
}

ss::future<> state_refresh_loop::run_loop() {
    auto sync_interval
      = config::shard_local_cfg()
          .cloud_storage_readreplica_manifest_sync_timeout_ms();
    auto log_exit = ss::defer([&] { vlog(logger_.debug, "Exiting loop"); });

    // First, if we haven't replicated anything before, figure out what domain
    // this partition should sync with.
    l1::domain_uuid domain = stm_->domain();
    while (domain().is_nil() && !as_.abort_requested() && is_leader()) {
        auto domain_res = co_await discover_domain();
        if (!domain_res) {
            log_error("Failed to discover domain: ", domain_res.error());
            co_await ssx::sleep_abortable(1s, as_);
            continue;
        }
        domain = domain_res.value();
    }

    // Refresh right away, given we just became leader and want to get an
    // up-to-date view of state.
    auto next_min_refresh = ss::lowres_clock::now();
    while (!as_.abort_requested() && is_leader()) {
        auto sync_res = co_await refresh_state_from_cloud(
          domain, next_min_refresh);
        if (!sync_res) {
            log_error("Failed to sync metadata: ", sync_res.error());
            co_await ssx::sleep_abortable(1s, as_);
            continue;
        }
        // Now that we've replicated, cache the time so that in our next
        // iteration (after sleeping) we'll get a fresher snapshot than the one
        // we just refreshed with, but one not so new that we're constantly
        // forcing a database refresh.
        next_min_refresh = ss::lowres_clock::now();
        co_await ssx::sleep_abortable(sync_interval, as_);
    }
}

bool state_refresh_loop::is_leader() const {
    auto raft = stm_->raft();
    return raft->is_leader() && raft->confirmed_term() == expected_term_;
}

ss::future<std::expected<l1::domain_uuid, state_refresh_loop::error>>
state_refresh_loop::discover_domain() {
    vlog(logger_.debug, "Discovering domain");
    auto manifest_res = co_await metastore_manifest_io_
                          ->download_metastore_manifest(remote_label_);
    if (!manifest_res.has_value()) {
        errc out_e{};
        switch (manifest_res.error().e) {
            using enum l1::manifest_io::errc;
        case not_found:
        case timedout:
        case failed:
            out_e = errc::io_error;
            break;
        case shutting_down:
            out_e = errc::not_leader;
            break;
        }
        co_return std::unexpected(
          std::move(manifest_res.error())
            .wrap(std::move(out_e), "Failed to download metastore manifest"));
    }

    auto& manifest = manifest_res.value();
    if (manifest.domains.empty()) {
        co_return std::unexpected(error(
          errc::metastore_manifest_error, "Metastore manifest has no domains"));
    }
    if (manifest.partitioning_strategy != "murmur") {
        co_return std::unexpected(error(
          errc::metastore_manifest_error,
          "Unsupported partitioning strategy in manifest: {}",
          manifest.partitioning_strategy));
    }

    // Hash the topic_id_partition to find the domain index
    iobuf temp;
    serde::write(temp, tidp_);
    auto bytes = iobuf_to_bytes(temp);
    auto idx = murmur2(bytes.data(), bytes.size()) % manifest.domains.size();
    auto domain = manifest.domains[idx];
    vlog(
      logger_.debug,
      "Discovered domain {} (index {} of {})",
      domain,
      idx,
      manifest.domains.size());
    co_return domain;
}

ss::future<std::expected<void, state_refresh_loop::error>>
state_refresh_loop::refresh_state_from_cloud(
  l1::domain_uuid domain, ss::lowres_clock::time_point min_refresh_time) {
    auto term_res = co_await stm_->sync(std::chrono::seconds(30), as_);
    if (!term_res.has_value()) {
        auto e = translate_stm_errc(term_res.error().e);
        co_return std::unexpected(
          std::move(term_res.error())
            .wrap(std::move(e), "Failed to sync for term {}", expected_term_));
    }
    if (term_res.value() != expected_term_) {
        co_return std::unexpected(error(
          errc::not_leader,
          "Current term: {}, expected: {}",
          term_res.value(),
          expected_term_));
    }
    auto sync_interval
      = config::shard_local_cfg()
          .cloud_storage_readreplica_manifest_sync_timeout_ms();
    auto last_seqno = stm_->get_state().seqno;
    auto snapshot_res = co_await snapshot_mgr_->get_snapshot(
      domain, bucket_, min_refresh_time, last_seqno, sync_interval);
    if (!snapshot_res.has_value()) {
        co_return std::unexpected(
          state_refresh_loop::error(
            state_refresh_loop::errc::io_error,
            "Failed to get snapshot: {}",
            snapshot_res.error()));
    }
    auto build_res = co_await build_update(domain, snapshot_res.value(), tidp_);
    if (!build_res.has_value()) {
        co_return std::unexpected(build_res.error());
    }
    auto& update = build_res.value();

    const auto& state = stm_->get_state();
    if (!update.can_apply(state)) {
        vlog(
          logger_.debug,
          "Metadata update can't be updated, likely a no-op: update: {}, "
          "state: {}",
          update,
          state);
        co_return std::expected<void, state_refresh_loop::error>();
    }
    auto replicate_res = co_await stm_->update(
      expected_term_, std::move(update), as_);
    if (!replicate_res.has_value()) {
        auto e = translate_stm_errc(replicate_res.error().e);
        co_return std::unexpected(
          state_refresh_loop::error(
            e, "Failed to replicate metadata: {}", replicate_res.error()));
    }

    co_return std::expected<void, state_refresh_loop::error>();
}

} // namespace cloud_topics::read_replica
