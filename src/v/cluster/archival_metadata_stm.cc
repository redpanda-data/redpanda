// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/archival_metadata_stm.h"

#include "cloud_storage/partition_manifest.h"
#include "cloud_storage/remote.h"
#include "cloud_storage/types.h"
#include "cluster/errc.h"
#include "cluster/persisted_stm.h"
#include "config/configuration.h"
#include "features/feature_table.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record.h"
#include "model/record_batch_types.h"
#include "model/record_utils.h"
#include "raft/consensus.h"
#include "resource_mgmt/io_priority.h"
#include "serde/envelope.h"
#include "serde/serde.h"
#include "ssx/future-util.h"
#include "storage/record_batch_builder.h"
#include "utils/fragmented_vector.h"
#include "utils/named_type.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/sleep.hh>

#include <algorithm>

namespace cluster {

namespace {

using cmd_key = named_type<uint8_t, struct cmd_key_tag>;

} // namespace

struct archival_metadata_stm::segment
  : public serde::
      envelope<segment, serde::version<0>, serde::compat_version<0>> {
    // ntp_revision is needed to reconstruct full remote path of
    // the segment. Deprecated because ntp_revision is now part of
    // segment_meta.
    model::initial_revision_id ntp_revision_deprecated;
    cloud_storage::segment_name name;
    cloud_storage::partition_manifest::segment_meta meta;
};

struct archival_metadata_stm::start_offset
  : public serde::
      envelope<segment, serde::version<0>, serde::compat_version<0>> {
    model::offset start_offset;
};

struct archival_metadata_stm::add_segment_cmd {
    static constexpr cmd_key key{0};

    using value = segment;
};

struct archival_metadata_stm::truncate_cmd {
    static constexpr cmd_key key{1};

    using value = start_offset;
};

struct archival_metadata_stm::update_start_offset_cmd {
    static constexpr cmd_key key{2};

    using value = start_offset;
};

struct archival_metadata_stm::cleanup_metadata_cmd {
    static constexpr cmd_key key{3};
};

struct archival_metadata_stm::reset_metadata_cmd {
    static constexpr cmd_key key{8};

    // Unused, left available in case it's useful to pass in further arguments.
    using value = iobuf;
};

struct archival_metadata_stm::snapshot
  : public serde::
      envelope<snapshot, serde::version<1>, serde::compat_version<0>> {
    /// List of segments
    fragmented_vector<segment> segments;
    /// List of replaced segments
    fragmented_vector<segment> replaced;
    /// Start offset (might be different from the base offset of the first
    /// segment). Default value means that the snapshot was old and didn't
    /// have start_offset. In this case we need to set it to compute it from
    /// segments.
    model::offset start_offset;
    /// Last uploaded offset (default value means that the snapshot was created
    /// using older version (snapshot v0) and we need to rebuild the offset from
    /// segments)
    model::offset last_offset;
    /// Last uploaded offset belonging to a compacted segment. If set to
    /// default, the next upload attempt will align this with start of manifest.
    model::offset last_uploaded_compacted_offset;
};

inline archival_metadata_stm::segment
segment_from_meta(const cloud_storage::segment_meta& meta) {
    auto name = cloud_storage::generate_local_segment_name(
      meta.base_offset, meta.segment_term);
    return archival_metadata_stm::segment{
      .ntp_revision_deprecated = meta.ntp_revision,
      .name = std::move(name),
      .meta = meta};
}

command_batch_builder::command_batch_builder(
  archival_metadata_stm& stm,
  ss::lowres_clock::time_point deadline,
  std::optional<std::reference_wrapper<ss::abort_source>> as)
  : _stm(stm)
  , _builder(model::record_batch_type::archival_metadata, model::offset(0))
  , _deadline(deadline)
  , _as(as)
  , _holder(stm._gate) {}

command_batch_builder& command_batch_builder::reset_metadata() {
    iobuf key_buf = serde::to_iobuf(
      archival_metadata_stm::reset_metadata_cmd::key);
    iobuf empty_buf;
    _builder.add_raw_kv(std::move(key_buf), std::move(empty_buf));
    return *this;
}

command_batch_builder& command_batch_builder::add_segments(
  std::vector<cloud_storage::segment_meta> add_segments) {
    for (auto& meta : add_segments) {
        iobuf key_buf = serde::to_iobuf(
          archival_metadata_stm::add_segment_cmd::key);
        if (meta.ntp_revision == model::initial_revision_id{}) {
            meta.ntp_revision = _stm.get()._manifest->get_revision_id();
        }
        auto record_val = archival_metadata_stm::add_segment_cmd::value{
          segment_from_meta(meta)};
        iobuf val_buf = serde::to_iobuf(std::move(record_val));
        _builder.add_raw_kv(std::move(key_buf), std::move(val_buf));
    }
    return *this;
}

command_batch_builder& command_batch_builder::cleanup_metadata() {
    // NOTE: the method doesn't check if the manifest has any data to cleanup.
    // This is needed because the cleanup_metadata_cmd command can be batched
    // together with other commands which will create some garbage to cleanup.
    iobuf key_buf = serde::to_iobuf(
      archival_metadata_stm::cleanup_metadata_cmd::key);
    iobuf empty_body;
    _builder.add_raw_kv(std::move(key_buf), std::move(empty_body));
    return *this;
}

command_batch_builder&
command_batch_builder::truncate(model::offset start_rp_offset) {
    iobuf key_buf = serde::to_iobuf(
      archival_metadata_stm::update_start_offset_cmd::key);
    auto record_val = archival_metadata_stm::update_start_offset_cmd::value{
      .start_offset = start_rp_offset};
    iobuf val_buf = serde::to_iobuf(record_val);
    _builder.add_raw_kv(std::move(key_buf), std::move(val_buf));
    return *this;
}

ss::future<std::error_code> command_batch_builder::replicate() {
    if (_as) {
        _as->get().check();
    }
    return _stm.get()._lock.with([this]() {
        vlog(
          _stm.get()._logger.debug, "command_batch_builder::replicate called");
        auto now = ss::lowres_clock::now();
        auto timeout = now < _deadline ? _deadline - now : 0ms;
        return _stm.get().sync(timeout).then([this](bool success) {
            if (!success) {
                return ss::make_ready_future<std::error_code>(errc::timeout);
            }
            auto batch = std::move(_builder).build();
            return _stm.get().do_replicate_commands(std::move(batch), _as);
        });
    });
}

command_batch_builder archival_metadata_stm::batch_start(
  ss::lowres_clock::time_point deadline,
  std::optional<std::reference_wrapper<ss::abort_source>> as) {
    return {*this, deadline, as};
}

fragmented_vector<archival_metadata_stm::segment>
archival_metadata_stm::segments_from_manifest(
  const cloud_storage::partition_manifest& manifest) {
    fragmented_vector<segment> segments;
    for (auto [key, meta] : manifest) {
        if (meta.ntp_revision == model::initial_revision_id{}) {
            meta.ntp_revision = manifest.get_revision_id();
        }
        // NOTE: manifest should have the 'segment_term' set to some
        // meaningful value in this place. During deserialization from
        // json it's set from the segment name (if it's not present in
        // the segment_meta). During deserialization of the archival snapshot
        // it's also initialized from the segment name if it's missing in
        // metadata.
        vassert(
          meta.segment_term != model::term_id{},
          "segment_term is invalid in segment with base offset {}",
          meta.base_offset);
        segments.push_back(segment_from_meta(meta));
    }

    return segments;
}

fragmented_vector<archival_metadata_stm::segment>
archival_metadata_stm::replaced_segments_from_manifest(
  const cloud_storage::partition_manifest& manifest) {
    auto replaced = manifest.replaced_segments();
    fragmented_vector<segment> segments;
    for (auto meta : replaced) {
        if (meta.ntp_revision == model::initial_revision_id{}) {
            meta.ntp_revision = manifest.get_revision_id();
        }
        segments.push_back(segment_from_meta(meta));
    }

    return segments;
}

ss::future<> archival_metadata_stm::make_snapshot(
  const storage::ntp_config& ntp_cfg,
  const cloud_storage::partition_manifest& m,
  model::offset insync_offset) {
    // Create archival_stm_snapshot
    auto segments = segments_from_manifest(m);
    auto replaced = replaced_segments_from_manifest(m);
    iobuf snap_data = serde::to_iobuf(snapshot{
      .segments = std::move(segments),
      .replaced = std::move(replaced),
      .start_offset = m.get_start_offset().value_or(model::offset{}),
      .last_offset = m.get_last_offset(),
      .last_uploaded_compacted_offset
      = m.get_last_uploaded_compacted_offset()});

    auto snapshot = stm_snapshot::create(
      0, insync_offset, std::move(snap_data));

    storage::simple_snapshot_manager tmp_snapshot_mgr(
      std::filesystem::path(ntp_cfg.work_directory()),
      "archival_metadata.snapshot",
      raft_priority());

    co_await persist_snapshot(tmp_snapshot_mgr, std::move(snapshot));
}

archival_metadata_stm::archival_metadata_stm(
  raft::consensus* raft,
  cloud_storage::remote& remote,
  features::feature_table& ft,
  ss::logger& logger)
  : cluster::persisted_stm("archival_metadata.snapshot", logger, raft)
  , _logger(logger, ssx::sformat("ntp: {}", raft->ntp()))
  , _manifest(ss::make_shared<cloud_storage::partition_manifest>(
      raft->ntp(), raft->log_config().get_initial_revision()))
  , _cloud_storage_api(remote)
  , _feature_table(ft) {}

ss::future<std::error_code> archival_metadata_stm::truncate(
  model::offset start_rp_offset,
  ss::lowres_clock::time_point deadline,
  std::optional<std::reference_wrapper<ss::abort_source>> as) {
    auto now = ss::lowres_clock::now();
    auto timeout = now < deadline ? deadline - now : 0ms;
    return _lock.with(timeout, [this, start_rp_offset, deadline, as] {
        return do_truncate(start_rp_offset, deadline, as);
    });
}

ss::future<std::error_code> archival_metadata_stm::cleanup_metadata(
  ss::lowres_clock::time_point deadline,
  std::optional<std::reference_wrapper<ss::abort_source>> as) {
    auto now = ss::lowres_clock::now();
    auto timeout = now < deadline ? deadline - now : 0ms;
    return _lock.with(timeout, [this, deadline, as] {
        return do_cleanup_metadata(deadline, as);
    });
}

ss::future<std::error_code> archival_metadata_stm::do_replicate_commands(
  model::record_batch batch,
  std::optional<std::reference_wrapper<ss::abort_source>> as) {
    auto current_term = _insync_term;
    auto fut = _raft->replicate(
      current_term,
      model::make_memory_record_batch_reader(std::move(batch)),
      raft::replicate_options{raft::consistency_level::quorum_ack});

    // Run with an abort source so shutdown doesn't have to wait a full
    // replication timeout to proceed.
    if (as) {
        fut = ssx::with_timeout_abortable(
          std::move(fut), model::no_timeout, *as);
    }

    auto result = co_await std::move(fut);
    if (!result) {
        vlog(
          _logger.warn,
          "error on replicating remote segment metadata: {}",
          result.error());
        // If there was an error for whatever reason, it is unsafe to make any
        // assumptions about whether batches were replicated or not. Explicitly
        // step down if we're still leader and force callers to re-sync in a
        // new term with a new leader.
        if (_c->is_leader() && _c->term() == current_term) {
            co_await _c->step_down(ssx::sformat(
              "failed to replicate archival batch in term {}", current_term));
        }
        co_return result.error();
    }

    auto applied = co_await wait_no_throw(
      result.value().last_offset, model::no_timeout, as);
    if (!applied) {
        if (
          as.has_value() && !as.value().get().abort_requested()
          && _c->is_leader() && _c->term() == current_term) {
            co_await _c->step_down(ssx::sformat(
              "failed to replicate archival batch in term {}", current_term));
        }
        co_return errc::replication_error;
    }

    co_return errc::success;
}

ss::future<std::error_code> archival_metadata_stm::do_truncate(
  model::offset start_rp_offset,
  ss::lowres_clock::time_point deadline,
  std::optional<std::reference_wrapper<ss::abort_source>> as) {
    vlog(
      _logger.trace,
      "do_truncate called, old so {}, new so {}",
      get_start_offset(),
      start_rp_offset);
    {
        auto now = ss::lowres_clock::now();
        auto timeout = now < deadline ? deadline - now : 0ms;
        if (!co_await sync(timeout)) {
            co_return errc::timeout;
        }
    }

    if (as) {
        as->get().check();
    }

    if (get_start_offset() > start_rp_offset) {
        co_return errc::success;
    }

    storage::record_batch_builder b(
      model::record_batch_type::archival_metadata, model::offset(0));
    iobuf key_buf = serde::to_iobuf(update_start_offset_cmd::key);
    auto record_val = update_start_offset_cmd::value{
      .start_offset = start_rp_offset};
    iobuf val_buf = serde::to_iobuf(record_val);
    b.add_raw_kv(std::move(key_buf), std::move(val_buf));

    auto batch = std::move(b).build();

    auto ec = co_await do_replicate_commands(std::move(batch), as);
    if (ec) {
        co_return ec;
    }

    vlog(
      _logger.info,
      "truncate command replicated, truncated up to {}, remote start_offset: "
      "{}, last_offset: {}",
      start_rp_offset,
      get_start_offset(),
      get_last_offset());

    co_return errc::success;
}

bool archival_metadata_stm::cleanup_needed() const {
    auto has_replaced_segments = !_manifest->replaced_segments().empty();
    auto has_trailing_segments = (_manifest->size() > 0
                                  && _manifest->get_start_offset())
                                   ? _manifest->begin()->first
                                       < _manifest->get_start_offset()
                                   : false;

    return has_replaced_segments || has_trailing_segments;
}

ss::future<std::error_code> archival_metadata_stm::do_cleanup_metadata(
  ss::lowres_clock::time_point deadline,
  std::optional<std::reference_wrapper<ss::abort_source>> as) {
    vlog(_logger.debug, "do_cleanup_metadata called");
    {
        auto now = ss::lowres_clock::now();
        auto timeout = now < deadline ? deadline - now : 0ms;
        if (!co_await sync(timeout)) {
            co_return errc::timeout;
        }
    }

    if (as) {
        as->get().check();
    }

    if (!cleanup_needed()) {
        vlog(_logger.debug, "no metadata to clean up");
        co_return errc::success;
    }

    storage::record_batch_builder b(
      model::record_batch_type::archival_metadata, model::offset(0));
    iobuf key_buf = serde::to_iobuf(cleanup_metadata_cmd::key);
    iobuf empty_body;
    b.add_raw_kv(std::move(key_buf), std::move(empty_body));

    auto batch = std::move(b).build();

    auto ec = co_await do_replicate_commands(std::move(batch), as);
    if (ec) {
        co_return ec;
    }

    vlog(_logger.debug, "cleanup_metadata command replicated");

    co_return errc::success;
}

ss::future<std::error_code> archival_metadata_stm::add_segments(
  std::vector<cloud_storage::segment_meta> segments,
  ss::lowres_clock::time_point deadline,
  std::optional<std::reference_wrapper<ss::abort_source>> as) {
    auto now = ss::lowres_clock::now();
    auto timeout = now < deadline ? deadline - now : 0ms;
    return _lock.with(
      timeout, [this, s = std::move(segments), deadline, as]() mutable {
          return do_add_segments(std::move(s), deadline, as);
      });
}

ss::future<std::error_code> archival_metadata_stm::do_add_segments(
  std::vector<cloud_storage::segment_meta> add_segments,
  ss::lowres_clock::time_point deadline,
  std::optional<std::reference_wrapper<ss::abort_source>> as) {
    {
        auto now = ss::lowres_clock::now();
        auto timeout = now < deadline ? deadline - now : 0ms;
        if (!co_await sync(timeout)) {
            co_return errc::timeout;
        }
    }

    if (as) {
        as->get().check();
    }

    if (add_segments.empty()) {
        co_return errc::success;
    }

    storage::record_batch_builder b(
      model::record_batch_type::archival_metadata, model::offset(0));
    for (auto& meta : add_segments) {
        iobuf key_buf = serde::to_iobuf(add_segment_cmd::key);
        if (meta.ntp_revision == model::initial_revision_id{}) {
            meta.ntp_revision = _manifest->get_revision_id();
        }
        auto record_val = add_segment_cmd::value{segment_from_meta(meta)};
        iobuf val_buf = serde::to_iobuf(std::move(record_val));
        b.add_raw_kv(std::move(key_buf), std::move(val_buf));
    }

    auto batch = std::move(b).build();
    auto ec = co_await do_replicate_commands(std::move(batch), as);
    if (ec) {
        co_return ec;
    }

    for (const auto& meta : add_segments) {
        auto name = cloud_storage::generate_local_segment_name(
          meta.base_offset, meta.segment_term);
        vlog(
          _logger.info,
          "new remote segment added (name: {}, base_offset: {} "
          "last_offset: "
          "{}), "
          "remote start_offset: {}, last_offset: {}",
          name,
          meta.base_offset,
          meta.committed_offset,
          get_start_offset(),
          get_last_offset());
    }

    co_return errc::success;
}

ss::future<> archival_metadata_stm::apply(model::record_batch b) {
    if (b.header().type != model::record_batch_type::archival_metadata) {
        _insync_offset = b.last_offset();
        co_return;
    }

    b.for_each_record([this](model::record&& r) {
        auto key = serde::from_iobuf<cmd_key>(r.release_key());
        switch (key) {
        case add_segment_cmd::key:
            apply_add_segment(
              serde::from_iobuf<add_segment_cmd::value>(r.release_value()));
            break;
        case truncate_cmd::key:
            // This is never executed but we have to keep
            // the handler since we might need to replay the log
            // generated by previous version.
            apply_truncate(
              serde::from_iobuf<truncate_cmd::value>(r.release_value()));
            break;
        case update_start_offset_cmd::key:
            apply_update_start_offset(
              serde::from_iobuf<update_start_offset_cmd::value>(
                r.release_value()));
            break;
        case cleanup_metadata_cmd::key:
            apply_cleanup_metadata();
            break;
        case reset_metadata_cmd::key:
            apply_reset_metadata();
            break;
        };
    });

    _insync_offset = b.last_offset();
    _manifest->advance_insync_offset(b.last_offset());
}

ss::future<> archival_metadata_stm::handle_raft_snapshot() {
    cloud_storage::partition_manifest manifest;

    auto bucket = config::shard_local_cfg().cloud_storage_bucket.value();
    vassert(bucket, "configuration property cloud_storage_bucket must be set");

    auto timeout
      = config::shard_local_cfg().cloud_storage_manifest_upload_timeout_ms();
    auto backoff = config::shard_local_cfg().cloud_storage_initial_backoff_ms();

    retry_chain_node rc_node(_download_as, timeout, backoff);
    auto res = co_await _cloud_storage_api.download_manifest(
      s3::bucket_name{*bucket},
      _manifest->get_manifest_path(),
      manifest,
      rc_node);

    if (res == cloud_storage::download_result::notfound) {
        _insync_offset = model::prev_offset(_raft->start_offset());
        set_next(_raft->start_offset());
        vlog(_logger.info, "handled log eviction, the manifest is absent");
        co_return;
    } else if (res != cloud_storage::download_result::success) {
        // sleep to the end of timeout to avoid calling handle_eviction in a
        // busy loop.
        co_await ss::sleep_abortable(rc_node.get_timeout(), _download_as);
        throw std::runtime_error{fmt::format(
          "couldn't download manifest {}: {}",
          _manifest->get_manifest_path(),
          res)};
    }

    *_manifest = std::move(manifest);
    auto start_offset = get_start_offset();

    auto iso = _manifest->get_insync_offset();
    if (iso == model::offset{}) {
        // Handle legacy manifests which don't have the 'insync_offset'
        // field.
        _insync_offset = _manifest->get_last_offset();
    } else {
        _insync_offset = iso;
    }
    auto next_offset = std::max(
      _raft->start_offset(), model::next_offset(_insync_offset));
    set_next(next_offset);

    vlog(
      _logger.info,
      "handled log eviction, next offset: {}, remote start_offset: {}, "
      "last_offset: {}",
      next_offset,
      start_offset,
      get_last_offset());
}

ss::future<> archival_metadata_stm::apply_snapshot(
  stm_snapshot_header header, iobuf&& data) {
    auto snap = serde::from_iobuf<snapshot>(std::move(data));

    if (
      snap.last_offset == model::offset{}
      || snap.start_offset == model::offset{}) {
        // Old format doesn't have start offset and last offset
        for (const auto& s : snap.segments) {
            if (snap.start_offset == model::offset{}) {
                snap.start_offset = s.meta.base_offset;
            } else {
                snap.start_offset = std::min(
                  snap.start_offset, s.meta.base_offset);
            }
            snap.last_offset = std::max(
              snap.last_offset, s.meta.committed_offset);
        }
    }
    vlog(
      _logger.info,
      "applying snapshot, so: {}, lo: {}, num segments: {}, num replaced: "
      "{}",
      snap.start_offset,
      snap.last_offset,
      snap.segments.size(),
      snap.replaced.size());

    *_manifest = cloud_storage::partition_manifest(
      _raft->ntp(),
      _raft->log_config().get_initial_revision(),
      snap.start_offset,
      snap.last_offset,
      snap.last_uploaded_compacted_offset,
      header.offset,
      snap.segments,
      snap.replaced);

    vlog(
      _logger.info,
      "applied snapshot at offset: {}, remote start_offset: {}, "
      "last_offset: "
      "{}",
      header.offset,
      get_start_offset(),
      get_last_offset());

    _last_snapshot_offset = header.offset;
    _insync_offset = header.offset;
    co_return;
}

ss::future<stm_snapshot> archival_metadata_stm::take_snapshot() {
    auto segments = segments_from_manifest(*_manifest);
    auto replaced = replaced_segments_from_manifest(*_manifest);
    iobuf snap_data = serde::to_iobuf(snapshot{
      .segments = std::move(segments),
      .replaced = std::move(replaced),
      .start_offset = _manifest->get_start_offset().value_or(model::offset()),
      .last_offset = _manifest->get_last_offset(),
      .last_uploaded_compacted_offset
      = _manifest->get_last_uploaded_compacted_offset()});

    vlog(
      _logger.debug,
      "creating snapshot at offset: {}, remote start_offset: {}, "
      "last_offset: "
      "{}",
      _insync_offset,
      get_start_offset(),
      get_last_offset());
    co_return stm_snapshot::create(0, _insync_offset, std::move(snap_data));
}

model::offset archival_metadata_stm::max_collectible_offset() {
    // From Redpanda 22.3 up, the ntp_config's impression of whether archival
    // is enabled is authoritative.
    bool collect_all = !_raft->log_config().is_archival_enabled();
    bool is_read_replica = _raft->log_config().is_read_replica_mode_enabled();

    // In earlier versions, we should assume every topic is archival enabled
    // if the global cloud_storage_enable_remote_write is true.
    if (
      !_feature_table.is_active(features::feature::cloud_retention)
      && config::shard_local_cfg().cloud_storage_enable_remote_write()) {
        collect_all = false;
    }

    if (collect_all || is_read_replica) {
        // The archival is disabled but the state machine still exists so we
        // shouldn't stop eviction from happening.
        // In read-replicas the state machine exists and stores segments from
        // the remote manifest. Since nothing is uploaded there is no need to
        // interact with local retention.
        return model::offset::max();
    }
    auto lo = get_last_offset();
    if (_manifest->size() == 0 && lo == model::offset{0}) {
        lo = model::offset::min();
    }
    return lo;
}

void archival_metadata_stm::apply_add_segment(const segment& segment) {
    auto meta = segment.meta;
    if (meta.ntp_revision == model::initial_revision_id{}) {
        // metadata serialized by old versions of redpanda doesn't have the
        // ntp_revision field.
        meta.ntp_revision = segment.ntp_revision_deprecated;
    }
    _manifest->add(segment.name, meta);

    if (meta.committed_offset > get_last_offset()) {
        if (meta.base_offset > model::next_offset(get_last_offset())) {
            // To ensure forward progress, we print a warning and skip over
            // the hole.

            vlog(
              _logger.warn,
              "hole in the remote offset range detected! previous last "
              "offset: "
              "{}, new segment base offset: {}",
              get_last_offset(),
              meta.base_offset);
        }
    }
}

void archival_metadata_stm::apply_truncate(const start_offset& so) {
    auto removed = _manifest->truncate(so.start_offset);
    vlog(
      _logger.debug,
      "Truncate command applied, new start offset: {}, new last offset: {}",
      get_start_offset(),
      get_last_offset());
}

void archival_metadata_stm::apply_cleanup_metadata() {
    auto backlog = get_segments_to_cleanup();
    if (backlog.empty()) {
        return;
    }
    _manifest->delete_replaced_segments();
    _manifest->truncate();
}

void archival_metadata_stm::apply_update_start_offset(const start_offset& so) {
    vlog(
      _logger.debug,
      "Updating start offset, current value {}, update {}",
      get_start_offset(),
      so.start_offset);
    if (!_manifest->advance_start_offset(so.start_offset)) {
        vlog(
          _logger.error,
          "Can't truncate manifest up to offset {}, offset out of range",
          so.start_offset);
    } else {
        vlog(_logger.debug, "Start offset updated to {}", get_start_offset());
    }
}

void archival_metadata_stm::apply_reset_metadata() {
    vlog(_logger.info, "Resetting manifest");
    _manifest->unsafe_reset();
}

std::vector<cloud_storage::partition_manifest::lw_segment_meta>
archival_metadata_stm::get_segments_to_cleanup() const {
    // Include replaced segments to the backlog
    using lw_segment_meta = cloud_storage::partition_manifest::lw_segment_meta;
    std::vector<lw_segment_meta> backlog = _manifest->lw_replaced_segments();

    // Make sure that 'replaced' list doesn't have any references to active
    // segments. This is a protection from the data loss. This should not
    // happen, but protects us from data loss in cases where bugs elsewhere.
    auto backlog_size = backlog.size();
    backlog.erase(
      std::remove_if(
        backlog.begin(),
        backlog.end(),
        [this](const lw_segment_meta& m) {
            auto it = _manifest->find(m.base_offset);
            if (it == _manifest->end()) {
                return false;
            }
            const auto& s = it->second;
            auto m_name = _manifest->generate_remote_segment_name(
              cloud_storage::partition_manifest::lw_segment_meta::convert(m));
            auto s_name = _manifest->generate_remote_segment_name(s);
            // The segment will have the same path as the one we have in
            // manifest in S3 so if we will delete it the data will be lost.
            if (m_name == s_name) {
                vlog(
                  _logger.warn,
                  "The replaced segment name {} collides with the segment {} "
                  "in the manifest. It will be removed to prevent the data "
                  "loss.",
                  m_name,
                  s_name);
                return true;
            }
            return false;
        }),
      backlog.end());

    if (backlog.size() < backlog_size) {
        vlog(
          _logger.warn,
          "{} segments will not be removed from the bucket because they're "
          "available in the manifest",
          backlog_size - backlog.size());
    }

    auto so = _manifest->get_start_offset().value_or(model::offset(0));
    for (const auto& m : *_manifest) {
        if (m.second.committed_offset < so) {
            backlog.push_back(lw_segment_meta::convert(m.second));
        } else {
            break;
        }
    }
    return backlog;
}

ss::future<> archival_metadata_stm::stop() {
    _download_as.request_abort();
    co_await raft::state_machine::stop();
}

const cloud_storage::partition_manifest&
archival_metadata_stm::manifest() const {
    return *_manifest;
}

model::offset archival_metadata_stm::get_start_offset() const {
    auto p = _manifest->get_start_offset();
    if (p.has_value()) {
        return p.value();
    }
    return {};
}

model::offset archival_metadata_stm::get_last_offset() const {
    return _manifest->get_last_offset();
}

} // namespace cluster
