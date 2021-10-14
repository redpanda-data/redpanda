// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/archival_metadata_stm.h"

#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_types.h"
#include "model/record_utils.h"
#include "raft/consensus.h"
#include "serde/envelope.h"
#include "serde/serde.h"
#include "storage/record_batch_builder.h"
#include "utils/named_type.h"
#include "vlog.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/io_priority_class.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <seastar/util/log.hh>

#include <cstdint>
#include <memory>
#include <type_traits>

namespace cluster {

static_assert(std::is_aggregate_v<archival_metadata_stm::segment>);

namespace {

static ss::logger logger{"archival_metadata_stm"};

using cmd_key = named_type<uint8_t, struct cmd_key_tag>;

struct add_segment_cmd {
    static constexpr cmd_key key{0};

    using value = archival_metadata_stm::segment;
};

struct snapshot : public serde::envelope<snapshot, serde::version<0>> {
    std::vector<archival_metadata_stm::segment> segments;
};

} // namespace

archival_metadata_stm::archival_metadata_stm(raft::consensus* raft)
  : cluster::persisted_stm("archival_metadata.snapshot", logger, raft)
  , _logger(logger, ssx::sformat("ntp: {}", raft->ntp()))
  , _manifest(raft->ntp(), raft->config().revision_id()) {}

ss::future<bool>
archival_metadata_stm::add_segments(const std::vector<segment>& segments) {
    return _lock.with([this, &segments] { return do_add_segments(segments); });
}

ss::future<bool>
archival_metadata_stm::do_add_segments(const std::vector<segment>& segments) {
    if (!co_await sync(model::max_duration)) {
        co_return false;
    }

    storage::record_batch_builder b(
      model::record_batch_type::archival_metadata, model::offset(0));

    for (const auto& segment : segments) {
        iobuf key_buf = serde::to_iobuf(add_segment_cmd::key);
        auto record_val = add_segment_cmd::value{segment};
        iobuf val_buf = serde::to_iobuf(std::move(record_val));
        b.add_raw_kv(std::move(key_buf), std::move(val_buf));
    }

    auto batch = std::move(b).build();
    auto result = co_await _raft->replicate(
      _insync_term,
      model::make_memory_record_batch_reader(std::move(batch)),
      raft::replicate_options{raft::consistency_level::quorum_ack});

    if (!result) {
        co_return false;
    }

    co_return co_await wait_no_throw(
      result.value().last_offset, model::max_duration);
}

ss::future<> archival_metadata_stm::apply(model::record_batch b) {
    if (b.header().type != model::record_batch_type::archival_metadata) {
        _insync_offset = b.last_offset();
        co_return;
    }

    b.for_each_record([this](model::record&& r) {
        auto key = serde::from_iobuf<cmd_key>(r.release_key());
        if (key == add_segment_cmd::key) {
            auto value = serde::from_iobuf<add_segment_cmd::value>(
              r.release_value());
            apply_add_segment(value);
        }
    });

    _insync_offset = b.last_offset();
}

void archival_metadata_stm::apply_add_segment(const segment& segment) {
    if (segment.ntp_revision == _manifest.get_revision_id()) {
        _manifest.add(segment.name, segment.meta);
    } else {
        auto path = cloud_storage::manifest::generate_remote_segment_path(
          _raft->ntp(), segment.ntp_revision, segment.name);
        _manifest.add(path, segment.meta);
    }

    // NOTE: here we don't take into account possibility of holes in the
    // remote offset range. Archival tries to upload segments in order, and
    // if for some reason is a hole, there are no mechanisms for correcting it.

    const cloud_storage::manifest::segment_meta& meta = segment.meta;

    if (_start_offset == model::offset{} || meta.base_offset < _start_offset) {
        _start_offset = meta.base_offset;
    }

    if (meta.committed_offset > _last_offset) {
        if (meta.base_offset > raft::details::next_offset(_last_offset)) {
            // To ensure forward progress, we print a warning and skip over the
            // hole.

            vlog(
              _logger.warn,
              "hole in the remote offset range detected! previous last offset: "
              "{}, new segment base offset: {}",
              _last_offset,
              meta.base_offset);
        }

        _last_offset = meta.committed_offset;
        if (_log_eviction_stm) {
            _log_eviction_stm->set_collectible_offset(_last_offset);
        }
    }

    vlog(
      _logger.info,
      "new remote segment (name: {}, base_offset: {} last_offset: {}), "
      "remote start_offset: {}, last_offset: {}",
      segment.name,
      meta.base_offset,
      meta.committed_offset,
      start_offset(),
      _last_offset);
}

ss::future<> archival_metadata_stm::apply_snapshot(
  stm_snapshot_header header, iobuf&& data) {
    vlog(_logger.info, "applying snapshot at offset: {}", header.offset);

    auto snap = serde::from_iobuf<snapshot>(std::move(data));

    for (const auto& segment : snap.segments) {
        apply_add_segment(segment);
    }

    _last_snapshot_offset = header.offset;
    _insync_offset = header.offset;
    co_return;
}

ss::future<stm_snapshot> archival_metadata_stm::take_snapshot() {
    std::vector<segment> segments;
    segments.reserve(_manifest.size());
    for (const auto& [key, meta] : _manifest) {
        model::revision_id ntp_revision;
        cloud_storage::segment_name name;

        if (std::holds_alternative<cloud_storage::remote_segment_path>(key)) {
            const auto& path = std::get<cloud_storage::remote_segment_path>(
              key);
            auto components = get_segment_path_components(path);
            vassert(components, "can't parse remote segment path {}", path);
            ntp_revision = components->_rev;
            name = components->_name;
        } else {
            ntp_revision = _raft->config().revision_id();
            name = std::get<cloud_storage::segment_name>(key);
        }

        segments.push_back(
          segment{.ntp_revision = ntp_revision, .name = name, .meta = meta});
    }

    iobuf snap_data = serde::to_iobuf(
      snapshot{.segments = std::move(segments)});

    vlog(_logger.trace, "created snapshot at offset: {}", _insync_offset);
    co_return stm_snapshot::create(0, _insync_offset, std::move(snap_data));
}

} // namespace cluster
