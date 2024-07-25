/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_topics/dl_stm_cmd.h"
#include "cloud_topics/types.h"
#include "cluster/state_machine_registry.h"
#include "model/fundamental.h"
#include "model/timestamp.h"
#include "raft/persisted_stm.h"
#include "raft/state_machine_manager.h"
#include "serde/envelope.h"
#include "serde/rw/rw.h"
#include "serde/rw/uuid.h"
#include "ssx/semaphore.h"
#include "storage/record_batch_builder.h"
#include "utils/uuid.h"

#include <seastar/core/abort_source.hh>

#include <absl/container/btree_map.h>

#include <iterator>
#include <queue>
#include <stdexcept>

namespace cloud_topics {

namespace detail {

struct sorted_run_t
  : public serde::
      envelope<sorted_run_t, serde::version<0>, serde::compat_version<0>> {
    explicit sorted_run_t(const dl_overlay& o);

    sorted_run_t() = default;

    bool maybe_append(const dl_overlay& o);

    auto serde_fields() {
        return std::tie(values, base, last, ts_base, ts_last);
    }

    bool operator==(const sorted_run_t& other) const noexcept = default;

    std::deque<dl_overlay> values;
    kafka::offset base;
    kafka::offset last;
    model::timestamp ts_base;
    model::timestamp ts_last;
};

/// Container class for the dl_stm
/// which is supposed to store sorted list of overlays.
/// It should be easy to use columnar compression in the
/// future and also quickly find sorted runs. The container
/// uses patience sort algorithm to maintain order of overlay
/// values.
///
/// Deletion, replacement and truncation are not implemented yet.
class overlay_collection
  : public serde::envelope<
      overlay_collection,
      serde::version<0>,
      serde::compat_version<0>> {
    static constexpr size_t max_runs = 8;

public:
    bool operator==(const overlay_collection&) const noexcept = default;

    void append(const dl_overlay& o) noexcept;

    /// Find best overlay to fetch the offset from.
    ///
    /// The overlay should contain the offset 'o' or any
    /// offset larger than 'o'. If the caller needs exact
    /// match it has to validate the returned output.
    std::optional<dl_overlay> lower_bound(
      kafka::offset o) const noexcept; // TODO: add overload for the timequery

    auto serde_fields() { return std::tie(_runs); }

    void compact() noexcept;

    void do_append(const dl_overlay& o) noexcept;

private:
    std::deque<sorted_run_t> _runs;
};

/// This is an im-memory state of the dl_stm.
/// It supports all basic state transitions and serialization/deserialization
/// but it's not hooked up to the persisted_stm. There is an implementation of
/// the dl_stm which uses this dl_stm_state.
///
/// The dl_stm_state applies dl_overlay batches to its state and manages
/// the current view of the partition's data layout.
class dl_stm_state
  : public serde::
      envelope<dl_stm_state, serde::version<0>, serde::compat_version<0>> {
public:
    bool operator==(const dl_stm_state&) const noexcept = default;

    bool register_overlay_cmd(const dl_overlay& o) noexcept;

    std::optional<dl_overlay> lower_bound(kafka::offset o) const noexcept;

    std::optional<kafka::offset>
    get_term_last_offset(model::term_id t) const noexcept;

    auto serde_fields() {
        return std::tie(
          _overlays, _insync_offset, _last_reconciled_offset, _term_map);
    }

    model::offset get_insync_offset() const noexcept { return _insync_offset; }

    kafka::offset get_last_reconciled() const noexcept {
        return _last_reconciled_offset;
    }

    void propagate_last_reconciled_offset(
      kafka::offset base, kafka::offset last) noexcept;

private:
    detail::overlay_collection _overlays;
    model::offset _insync_offset;
    kafka::offset _last_reconciled_offset;
    // TODO: add _last_persisted_offset which is equal to _insync_offset of the
    // last uploaded snapshot.
    // TODO: add start offset/last offset caching
    absl::btree_map<model::term_id, kafka::offset> _term_map;
};

} // namespace detail

class dl_stm;

class dl_stm_command_builder {
public:
    explicit dl_stm_command_builder(dl_stm* parent, model::term_id term);

    void add_overlay_batch(
      object_id id,
      first_byte_offset_t offset,
      byte_range_size_t size_bytes,
      dl_stm_object_ownership ownership,
      kafka::offset base_offset,
      kafka::offset last_offset,
      model::timestamp base_ts,
      model::timestamp last_ts,
      absl::btree_map<model::term_id, kafka::offset> terms = {});

    // TODO: add fencing mechanism
    // for PoC assume that we're running one instance of the STM
    // per partition/broker and persisted_stm<>::sync() is called
    // after the leadership is acquired.

    ss::future<bool> replicate();

private:
    storage::record_batch_builder _builder;
    dl_stm* _parent;
    model::term_id _term;
};

class dl_stm final : public raft::persisted_stm<> {
public:
    static constexpr const char* name = "dl_stm";
    friend class dl_stm_command_builder;

    dl_stm(ss::logger&, raft::consensus*);

    ss::future<> stop() override;
    model::offset max_collectible_offset() override;

    /// Find overlays by offset
    std::optional<dl_overlay> lower_bound(kafka::offset o) const noexcept;
    std::optional<kafka::offset>
    get_term_last_offset(model::term_id t) const noexcept;
    kafka::offset get_last_reconciled() const noexcept;

    dl_stm_command_builder make_command_builder(model::term_id term);

private:
    ss::future<bool> replicate(model::term_id term, model::record_batch rb);

    /// Apply record batches
    ss::future<> do_apply(const model::record_batch& batch) override;

    ss::future<> apply_raft_snapshot(const iobuf&) override;

    ss::future<>
    apply_local_snapshot(raft::stm_snapshot_header, iobuf&&) override;
    ss::future<raft::stm_snapshot>
    take_local_snapshot(ssx::semaphore_units u) override;
    ss::future<iobuf> take_snapshot(model::offset) override;

    ss::gate _gate;
    ss::abort_source _as;
    detail::dl_stm_state _state;
};

class dl_stm_factory : public cluster::state_machine_factory {
public:
    dl_stm_factory() = default;

    bool is_applicable_for(const storage::ntp_config& ntp_cfg) const final;

    bool is_shadow_topic(const storage::ntp_config& ntp_cfg) const;

    void create(
      raft::state_machine_manager_builder& builder,
      raft::consensus* raft) final;
};

} // namespace cloud_topics
