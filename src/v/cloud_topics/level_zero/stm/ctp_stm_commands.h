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

#include "cloud_topics/level_zero/stm/ctp_stm_state.h"
#include "cloud_topics/level_zero/stm/types.h"
#include "model/fundamental.h"
#include "serde/envelope.h"

#include <utility>

namespace cloud_topics {

using cmd_key = named_type<uint8_t, struct cmd_key_tag>;

/// This command advances LRO (Last Reconciled Offset) in the ctp_stm.
/// The command is replicated by the reconciler and is used to
/// notify the ctp_stm about the new LRO. This is needed to ensure that
/// the max_collectible_offset could be advanced and the local retention
/// could be applied. It's also used in computation of the min epoch.
struct advance_reconciled_offset_cmd
  : public serde::envelope<
      advance_reconciled_offset_cmd,
      serde::version<0>,
      serde::compat_version<0>> {
    static constexpr cmd_key key = cmd_key(
      std::to_underlying(ctp_stm_key::advance_reconciled_offset));

    advance_reconciled_offset_cmd() noexcept = default;

    explicit advance_reconciled_offset_cmd(
      kafka::offset lro, model::offset lrlo) noexcept
      : last_reconciled_offset(lro)
      , last_reconciled_log_offset(lrlo) {}

    auto serde_fields() {
        return std::tie(last_reconciled_offset, last_reconciled_log_offset);
    }

    kafka::offset last_reconciled_offset;
    model::offset last_reconciled_log_offset;
};

// This command sets the partition's start offset in the ctp_stm.
//
// The command is replicated by the housekeeper and is used to set the start
// offset in list offset requests to limit the offsets that can be fetched. This
// value is asynchronously pushed to L1 via the reconciler.
struct set_start_offset_cmd
  : public serde::envelope<
      set_start_offset_cmd,
      serde::version<0>,
      serde::compat_version<0>> {
    static constexpr cmd_key key = cmd_key(
      std::to_underlying(ctp_stm_key::set_start_offset));

    set_start_offset_cmd() noexcept = default;

    explicit set_start_offset_cmd(kafka::offset start_offset) noexcept
      : new_start_offset(start_offset) {}

    auto serde_fields() { return std::tie(new_start_offset); }

    kafka::offset new_start_offset;
};

struct advance_epoch_cmd
  : public serde::
      envelope<advance_epoch_cmd, serde::version<0>, serde::compat_version<0>> {
    static constexpr cmd_key key = cmd_key(
      std::to_underlying(ctp_stm_key::advance_epoch));

    advance_epoch_cmd() noexcept = default;

    explicit advance_epoch_cmd(cluster_epoch epoch)
      : new_epoch(epoch) {}

    auto serde_fields() { return std::tie(new_epoch); }

    cluster_epoch new_epoch;
};

/// This command resets the ctp_stm state to a serialized snapshot.
///
/// When applied, the STM replaces its entire in-memory state with the
/// one embedded in this command. This is used as an escape hatch to
/// recover from state corruption or to force a known-good state onto
/// the partition without truncating the log.
struct reset_state_cmd
  : public serde::
      envelope<reset_state_cmd, serde::version<0>, serde::compat_version<0>> {
    static constexpr cmd_key key = cmd_key(
      std::to_underlying(ctp_stm_key::reset_state));

    reset_state_cmd() noexcept = default;

    explicit reset_state_cmd(ctp_stm_state new_state) noexcept
      : state(std::move(new_state)) {}

    auto serde_fields() { return std::tie(state); }

    ctp_stm_state state;
};

} // namespace cloud_topics
