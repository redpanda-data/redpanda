/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "compat/generator.h"
#include "model/tests/random_batch.h"
#include "raft/types.h"
#include "random/generators.h"
#include "test_utils/randoms.h"

namespace compat {

template<>
struct instance_generator<raft::errc> {
    static raft::errc random() {
        return random_generators::random_choice(
          {raft::errc::success,
           raft::errc::disconnected_endpoint,
           raft::errc::exponential_backoff,
           raft::errc::non_majority_replication,
           raft::errc::not_leader,
           raft::errc::vote_dispatch_error,
           raft::errc::append_entries_dispatch_error,
           raft::errc::replicated_entry_truncated,
           raft::errc::leader_flush_failed,
           raft::errc::leader_append_failed,
           raft::errc::timeout,
           raft::errc::configuration_change_in_progress,
           raft::errc::node_does_not_exists,
           raft::errc::leadership_transfer_in_progress,
           raft::errc::transfer_to_current_leader,
           raft::errc::node_already_exists,
           raft::errc::invalid_configuration_update,
           raft::errc::not_voter,
           raft::errc::invalid_target_node,
           raft::errc::shutting_down,
           raft::errc::replicate_batcher_cache_error,
           raft::errc::group_not_exists,
           raft::errc::replicate_first_stage_exception});
    }

    static std::vector<raft::errc> limits() { return {}; }
};

template<>
struct instance_generator<raft::vnode> {
    static raft::vnode random() {
        return {
          tests::random_named_int<model::node_id>(),
          tests::random_named_int<model::revision_id>()};
    }

    static std::vector<raft::vnode> limits() {
        return {
          {model::node_id::min(), model::revision_id::min()},
          {model::node_id::max(), model::revision_id::max()},
          {model::node_id{0}, model::revision_id{0}},
        };
    }
};

template<>
struct instance_generator<raft::timeout_now_request> {
    static raft::timeout_now_request random() {
        return {
          .target_node_id = instance_generator<raft::vnode>::random(),
          .node_id = instance_generator<raft::vnode>::random(),
          .group = tests::random_named_int<raft::group_id>(),
          .term = tests::random_named_int<model::term_id>(),
        };
    }

    static std::vector<raft::timeout_now_request> limits() {
        return {
          {
            .target_node_id = instance_generator<raft::vnode>::random(),
            .node_id = instance_generator<raft::vnode>::random(),
            .group = raft::group_id::min(),
            .term = model::term_id::min(),
          },
          {
            .target_node_id = instance_generator<raft::vnode>::random(),
            .node_id = instance_generator<raft::vnode>::random(),
            .group = raft::group_id::max(),
            .term = model::term_id::max(),
          },
        };
    }
};

template<>
struct instance_generator<raft::timeout_now_reply> {
    static raft::timeout_now_reply random() {
        return {
          .target_node_id = instance_generator<raft::vnode>::random(),
          .term = tests::random_named_int<model::term_id>(),
          .result = random_generators::random_choice(
            {raft::timeout_now_reply::status::success,
             raft::timeout_now_reply::status::failure}),
        };
    }

    static std::vector<raft::timeout_now_reply> limits() {
        return {
          {
            .target_node_id = instance_generator<raft::vnode>::random(),
            .term = model::term_id::min(),
            .result = raft::timeout_now_reply::status::success,
          },
          {
            .target_node_id = instance_generator<raft::vnode>::random(),
            .term = model::term_id::max(),
            .result = raft::timeout_now_reply::status::failure,
          },
        };
    }
};

template<>
struct instance_generator<raft::transfer_leadership_request> {
    static raft::transfer_leadership_request random() {
        return {
          .group = tests::random_named_int<raft::group_id>(),
          .target = tests::random_named_int<model::node_id>(),
          .timeout = tests::random_duration_ms(),
        };
    }

    static std::vector<raft::transfer_leadership_request> limits() {
        return {
          {
            .group = tests::random_named_int<raft::group_id>(),
            .target = std::nullopt,
            .timeout = std::nullopt,
          },
          {
            .group = raft::group_id::min(),
            .target = std::nullopt,
            .timeout = std::nullopt,
          },
          {
            .group = raft::group_id::max(),
            .target = std::nullopt,
            .timeout = std::nullopt,
          },
          {
            .group = raft::group_id::min(),
            .target = tests::random_named_int<model::node_id>(),
            .timeout = tests::random_duration_ms(),
          },
          {
            .group = raft::group_id::max(),
            .target = tests::random_named_int<model::node_id>(),
            .timeout = tests::random_duration_ms(),
          },
        };
    }
};

template<>
struct instance_generator<raft::transfer_leadership_reply> {
    static raft::transfer_leadership_reply random() {
        return {
          .success = tests::random_bool(),
          .result = instance_generator<raft::errc>::random(),
        };
    }

    static std::vector<raft::transfer_leadership_reply> limits() { return {}; }
};

template<>
struct instance_generator<raft::install_snapshot_request> {
    static raft::install_snapshot_request random() {
        return {
          .target_node_id = instance_generator<raft::vnode>::random(),
          .term = tests::random_named_int<model::term_id>(),
          .group = tests::random_named_int<raft::group_id>(),
          .node_id = instance_generator<raft::vnode>::random(),
          .last_included_index = tests::random_named_int<model::offset>(),
          .file_offset = random_generators::get_int<uint64_t>(),
          .chunk = bytes_to_iobuf(
            random_generators::get_bytes(random_generators::get_int(1, 512))),
          .done = tests::random_bool(),
        };
    }

    static std::vector<raft::install_snapshot_request> limits() { return {}; }
};

template<>
struct instance_generator<raft::install_snapshot_reply> {
    static raft::install_snapshot_reply random() {
        return {
          .target_node_id = instance_generator<raft::vnode>::random(),
          .term = tests::random_named_int<model::term_id>(),
          .bytes_stored = random_generators::get_int<uint64_t>(),
          .success = tests::random_bool(),
        };
    }

    static std::vector<raft::install_snapshot_reply> limits() { return {}; }
};

template<>
struct instance_generator<raft::vote_request> {
    static raft::vote_request random() {
        return {
          .node_id = instance_generator<raft::vnode>::random(),
          .target_node_id = instance_generator<raft::vnode>::random(),
          .group = tests::random_named_int<raft::group_id>(),
          .term = tests::random_named_int<model::term_id>(),
          .prev_log_index = tests::random_named_int<model::offset>(),
          .prev_log_term = tests::random_named_int<model::term_id>(),
          .leadership_transfer = tests::random_bool(),
        };
    }

    static std::vector<raft::vote_request> limits() { return {}; }
};

template<>
struct instance_generator<raft::vote_reply> {
    static raft::vote_reply random() {
        return {
          .target_node_id = instance_generator<raft::vnode>::random(),
          .term = tests::random_named_int<model::term_id>(),
          .granted = tests::random_bool(),
          .log_ok = tests::random_bool(),
        };
    }

    static std::vector<raft::vote_reply> limits() { return {}; }
};

template<>
struct instance_generator<raft::protocol_metadata> {
    static raft::protocol_metadata random() {
        return {
          .group = tests::random_named_int<raft::group_id>(),
          .commit_index = tests::random_named_int<model::offset>(),
          .term = tests::random_named_int<model::term_id>(),
          .prev_log_index = tests::random_named_int<model::offset>(),
          .prev_log_term = tests::random_named_int<model::term_id>(),
          .last_visible_index = tests::random_named_int<model::offset>(),
        };
    }

    static std::vector<raft::vote_reply> limits() { return {}; }
};

template<>
struct instance_generator<raft::heartbeat_request> {
    static raft::heartbeat_request random() {
        /*
         * heartbeat_request encoding assumes that the first node/target_node in
         * the heartbeat set have the same node_id (revisions can be different).
         * that needs to be true here in the generated data otherwise we won't
         * compare equal at after deserialization.
         */
        const auto node_id = tests::random_named_int<model::node_id>();
        const auto target_node_id = tests::random_named_int<model::node_id>();

        auto ret = raft::heartbeat_request{{
          {
            .meta = instance_generator<raft::protocol_metadata>::random(),
            .node_id = raft::
              vnode{node_id, tests::random_named_int<model::revision_id>()},
            .target_node_id = raft::
              vnode{target_node_id, tests::random_named_int<model::revision_id>()},
          },
          {
            .meta = instance_generator<raft::protocol_metadata>::random(),
            .node_id = raft::
              vnode{node_id, tests::random_named_int<model::revision_id>()},
            .target_node_id = raft::
              vnode{target_node_id, tests::random_named_int<model::revision_id>()},
          },
        }};

        /*
         * the serialization step for heartbeat_request will automatically sort
         * the heartbeats. so for equality to work as expected we need to also
         * sort the same way so that when we serialize to json we retain that
         * ordering that will be present after the deserialization step.
         */
        struct sorter_fn {
            constexpr bool operator()(
              const raft::heartbeat_metadata& lhs,
              const raft::heartbeat_metadata& rhs) const {
                return lhs.meta.commit_index < rhs.meta.commit_index;
            }
        };

        std::sort(ret.heartbeats.begin(), ret.heartbeats.end(), sorter_fn{});

        return ret;
    }

    static std::vector<raft::heartbeat_request> limits() { return {}; }
};

template<>
struct instance_generator<raft::append_entries_request> {
    static raft::append_entries_request random() {
        return raft::append_entries_request{
          instance_generator<raft::vnode>::random(),
          instance_generator<raft::vnode>::random(),
          instance_generator<raft::protocol_metadata>::random(),
          model::make_memory_record_batch_reader(
            model::test::make_random_batches(model::offset(0), 3, false)),
          raft::append_entries_request::flush_after_append(
            tests::random_bool()),
        };
    }

    static std::vector<raft::append_entries_request> limits() { return {}; }
};

template<>
struct instance_generator<raft::append_entries_reply> {
    static raft::append_entries_reply random() {
        return {
          .target_node_id = instance_generator<raft::vnode>::random(),
          .node_id = instance_generator<raft::vnode>::random(),
          .group = tests::random_named_int<raft::group_id>(),
          .term = tests::random_named_int<model::term_id>(),
          .last_flushed_log_index = tests::random_named_int<model::offset>(),
          .last_dirty_log_index = tests::random_named_int<model::offset>(),
          .last_term_base_offset = tests::random_named_int<model::offset>(),
          .result = random_generators::random_choice(
            {raft::append_entries_reply::status::success,
             raft::append_entries_reply::status::failure,
             raft::append_entries_reply::status::group_unavailable,
             raft::append_entries_reply::status::timeout}),
        };
    }

    static std::vector<raft::append_entries_reply> limits() { return {}; }
};

template<>
struct instance_generator<raft::heartbeat_reply> {
    static raft::heartbeat_reply random() {
        auto ret = raft::heartbeat_reply{{
          instance_generator<raft::append_entries_reply>::random(),
          instance_generator<raft::append_entries_reply>::random(),
        }};

        /*
         * override node_id and target_node_id heartbeat_reply encoding assumes
         * that the first node/target_node in the heartbeat set have the same
         * node_id (revisions can be different).  that needs to be true here in
         * the generated data otherwise we won't compare equal at after
         * deserialization.
         */
        const auto node_id = tests::random_named_int<model::node_id>();
        const auto target_node_id = tests::random_named_int<model::node_id>();

        for (auto& r : ret.meta) {
            r.target_node_id = raft::vnode{
              target_node_id, r.target_node_id.revision()};
            r.node_id = raft::vnode{node_id, r.target_node_id.revision()};
        }

        /*
         * encoder will sort before serializing, so for equality test to work
         * after deserializing we sort here first so that the json value we save
         * has the same ordering.
         */
        struct sorter_fn {
            constexpr bool operator()(
              const raft::append_entries_reply& lhs,
              const raft::append_entries_reply& rhs) const {
                return lhs.last_flushed_log_index < rhs.last_flushed_log_index;
            }
        };

        std::sort(ret.meta.begin(), ret.meta.end(), sorter_fn{});

        return ret;
    }

    static std::vector<raft::heartbeat_reply> limits() { return {}; }
};

} // namespace compat
