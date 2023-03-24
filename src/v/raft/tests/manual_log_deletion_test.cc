// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/configuration.h"
#include "finjector/hbadger.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record.h"
#include "model/timestamp.h"
#include "raft/consensus_utils.h"
#include "raft/tests/raft_group_fixture.h"
#include "raft/types.h"
#include "random/generators.h"
#include "storage/record_batch_builder.h"
#include "storage/tests/utils/disk_log_builder.h"
#include "test_utils/async.h"

#include <seastar/core/abort_source.hh>

#include <filesystem>
#include <optional>
#include <system_error>
#include <vector>

struct manual_deletion_fixture : public raft_test_fixture {
    manual_deletion_fixture()
      : gr(
        raft::group_id(0), 3, model::cleanup_policy_bitflags::deletion, 1_KiB) {
        config::shard_local_cfg().log_segment_size_min.set_value(
          std::optional<uint64_t>());
        gr.enable_all();
    }

    void prepare_raft_group() {
        wait_for_group_leader(gr);
        ss::abort_source as;

        auto first_ts = model::timestamp::now();
        // append some entries
        [[maybe_unused]] bool res
          = replicate_compactible_batches(gr, first_ts).get0();
        auto second_ts = model::timestamp(first_ts() + 200000);
        // append some more entries
        res = replicate_compactible_batches(gr, second_ts).get0();
        retention_timestamp = first_ts;
        validate_logs_replication(gr);
    }

    void apply_retention_policy() {
        wait_for(
          2s,
          [this] {
              for (auto& [_, n] : gr.get_members()) {
                  n.log
                    ->compact(storage::compaction_config(
                      retention_timestamp,
                      100_MiB,
                      model::offset::max(),
                      ss::default_priority_class(),
                      as,
                      storage::debug_sanitize_files::yes))
                    .get0();
                  if (n.log->offsets().start_offset <= model::offset(0)) {
                      return false;
                  }
              }
              return true;
          },
          "logs has prefix truncated");
    }

    void remove_data(std::vector<model::node_id> nodes) {
        std::vector<std::filesystem::path> to_delete;
        to_delete.reserve(nodes.size());

        // disable and remove data
        for (auto id : nodes) {
            to_delete.push_back(std::filesystem::path(
              gr.get_member(id).log->config().topic_directory()));
            gr.disable_node(id);
        }
        for (auto& path : to_delete) {
            std::filesystem::remove_all(path);
        }
        // enable back
        for (auto id : nodes) {
            gr.enable_node(id);
        }
    }

    void remove_all_data() {
        std::vector<model::node_id> nodes;
        for (auto& [id, _] : gr.get_members()) {
            nodes.push_back(id);
        }
        remove_data(nodes);
    }

    raft_group gr;
    model::timestamp retention_timestamp;
    ss::abort_source as;
};

FIXTURE_TEST(
  test_collected_log_recovery_admin_deletion_all, manual_deletion_fixture) {
    prepare_raft_group();
    // compact logs
    apply_retention_policy();

    // simulate admin deleting log folders. For more details look here:
    //
    // https://github.com/redpanda-data/redpanda/issues/321

    remove_all_data();

    validate_logs_replication(gr);

    wait_for(
      10s,
      [this] { return are_all_commit_indexes_the_same(gr); },
      "After recovery state is consistent");
};

FIXTURE_TEST(
  test_collected_log_recovery_admin_deletion_one, manual_deletion_fixture) {
    prepare_raft_group();
    // compact logs
    apply_retention_policy();

    // simulate admin deleting log folders. For more details look here:
    //
    // https://github.com/redpanda-data/redpanda/issues/321

    remove_data({model::node_id(1)});

    validate_logs_replication(gr);

    wait_for(
      10s,
      [this] { return are_all_commit_indexes_the_same(gr); },
      "After recovery state is consistent");
};
