/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "kafka/client/utils.h"
#include "kafka/protocol/offset_commit.h"

#include <gtest/gtest.h>

TEST(utils_test, test_copying_offset_commit_request) {
    chunked_vector<kafka::offset_commit_request_topic> topics;
    absl::btree_map<kafka::tag_id, bytes> tags;
    tags.emplace(kafka::tag_id(1), bytes::from_string("unknown-tag-data"));

    for (int i = 0; i < 10; ++i) {
        topics.emplace_back(kafka::offset_commit_request_topic{
          .name = model::topic(ssx::sformat("topic-{}", i)),
          .partitions = {
            kafka::offset_commit_request_partition{
              .partition_index = model::partition_id(i),
              .committed_offset = model::offset(i),
              .committed_metadata = ssx::sformat("metadata-{}", i),
            },
          },    
          .unknown_tags = kafka::tagged_fields{tags},
        });
    }

    ASSERT_EQ(topics, kafka::client::make_copy(topics));
}
