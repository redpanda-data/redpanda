// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/scoped_file_tracker.h"

#include <absl/container/btree_set.h>
#include <gtest/gtest.h>

#include <filesystem>

using namespace storage;

TEST(ScopedFileTrackerTest, TestBasic) {
    scoped_file_tracker::set_t tracked;

    // Clearing the tracker will leave nothing tracked.
    {
        scoped_file_tracker t(&tracked, {"foo"});
        t.clear();
    }
    ASSERT_EQ(0, tracked.size());

    // If we leave scope without clearing, we leave behind some files.
    { scoped_file_tracker t(&tracked, {"foo"}); }
    ASSERT_EQ(1, tracked.size());

    // Even if we clear from a new tracker, we don't affect the already tracked
    // files.
    {
        scoped_file_tracker t(&tracked, {"foo"});
        t.clear();
    }
    ASSERT_EQ(1, tracked.size());
}

TEST(ScopedFileTrackerTest, TestMoveConstruct) {
    scoped_file_tracker::set_t tracked;
    {
        scoped_file_tracker t(&tracked, {"foo"});
        {
            // Move the tracker and clear it.
            scoped_file_tracker t2(std::move(t));
            t2.clear();
        }
        ASSERT_EQ(0, tracked.size());
    }
    ASSERT_EQ(0, tracked.size());
    {
        scoped_file_tracker t(&tracked, {"foo"});
        {
            // This time don't clear it. This will leave behind some files.
            scoped_file_tracker t2(std::move(t));
        }
        ASSERT_EQ(1, tracked.size());
    }
    ASSERT_EQ(1, tracked.size());
}

TEST(ScopedFileTrackerTest, TestOptional) {
    scoped_file_tracker::set_t tracked;
    std::optional<scoped_file_tracker> cleanup;
    {
        // Move construct and clear a tracker. Note that this creates a
        // temporary.
        cleanup.emplace(scoped_file_tracker{&tracked, {"foo"}});
        cleanup->clear();
    }
    // Upon destructing the tracker, this should leave nothing behind.
    ASSERT_EQ(0, tracked.size());
    cleanup.reset();
    ASSERT_EQ(0, tracked.size());

    {
        // Now exit without calling clear().
        cleanup.emplace(scoped_file_tracker{&tracked, {"foo"}});
    }
    // We haven't destructed yet, so nothing should be tracked.
    ASSERT_EQ(0, tracked.size());

    // But once we do, we should see a file.
    cleanup.reset();
    ASSERT_EQ(1, tracked.size());
}
