// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found at https://github.com/google/leveldb/blob/main/LICENSE. See
// https://github.com/google/leveldb/blob/main/AUTHORS for names of
// contributors.
//
// Modifications copyright 2025 Redpanda Data, Inc.

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "lsm/core/internal/keys.h"
#include "lsm/db/snapshot.h"

#include <memory>
#include <optional>

using namespace lsm::db;
using lsm::internal::operator""_seqno;

TEST(SnapshotListTest, EmptyByDefault) {
    snapshot_list list;
    EXPECT_TRUE(list.empty());
    EXPECT_EQ(list.newest_seqno(), std::nullopt);
    EXPECT_EQ(list.oldest_seqno(), std::nullopt);
}

TEST(SnapshotListTest, CreateSingleSnapshot) {
    snapshot_list list;
    auto snap = list.create(100_seqno);
    ASSERT_NE(snap, nullptr);
    EXPECT_FALSE(list.empty());
    EXPECT_EQ(snap->seqno(), 100_seqno);
    EXPECT_EQ(list.newest_seqno(), 100_seqno);
    EXPECT_EQ(list.oldest_seqno(), 100_seqno);
}

TEST(SnapshotListTest, CreateMultipleSnapshotsInOrder) {
    snapshot_list list;
    auto snap1 = list.create(100_seqno);
    auto snap2 = list.create(200_seqno);
    auto snap3 = list.create(300_seqno);

    EXPECT_FALSE(list.empty());
    EXPECT_EQ(snap1->seqno(), 100_seqno);
    EXPECT_EQ(snap2->seqno(), 200_seqno);
    EXPECT_EQ(snap3->seqno(), 300_seqno);

    // Newest should be the most recently created
    EXPECT_EQ(list.newest_seqno(), 300_seqno);
    // Oldest should be the first created
    EXPECT_EQ(list.oldest_seqno(), 100_seqno);
}

TEST(SnapshotListTest, DestroyingSnapshotRemovesItFromList) {
    snapshot_list list;
    auto snap1 = list.create(100_seqno);
    auto snap2 = list.create(200_seqno);
    auto snap3 = list.create(300_seqno);

    EXPECT_EQ(list.newest_seqno(), 300_seqno);
    EXPECT_EQ(list.oldest_seqno(), 100_seqno);

    // Destroy the middle snapshot
    snap2.reset();
    EXPECT_EQ(list.newest_seqno(), 300_seqno);
    EXPECT_EQ(list.oldest_seqno(), 100_seqno);

    // Destroy the oldest
    snap1.reset();
    EXPECT_EQ(list.newest_seqno(), 300_seqno);
    EXPECT_EQ(list.oldest_seqno(), 300_seqno);

    // Destroy the last one
    snap3.reset();
    EXPECT_TRUE(list.empty());
    EXPECT_EQ(list.newest_seqno(), std::nullopt);
    EXPECT_EQ(list.oldest_seqno(), std::nullopt);
}

TEST(SnapshotListTest, DestroyingNewestSnapshot) {
    snapshot_list list;
    auto snap1 = list.create(100_seqno);
    auto snap2 = list.create(200_seqno);
    auto snap3 = list.create(300_seqno);

    // Destroy newest
    snap3.reset();
    EXPECT_EQ(list.newest_seqno(), 200_seqno);
    EXPECT_EQ(list.oldest_seqno(), 100_seqno);
}

TEST(SnapshotListTest, DestroyingOldestSnapshot) {
    snapshot_list list;
    auto snap1 = list.create(100_seqno);
    auto snap2 = list.create(200_seqno);
    auto snap3 = list.create(300_seqno);

    // Destroy oldest
    snap1.reset();
    EXPECT_EQ(list.newest_seqno(), 300_seqno);
    EXPECT_EQ(list.oldest_seqno(), 200_seqno);
}

TEST(SnapshotListTest, CanCreateSnapshotWithSameSeqno) {
    snapshot_list list;
    auto snap1 = list.create(100_seqno);
    auto snap2 = list.create(100_seqno);

    EXPECT_EQ(snap1->seqno(), 100_seqno);
    EXPECT_EQ(snap2->seqno(), 100_seqno);
    EXPECT_EQ(list.newest_seqno(), 100_seqno);
    EXPECT_EQ(list.oldest_seqno(), 100_seqno);
}

TEST(SnapshotListTest, MultipleSnapshotsWithInterleavedDestruction) {
    snapshot_list list;
    auto snap1 = list.create(100_seqno);
    auto snap2 = list.create(200_seqno);
    auto snap3 = list.create(300_seqno);
    auto snap4 = list.create(400_seqno);
    auto snap5 = list.create(500_seqno);

    EXPECT_EQ(list.newest_seqno(), 500_seqno);
    EXPECT_EQ(list.oldest_seqno(), 100_seqno);

    // Destroy in non-sequential order
    snap3.reset();
    EXPECT_EQ(list.newest_seqno(), 500_seqno);
    EXPECT_EQ(list.oldest_seqno(), 100_seqno);

    snap1.reset();
    EXPECT_EQ(list.newest_seqno(), 500_seqno);
    EXPECT_EQ(list.oldest_seqno(), 200_seqno);

    snap5.reset();
    EXPECT_EQ(list.newest_seqno(), 400_seqno);
    EXPECT_EQ(list.oldest_seqno(), 200_seqno);

    snap2.reset();
    EXPECT_EQ(list.newest_seqno(), 400_seqno);
    EXPECT_EQ(list.oldest_seqno(), 400_seqno);

    snap4.reset();
    EXPECT_TRUE(list.empty());
}
