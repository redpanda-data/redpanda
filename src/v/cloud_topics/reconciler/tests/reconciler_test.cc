/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/level_one/common/fake_io.h"
#include "cloud_topics/level_one/metastore/simple_metastore.h"
#include "cloud_topics/reconciler/reconciler.h"
#include "cloud_topics/reconciler/reconciliation_source.h"
#include "gmock/gmock.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/tests/random_batch.h"
#include "model/tests/randoms.h"

#include <seastar/util/defer.hh>

#include <gtest/gtest.h>

#include <expected>
#include <utility>

using namespace cloud_topics;

namespace {

class fake_source : public reconciler::source {
public:
    fake_source(model::ntp ntp, model::topic_id_partition tidp)
      : reconciler::source(std::move(ntp), tidp) {}

    void add_batch(model::test::record_batch_spec spec) {
        if (!_source_log.empty()) {
            spec.offset = _source_log.back().last_offset() + model::offset{1};
        }
        auto batch = model::test::make_random_batch(spec);
        _source_log.push_back(std::move(batch));
    }

    kafka::offset last_reconciled_offset() override { return _lro; }

    ss::future<std::expected<void, errc>>
    set_last_reconciled_offset(kafka::offset o, ss::abort_source&) override {
        _lro = o;
        co_return std::expected<void, errc>{};
    }

    ss::future<model::record_batch_reader>
    make_reader(cloud_topic_log_reader_config cfg) override {
        chunked_vector<model::record_batch> log;
        size_t size = 0;
        for (const auto& batch : _source_log) {
            if (model::offset_cast(batch.base_offset()) < cfg.start_offset) {
                continue;
            }
            if (model::offset_cast(batch.last_offset()) > cfg.max_offset) {
                break;
            }
            size += batch.size_bytes();
            log.push_back(batch.copy());
            if (size > cfg.max_bytes) {
                break;
            }
        }
        co_return model::make_chunked_memory_record_batch_reader(
          std::move(log));
    }

private:
    kafka::offset _lro;
    chunked_vector<model::record_batch> _source_log;
};

class ReconcilerTest : public testing::Test {
public:
    ss::shared_ptr<fake_source> add_source() {
        auto ntp = model::random_ntp();
        auto tid = model::create_topic_id();
        auto src = ss::make_shared<fake_source>(
          ntp, model::topic_id_partition{tid, ntp.tp.partition});
        _reconciler.attach_source(src);
        return src;
    }

    void reconcile() { _reconciler.reconcile().get(); }

    std::optional<kafka::offset>
    metastore_next_offset(ss::shared_ptr<fake_source> src) {
        auto offsets = _metastore.get_offsets(src->topic_id_partition()).get();
        if (!offsets.has_value()) {
            return std::nullopt;
        }
        return offsets.value().next_offset;
    }

private:
    l1::fake_io _io;
    l1::simple_metastore _metastore;
    reconciler::reconciler _reconciler{&_io, &_metastore};
};

using ::testing::Optional;

} // namespace

TEST_F(ReconcilerTest, EmptySource) {
    auto src = add_source();
    reconcile();
    EXPECT_EQ(src->last_reconciled_offset(), kafka::offset{});
    EXPECT_EQ(metastore_next_offset(src), std::nullopt);
}

TEST_F(ReconcilerTest, SingleSource) {
    auto src = add_source();
    src->add_batch({.count = 10});
    src->add_batch({.count = 10});
    src->add_batch({.count = 10});
    reconcile();
    EXPECT_EQ(src->last_reconciled_offset(), kafka::offset{29});
    EXPECT_THAT(metastore_next_offset(src), Optional(kafka::offset{30}));
    src->add_batch({.count = 10});
    reconcile();
    EXPECT_EQ(src->last_reconciled_offset(), kafka::offset{39});
    EXPECT_THAT(metastore_next_offset(src), Optional(kafka::offset{40}));
}
