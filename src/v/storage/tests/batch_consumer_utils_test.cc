// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/iostream.h"
#include "model/tests/random_batch.h"
#include "storage/batch_consumer_utils.h"
#include "storage/directories.h"
#include "storage/log_manager.h"
#include "storage/parser.h"
#include "storage/segment_reader.h"
#include "storage/tests/utils/disk_log_builder.h"
#include "test_utils/tmp_dir.h"

#include <seastar/core/io_priority_class.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>

#include <boost/test/tools/old/interface.hpp>

#include <memory>
#include <stdexcept>

// Consumes or skips every 2nd batch.
// Depending on the 'side' it can either consume first and skip second
// or skip first and consume second. Two consumers with the opposite side
// chained together are supposed to consume full set of batches and skip full
// set of batches.
struct round_robin_consumer : storage::batch_consumer {
    consume_result
    accept_batch_start(const model::record_batch_header& h) const override {
        if (expect_close) {
            if (accept_cnt == 0) {
                accept_cnt++;
                return consume_result::stop_parser;
            } else {
                BOOST_FAIL("Unexpected accept_batch_start");
            }
        }
        if ((static_cast<size_t>(h.base_offset()) % 2) == side) {
            accept_cnt++;
            return consume_result::accept_batch;
        }
        return consume_result::skip_batch;
    }

    void consume_batch_start(
      model::record_batch_header hdr, size_t, size_t) override {
        if (expect_close) {
            BOOST_FAIL("Unexpected consume_batch_start");
        }
        consumed->push_back(hdr);
    }

    void
    skip_batch_start(model::record_batch_header hdr, size_t, size_t) override {
        if (expect_close) {
            BOOST_FAIL("Unexpected skip_batch_start");
        }
        skipped->push_back(hdr);
    }

    void consume_records(iobuf&&) override {
        if (expect_close) {
            BOOST_FAIL("Unexpected consume_records");
        }
    }

    ss::future<stop_parser> consume_batch_end() override {
        if (expect_close) {
            BOOST_FAIL("Unexpected consume_batch_end");
        }
        if (consumed->size() < size_limit) {
            co_return stop_parser::no;
        }
        co_return stop_parser::yes;
    }

    void print(std::ostream& o) const override { o << "test_consumer"; }

    size_t side{0};
    size_t size_limit{0};
    std::deque<model::record_batch_header>* consumed;
    std::deque<model::record_batch_header>* skipped;
    // If this is true any call to consume_batch_* or skip_batch_* will
    // trigger test failure.
    bool expect_close{false};
    mutable size_t accept_cnt{0};
};

struct throwing_consumer : storage::batch_consumer {
    consume_result
    accept_batch_start(const model::record_batch_header&) const override {
        throw std::runtime_error("Expected exception");
    }

    void
    consume_batch_start(model::record_batch_header, size_t, size_t) override {
        BOOST_FAIL("Unexpected consume_batch_start");
        __builtin_unreachable();
    }

    void skip_batch_start(model::record_batch_header, size_t, size_t) override {
        BOOST_FAIL("Unexpected skip_batch_start");
        __builtin_unreachable();
    }

    void consume_records(iobuf&&) override {
        BOOST_FAIL("Unexpected consume_records");
        __builtin_unreachable();
    }

    ss::future<stop_parser> consume_batch_end() override {
        BOOST_FAIL("Unexpected consume_batch_end");
        __builtin_unreachable();
    }

    void print(std::ostream& o) const override {
        o << "throwing_test_consumer";
    }
};

SEASTAR_THREAD_TEST_CASE(test_chained_consumers_round_robin) {
    temporary_dir tmp_dir("chained_consumers");
    auto tmp_path = tmp_dir.get_path();
    using namespace storage;

    disk_log_builder b{log_config{
      tmp_path.string(),
      1024 * 1024,
      ss::default_priority_class(),
      storage::make_sanitized_file_config()}};

    b | start(ntp_config{{"kafka", "panda-topic", 0}, {tmp_path}})
      | add_segment(0);

    for (int i = 0; i < 20; i++) {
        b | add_random_batch(i, 1);
    }
    auto defer = ss::defer([&b] { b.stop().get(); });

    std::deque<model::record_batch_header> consumed;
    std::deque<model::record_batch_header> skipped;

    auto c1 = std::make_unique<round_robin_consumer>();
    c1->side = 0;
    c1->size_limit = 10000;
    c1->consumed = &consumed;
    c1->skipped = &skipped;
    auto c2 = std::make_unique<round_robin_consumer>();
    c2->side = 1;
    c2->size_limit = 10000;
    c2->consumed = &consumed;
    c2->skipped = &skipped;

    auto cc = std::make_unique<chained_batch_consumer<2>>(
      std::move(c1), std::move(c2));
    auto s = b.get_log_segments().front();
    auto str = s->offset_data_stream(
                  s->offsets().get_base_offset(), ss::default_priority_class())
                 .get();

    auto parser = ss::make_lw_shared<storage::continuous_batch_parser>(
      std::move(cc), storage::segment_reader_handle(std::move(str)));

    auto close = ss::defer([parser] { parser->close().get(); });

    auto cnt = parser->consume().get();
    BOOST_REQUIRE(cnt.has_value());
    BOOST_REQUIRE(cnt.value() > 0);
    BOOST_REQUIRE(consumed.size() > 0);
    BOOST_REQUIRE(consumed.size() == skipped.size());
    BOOST_REQUIRE(consumed == skipped);
}

SEASTAR_THREAD_TEST_CASE(test_chained_consumers_early_stop) {
    temporary_dir tmp_dir("chained_consumers");
    auto tmp_path = tmp_dir.get_path();
    using namespace storage;

    disk_log_builder b{log_config{
      tmp_path.string(),
      1024 * 1024,
      ss::default_priority_class(),
      storage::make_sanitized_file_config()}};

    b | start(ntp_config{{"kafka", "panda-topic", 0}, {tmp_path}})
      | add_segment(0);

    for (int i = 0; i < 20; i++) {
        b | add_random_batch(i, 1);
    }
    auto defer = ss::defer([&b] { b.stop().get(); });

    std::deque<model::record_batch_header> consumed;
    std::deque<model::record_batch_header> skipped;

    auto c_stopped = std::make_unique<round_robin_consumer>();
    c_stopped->side = 0;
    c_stopped->size_limit = 0;
    c_stopped->consumed = &consumed;
    c_stopped->skipped = &skipped;
    c_stopped->expect_close = true;
    auto c1 = std::make_unique<round_robin_consumer>();
    c1->side = 0;
    c1->size_limit = 10000;
    c1->consumed = &consumed;
    c1->skipped = &skipped;
    auto c2 = std::make_unique<round_robin_consumer>();
    c2->side = 1;
    c2->size_limit = 10000;
    c2->consumed = &consumed;
    c2->skipped = &skipped;

    auto cc = std::make_unique<chained_batch_consumer<3>>(
      std::move(c_stopped), std::move(c1), std::move(c2));
    auto s = b.get_log_segments().front();
    auto str = s->offset_data_stream(
                  s->offsets().get_base_offset(), ss::default_priority_class())
                 .get();

    auto parser = ss::make_lw_shared<storage::continuous_batch_parser>(
      std::move(cc), storage::segment_reader_handle(std::move(str)));

    auto close = ss::defer([parser] { parser->close().get(); });

    auto cnt = parser->consume().get();
    // Check that stopped consumer does not affect the remaining consumers
    BOOST_REQUIRE(cnt.has_value());
    BOOST_REQUIRE(cnt.value() > 0);
    BOOST_REQUIRE(consumed.size() > 0);
    BOOST_REQUIRE(consumed.size() == skipped.size());
    BOOST_REQUIRE(consumed == skipped);
}

SEASTAR_THREAD_TEST_CASE(test_chained_consumers_stop_all) {
    temporary_dir tmp_dir("chained_consumers");
    auto tmp_path = tmp_dir.get_path();
    using namespace storage;

    disk_log_builder b{log_config{
      tmp_path.string(),
      1024 * 1024,
      ss::default_priority_class(),
      storage::make_sanitized_file_config()}};

    b | start(ntp_config{{"kafka", "panda-topic", 0}, {tmp_path}})
      | add_segment(0);

    for (int i = 0; i < 20; i++) {
        b | add_random_batch(i, 1);
    }
    auto defer = ss::defer([&b] { b.stop().get(); });

    std::deque<model::record_batch_header> consumed;
    std::deque<model::record_batch_header> skipped;

    auto c1 = std::make_unique<round_robin_consumer>();
    c1->side = 0;
    c1->size_limit = 0;
    c1->consumed = &consumed;
    c1->skipped = &skipped;
    c1->expect_close = true;
    auto c2 = std::make_unique<round_robin_consumer>();
    c2->side = 1;
    c2->size_limit = 0;
    c2->consumed = &consumed;
    c2->skipped = &skipped;
    c2->expect_close = true;

    auto cc = std::make_unique<chained_batch_consumer<2>>(
      std::move(c1), std::move(c2));

    auto s = b.get_log_segments().front();
    auto str = s->offset_data_stream(
                  s->offsets().get_base_offset(), ss::default_priority_class())
                 .get();

    auto parser = ss::make_lw_shared<storage::continuous_batch_parser>(
      std::move(cc), storage::segment_reader_handle(std::move(str)));

    auto close = ss::defer([parser] { parser->close().get(); });

    auto cnt = parser->consume().get();

    // Check that nothing is consumed
    BOOST_REQUIRE(cnt.has_value());
    BOOST_REQUIRE(cnt.value() == 0);
    BOOST_REQUIRE(consumed.size() == 0);
    BOOST_REQUIRE(consumed.size() == skipped.size());
}

SEASTAR_THREAD_TEST_CASE(test_chained_consumers_skip_all) {
    temporary_dir tmp_dir("chained_consumers");
    auto tmp_path = tmp_dir.get_path();
    using namespace storage;

    disk_log_builder b{log_config{
      tmp_path.string(),
      1024 * 1024,
      ss::default_priority_class(),
      storage::make_sanitized_file_config()}};

    b | start(ntp_config{{"kafka", "panda-topic", 0}, {tmp_path}})
      | add_segment(0);

    for (int i = 0; i < 20; i++) {
        b | add_random_batch(i, 1);
    }
    auto defer = ss::defer([&b] { b.stop().get(); });

    std::deque<model::record_batch_header> consumed;
    std::deque<model::record_batch_header> skipped;

    auto c1 = std::make_unique<round_robin_consumer>();
    c1->side
      = 0xFFFFFFFF; // The data set is not large enough to accept anything
    c1->size_limit = 0;
    c1->consumed = &consumed;
    c1->skipped = &skipped;
    auto c2 = std::make_unique<round_robin_consumer>();
    c2->side = 0xFFFFFFFF;
    c2->size_limit = 0;
    c2->consumed = &consumed;
    c2->skipped = &skipped;

    auto cc = std::make_unique<chained_batch_consumer<2>>(
      std::move(c1), std::move(c2));

    auto s = b.get_log_segments().front();
    auto str = s->offset_data_stream(
                  s->offsets().get_base_offset(), ss::default_priority_class())
                 .get();

    auto parser = ss::make_lw_shared<storage::continuous_batch_parser>(
      std::move(cc), storage::segment_reader_handle(std::move(str)));

    auto close = ss::defer([parser] { parser->close().get(); });

    auto cnt = parser->consume().get();

    // Check that nothing is consumed
    BOOST_REQUIRE(cnt.has_value());
    BOOST_REQUIRE(cnt.value() > 0);
    BOOST_REQUIRE(consumed.size() == 0);
    BOOST_REQUIRE(skipped.size() > 0);
    BOOST_REQUIRE_EQUAL(skipped.size(), 2 * 20); // 2 consumers 20 batches
}

SEASTAR_THREAD_TEST_CASE(test_chained_consumers_exception_propagation) {
    temporary_dir tmp_dir("chained_consumers");
    auto tmp_path = tmp_dir.get_path();
    using namespace storage;

    disk_log_builder b{log_config{
      tmp_path.string(),
      1024 * 1024,
      ss::default_priority_class(),
      storage::make_sanitized_file_config()}};

    b | start(ntp_config{{"kafka", "panda-topic", 0}, {tmp_path}})
      | add_segment(0);

    for (int i = 0; i < 20; i++) {
        b | add_random_batch(i, 1);
    }
    auto defer = ss::defer([&b] { b.stop().get(); });

    std::deque<model::record_batch_header> consumed;
    std::deque<model::record_batch_header> skipped;

    auto c_throw = std::make_unique<throwing_consumer>();
    auto c1 = std::make_unique<round_robin_consumer>();
    c1->side = 0;
    c1->size_limit = 1000;
    c1->consumed = &consumed;
    c1->skipped = &skipped;

    auto cc = std::make_unique<chained_batch_consumer<2>>(
      std::move(c1), std::move(c_throw));

    auto s = b.get_log_segments().front();
    auto str = s->offset_data_stream(
                  s->offsets().get_base_offset(), ss::default_priority_class())
                 .get();

    auto parser = ss::make_lw_shared<storage::continuous_batch_parser>(
      std::move(cc), storage::segment_reader_handle(std::move(str)));

    auto close = ss::defer([parser] { parser->close().get(); });

    BOOST_REQUIRE_THROW(parser->consume().get(), std::runtime_error);
    // other consumers in the chain shouldn't get result
    // because we stop the batch parser before any batches are
    // consumed
    BOOST_REQUIRE_EQUAL(consumed.size(), 0);
    BOOST_REQUIRE_EQUAL(skipped.size(), 0);
}
