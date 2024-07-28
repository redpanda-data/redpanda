// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/details/io_iterator_consumer.h"
#include "bytes/iobuf_parser.h"
#include "kafka/protocol/batch_consumer.h"
#include "kafka/protocol/batch_reader.h"
#include "kafka/protocol/exceptions.h"
#include "kafka/protocol/kafka_batch_adapter.h"
#include "model/fundamental.h"
#include "model/tests/random_batch.h"

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/sstring.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/unit_test.hpp>

#include <iterator>
#include <type_traits>

struct context {
    model::offset base_offset;
    model::offset last_offset;
    iobuf record_set;
};

context make_context(model::offset base_offset, size_t batch_count) {
    auto input
      = model::test::make_random_batches(base_offset, batch_count).get();
    BOOST_REQUIRE(!input.empty());
    const auto last_offset = input.back().last_offset();

    // Serialise the batches
    auto mem_res = model::make_memory_record_batch_reader(std::move(input))
                     .consume(
                       kafka::kafka_batch_serializer{}, model::no_timeout)
                     .get();
    BOOST_REQUIRE_EQUAL(
      (last_offset - base_offset)() + 1, mem_res.record_count);
    return context{
      .base_offset{base_offset},
      .last_offset{last_offset},
      .record_set{std::move(mem_res.data)}};
}

void corrupt_with_short_header(iobuf& record_set) {
    record_set.trim_back(
      record_set.size_bytes() - kafka::internal::kafka_header_size + 3);
}

template<
  typename T,
  typename = std::enable_if_t<std::is_trivially_copyable_v<T>>>
void corrupt_offset(iobuf& buf, size_t offset, void (*fun)(T& t)) {
    ss::sstring linearised{ss::uninitialized_string(buf.size_bytes())};
    auto consumer = details::io_iterator_consumer{buf.begin(), buf.end()};
    consumer.consume_to(buf.size_bytes(), linearised.data());
    buf.clear();

    T t;
    auto val_it = reinterpret_cast<char*>(&t);                         // NOLINT
    auto buf_it = reinterpret_cast<char*>(linearised.data() + offset); // NOLINT
    std::memcpy(val_it, buf_it, sizeof(T));
    fun(t);
    std::memcpy(buf_it, val_it, sizeof(T));

    buf.append(linearised.data(), linearised.size());
}

static constexpr size_t mag_offset = sizeof(model::offset) + // base_offset
                                     sizeof(int32_t) +       // batch_length
                                     sizeof(int32_t);        // part_lead_epoch
static constexpr size_t crc_offset = mag_offset +            //
                                     sizeof(int8_t);         // magic
static constexpr size_t lod_offset = crc_offset +            //
                                     sizeof(int32_t) +       // crc
                                     sizeof(int16_t);        // attr

static constexpr model::offset base_offset(42);
static constexpr size_t few_batches = 2;
static constexpr size_t many_batches = 40;

SEASTAR_THREAD_TEST_CASE(batch_reader_last_offset) {
    auto ctx = make_context(base_offset, many_batches);

    auto crs = kafka::batch_reader(std::move(ctx.record_set));
    BOOST_REQUIRE_EQUAL(crs.last_offset(), ctx.last_offset);
}

SEASTAR_THREAD_TEST_CASE(batch_reader_last_offset_short_header) {
    auto ctx = make_context(base_offset, few_batches);
    corrupt_with_short_header(ctx.record_set);

    auto crs = kafka::batch_reader(std::move(ctx.record_set));
    BOOST_REQUIRE_EXCEPTION(
      crs.last_offset(), kafka::exception, [](const kafka::exception& e) {
          return e.error == kafka::error_code::corrupt_message;
      });
}

SEASTAR_THREAD_TEST_CASE(consumer_records_consume_batch) {
    auto ctx = make_context(base_offset, many_batches);

    auto crs = kafka::batch_reader(std::move(ctx.record_set));

    model::offset last_offset{};
    while (!crs.empty()) {
        auto kba = crs.consume_batch();
        BOOST_REQUIRE(kba.v2_format);
        BOOST_REQUIRE(kba.valid_crc);
        BOOST_REQUIRE(kba.batch);
        last_offset = kba.batch->last_offset();
    }

    BOOST_REQUIRE_EQUAL(last_offset, ctx.last_offset);
}

SEASTAR_THREAD_TEST_CASE(consumer_records_consume_batch_fail_magic) {
    auto ctx = make_context(base_offset, few_batches);
    corrupt_offset<int32_t>(
      ctx.record_set, mag_offset, [](int32_t& t) { --t; });

    auto crs = kafka::batch_reader(std::move(ctx.record_set));

    auto kba = crs.consume_batch();
    BOOST_REQUIRE(!kba.v2_format);
}

SEASTAR_THREAD_TEST_CASE(consumer_records_consume_batch_fail_crc) {
    auto ctx = make_context(base_offset, few_batches);
    corrupt_offset<int32_t>(
      ctx.record_set, crc_offset, [](int32_t& t) { --t; });

    auto crs = kafka::batch_reader(std::move(ctx.record_set));

    auto kba = crs.consume_batch();
    BOOST_REQUIRE(!kba.valid_crc);
}

SEASTAR_THREAD_TEST_CASE(batch_reader_record_batch_reader_impl) {
    auto ctx = make_context(base_offset, many_batches);

    auto rdr = model::make_record_batch_reader<kafka::batch_reader>(
      std::move(ctx.record_set));
    auto output = model::consume_reader_to_memory(
                    std::move(rdr), model::no_timeout)
                    .get();

    BOOST_REQUIRE(!output.empty());
    BOOST_REQUIRE_EQUAL(output.back().last_offset(), ctx.last_offset);
}

SEASTAR_THREAD_TEST_CASE(batch_reader_record_batch_reader_impl_fail_short_hdr) {
    auto ctx = make_context(base_offset, few_batches);
    corrupt_with_short_header(ctx.record_set);

    auto rdr = model::make_record_batch_reader<kafka::batch_reader>(
      std::move(ctx.record_set));

    BOOST_REQUIRE_EXCEPTION(
      // NOLINTNEXTLINE(bugprone-use-after-move)
      model::consume_reader_to_memory(std::move(rdr), model::no_timeout).get(),
      kafka::exception,
      [](const kafka::exception& e) {
          return e.error == kafka::error_code::corrupt_message;
      });
}

SEASTAR_THREAD_TEST_CASE(batch_reader_record_batch_reader_impl_fail_crc) {
    auto ctx = make_context(base_offset, few_batches);
    corrupt_offset<int32_t>(
      ctx.record_set, crc_offset, [](int32_t& t) { --t; });

    auto rdr = model::make_record_batch_reader<kafka::batch_reader>(
      std::move(ctx.record_set));

    BOOST_REQUIRE_EXCEPTION(
      // NOLINTNEXTLINE(bugprone-use-after-move)
      model::consume_reader_to_memory(std::move(rdr), model::no_timeout).get(),
      kafka::exception,
      [](const kafka::exception& e) {
          return e.error == kafka::error_code::corrupt_message;
      });
}

SEASTAR_THREAD_TEST_CASE(batch_reader_record_batch_reader_impl_fail_lod) {
    auto ctx = make_context(base_offset, few_batches);
    // The lod is an arbitrary choice here, failure is due to crc mismatch.
    corrupt_offset<int32_t>(
      ctx.record_set, lod_offset, [](int32_t& t) { --t; });

    auto rdr = model::make_record_batch_reader<kafka::batch_reader>(
      std::move(ctx.record_set));

    BOOST_REQUIRE_EXCEPTION(
      // NOLINTNEXTLINE(bugprone-use-after-move)
      model::consume_reader_to_memory(std::move(rdr), model::no_timeout).get(),
      kafka::exception,
      [](const kafka::exception& e) {
          return e.error == kafka::error_code::corrupt_message;
      });
}

SEASTAR_THREAD_TEST_CASE(batch_reader_record_batch_reader_impl_fail_magic) {
    auto ctx = make_context(base_offset, few_batches);
    corrupt_offset<int32_t>(
      ctx.record_set, mag_offset, [](int32_t& t) { --t; });

    auto rdr = model::make_record_batch_reader<kafka::batch_reader>(
      std::move(ctx.record_set));

    BOOST_REQUIRE_EXCEPTION(
      // NOLINTNEXTLINE(bugprone-use-after-move)
      model::consume_reader_to_memory(std::move(rdr), model::no_timeout).get(),
      kafka::exception,
      [](const kafka::exception& e) {
          return e.error == kafka::error_code::corrupt_message;
      });
}
