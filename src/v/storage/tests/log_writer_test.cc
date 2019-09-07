#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "storage/constants.h"
#include "storage/log.h"
#include "storage/log_manager.h"
#include "storage/log_segment.h"
#include "storage/log_writer.h"
#include "storage/tests/random_batch.h"
#include "utils/fragmented_temporary_buffer.h"

#include <seastar/core/byteorder.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/unaligned.hh>
#include <seastar/testing/thread_test_case.hh>

using namespace storage;

struct context {
    context(size_t max_segment_size)
      : config{"test", max_segment_size, log_config::sanitize_files::yes} {
    }
    log_config config;
    log_manager manager = log_manager(config);
    storage::log log = storage::log(
      model::namespaced_topic_partition{
        model::ns("ns"),
        model::topic_partition{model::topic("topic"), model::partition(6)}},
      manager,
      log_set({}));
};

log_append_config config() {
    return log_append_config{log_append_config::fsync::yes,
                             default_priority_class(),
                             model::timeout_clock::time_point::max()};
}

SEASTAR_THREAD_TEST_CASE(test_can_write_single_batch) {
    context ctx(1 * 1024 * 1024);
    auto batches = test::make_random_batches(std::vector{model::offset(1)});
    auto reader = model::make_memory_record_batch_reader(std::move(batches));
    ctx.log.append(std::move(reader), config()).get();

    BOOST_REQUIRE(ctx.log.segments().begin() != ctx.log.segments().end());
    auto seg = *ctx.log.segments().begin();
    seg->flush().get();
    auto in = seg->data_stream(0, default_priority_class());
    auto buf = in.read_exactly(sizeof(packed_header_size)).get0();
    auto [value, read] = vint::deserialize(buf);
    buf.trim_front(read);
    auto offset = be_to_cpu(*unaligned_cast<uint64_t*>(buf.get()));
    BOOST_REQUIRE_EQUAL(offset, 1);

    auto file_size = seg->stat().get0().st_size;
    BOOST_REQUIRE_EQUAL(value + read, file_size);

    in.close().get();
    ctx.log.close().get();
}

SEASTAR_THREAD_TEST_CASE(test_log_segment_rolling) {
    context ctx(1024);
    auto batches = test::make_random_batches(std::vector{model::offset(1)});
    auto reader = model::make_memory_record_batch_reader(std::move(batches));
    ctx.log.append(std::move(reader), config()).get();

    {
        BOOST_REQUIRE(ctx.log.segments().begin() != ctx.log.segments().end());
        auto seg = *ctx.log.segments().begin();
        seg->flush().get();
        auto in = seg->data_stream(0, default_priority_class());
        auto buf = in.read_exactly(sizeof(packed_header_size)).get0();
        auto [value, read] = vint::deserialize(buf);
        buf.trim_front(read);
        auto offset = be_to_cpu(*unaligned_cast<uint64_t*>(buf.get()));
        BOOST_REQUIRE_EQUAL(offset, 1);

        auto file_size = seg->stat().get0().st_size;
        BOOST_REQUIRE_EQUAL(value + read, file_size);
        in.close().get();
    }

    {
        auto seg = *(ctx.log.segments().begin() + 1);
        auto file_size = seg->stat().get0().st_size;
        BOOST_REQUIRE_EQUAL(0, file_size);
    }

    ctx.log.close().get();
}

SEASTAR_THREAD_TEST_CASE(test_log_segment_rolling_middle_of_writting) {
    context ctx(1024);
    auto batches = test::make_random_batches(
      {model::offset(1), model::offset(2)});
    auto reader = model::make_memory_record_batch_reader(std::move(batches));
    ctx.log.append(std::move(reader), config()).get();

    {
        BOOST_REQUIRE(ctx.log.segments().begin() != ctx.log.segments().end());
        auto seg = *ctx.log.segments().begin();
        seg->flush().get();
        auto in = seg->data_stream(0, default_priority_class());
        auto buf = in.read_exactly(sizeof(packed_header_size)).get0();
        auto [value, read] = vint::deserialize(buf);
        buf.trim_front(read);
        auto offset = be_to_cpu(*unaligned_cast<uint64_t*>(buf.get()));
        BOOST_REQUIRE_EQUAL(offset, 1);

        auto file_size = seg->stat().get0().st_size;
        BOOST_REQUIRE_EQUAL(value + read, file_size);
        in.close().get();
    }

    {
        auto seg = *(ctx.log.segments().begin() + 1);
        seg->flush().get();
        auto in = seg->data_stream(0, default_priority_class());
        auto buf = in.read_exactly(sizeof(packed_header_size)).get0();
        auto [value, read] = vint::deserialize(buf);
        buf.trim_front(read);
        auto offset = be_to_cpu(*unaligned_cast<uint64_t*>(buf.get()));
        BOOST_REQUIRE_EQUAL(offset, 2);

        auto file_size = seg->stat().get0().st_size;
        BOOST_REQUIRE_EQUAL(value + read, file_size);
        in.close().get();
    }

    ctx.log.close().get();
}