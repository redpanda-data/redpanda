#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "storage/constants.h"
#include "storage/log.h"
#include "storage/log_manager.h"
#include "storage/log_segment.h"
#include "storage/log_writer.h"
#include "storage/tests/random_batch.h"
#include "utils/fragbuf.h"

#include <seastar/core/byteorder.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/unaligned.hh>
#include <seastar/testing/thread_test_case.hh>

using namespace storage;

struct context {
    context(size_t max_segment_size, sstring test_name)
      : config{"test", max_segment_size, log_config::sanitize_files::yes} {
        log = manager
                .manage(
                  model::ntp{model::ns(std::move(test_name)),
                             model::topic_partition{model::topic("topic"),
                                                    model::partition_id(6)}})
                .get0();
    }
    log_config config;
    log_manager manager = log_manager(config);
    log_ptr log;

    ~context() {
        manager.stop().get();
    }
};

log_append_config config() {
    return log_append_config{log_append_config::fsync::yes,
                             default_priority_class(),
                             model::timeout_clock::time_point::max()};
}

SEASTAR_THREAD_TEST_CASE(test_can_write_single_batch) {
    context ctx(1 * 1024 * 1024, "single_batch");
    auto batches = test::make_random_batches(model::offset(1), 1);
    auto reader = model::make_memory_record_batch_reader(std::move(batches));
    ctx.log->append(std::move(reader), config()).get();

    BOOST_REQUIRE(ctx.log->segments().begin() != ctx.log->segments().end());
    auto seg = *ctx.log->segments().begin();
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

SEASTAR_THREAD_TEST_CASE(test_log_segment_rolling) {
    context ctx(1024, "segment_rolling");
    auto batches = test::make_random_batches(model::offset(1), 1);
    auto reader = model::make_memory_record_batch_reader(std::move(batches));
    ctx.log->append(std::move(reader), config()).get();

    {
        BOOST_REQUIRE(ctx.log->segments().begin() != ctx.log->segments().end());
        auto seg = *ctx.log->segments().begin();
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
        auto seg = *(ctx.log->segments().begin() + 1);
        auto file_size = seg->stat().get0().st_size;
        BOOST_REQUIRE_EQUAL(0, file_size);
    }
}

SEASTAR_THREAD_TEST_CASE(test_log_segment_rolling_middle_of_writting) {
    context ctx(1024, "rolling_middle_of_writing");
    auto first_offset = model::offset(1);
    auto batches = test::make_random_batches(first_offset, 2);
    auto second_offset = batches.back().base_offset();
    auto reader = model::make_memory_record_batch_reader(std::move(batches));
    ctx.log->append(std::move(reader), config()).get();

    {
        BOOST_REQUIRE(ctx.log->segments().begin() != ctx.log->segments().end());
        auto seg = *ctx.log->segments().begin();
        seg->flush().get();
        auto in = seg->data_stream(0, default_priority_class());
        auto buf = in.read_exactly(sizeof(packed_header_size)).get0();
        auto [value, read] = vint::deserialize(buf);
        buf.trim_front(read);
        auto offset = be_to_cpu(*unaligned_cast<uint64_t*>(buf.get()));
        BOOST_REQUIRE_EQUAL(offset, first_offset.value());

        auto file_size = seg->stat().get0().st_size;
        BOOST_REQUIRE_EQUAL(value + read, file_size);
        in.close().get();
    }

    {
        auto seg = *(ctx.log->segments().begin() + 1);
        seg->flush().get();
        auto in = seg->data_stream(0, default_priority_class());
        auto buf = in.read_exactly(sizeof(packed_header_size)).get0();
        auto [value, read] = vint::deserialize(buf);
        buf.trim_front(read);
        auto offset = be_to_cpu(*unaligned_cast<uint64_t*>(buf.get()));
        BOOST_REQUIRE_EQUAL(offset, second_offset.value());

        auto file_size = seg->stat().get0().st_size;
        BOOST_REQUIRE_EQUAL(value + read, file_size);
        in.close().get();
    }
}
