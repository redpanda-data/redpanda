#include "bytes/iobuf.h"
#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "storage/constants.h"
#include "storage/log.h"
#include "storage/log_manager.h"
#include "storage/log_segment_reader.h"
#include "storage/log_writer.h"
#include "storage/tests/random_batch.h"

#include <seastar/core/byteorder.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/unaligned.hh>
#include <seastar/testing/thread_test_case.hh>

#include <fmt/ostream.h>

using namespace storage; // NOLINT

struct context {
    context(size_t max_segment_size, ss::sstring test_name)
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

    ~context() { manager.stop().get(); }
};

log_append_config config() {
    return log_append_config{log_append_config::fsync::yes,
                             ss::default_priority_class(),
                             model::timeout_clock::time_point::max()};
}

SEASTAR_THREAD_TEST_CASE(test_can_write_single_batch) {
    std::cout.setf(std::ios::unitbuf);

    context ctx(1 * 1024 * 1024, "single_batch");
    auto batches = test::make_random_batches(model::offset(1), 1);
    auto reader = model::make_memory_record_batch_reader(std::move(batches));
    ctx.log->append(std::move(reader), config()).get();

    BOOST_REQUIRE(ctx.log->segments().begin() != ctx.log->segments().end());
    auto seg = *ctx.log->segments().begin();
    ctx.log->flush().get();
    auto in = seg->data_stream(0, ss::default_priority_class());
    auto buf = in.read_exactly(sizeof(packed_header_size)).get0();
    BOOST_REQUIRE(!buf.empty());
    auto [value, read] = vint::deserialize(buf);
    buf.trim_front(read);
    auto offset = be_to_cpu(*ss::unaligned_cast<uint64_t*>(buf.get()));
    BOOST_REQUIRE_EQUAL(offset, 0);

    BOOST_REQUIRE_EQUAL(value + read, ctx.log->appender().file_byte_offset());

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
        ctx.log->flush().get();
        auto in = seg->data_stream(0, ss::default_priority_class());
        auto buf = in.read_exactly(sizeof(packed_header_size)).get0();
        BOOST_REQUIRE(!buf.empty());
        auto [value, read] = vint::deserialize(buf);
        buf.trim_front(read);
        auto offset = be_to_cpu(*ss::unaligned_cast<uint64_t*>(buf.get()));
        BOOST_REQUIRE_EQUAL(offset, 0);

        auto file_size = seg->stat().get0().st_size;
        BOOST_REQUIRE_EQUAL(value + read, file_size);
        in.close().get();
    }

    {
        auto seg = *(ctx.log->segments().begin() + 1);
        auto file_size = seg->file_size();
        BOOST_REQUIRE_EQUAL(0, file_size);
    }
}

SEASTAR_THREAD_TEST_CASE(test_log_segment_rolling_middle_of_writting) {
    context ctx(1024, "rolling_middle_of_writing");
    auto first_offset = model::offset(0);
    auto batches = test::make_random_batches(first_offset, 2);
    auto second_offset = batches.back().base_offset();
    auto reader = model::make_memory_record_batch_reader(std::move(batches));
    ctx.log->append(std::move(reader), config()).get();

    {
        BOOST_REQUIRE(ctx.log->segments().begin() != ctx.log->segments().end());
        auto seg = *ctx.log->segments().begin();
        ctx.log->flush().get();
        auto in = seg->data_stream(0, ss::default_priority_class());
        auto buf = in.read_exactly(sizeof(packed_header_size)).get0();
        BOOST_REQUIRE(!buf.empty());
        auto [value, read] = vint::deserialize(buf);
        buf.trim_front(read);
        auto offset = be_to_cpu(*ss::unaligned_cast<uint64_t*>(buf.get()));
        BOOST_REQUIRE_EQUAL(offset, first_offset());

        auto file_size = seg->file_size();
        BOOST_REQUIRE_EQUAL(value + read, file_size);
        in.close().get();
    }

    {
        auto seg = *(ctx.log->segments().begin() + 1);
        ctx.log->flush().get();
        auto in = seg->data_stream(0, ss::default_priority_class());
        auto buf = in.read_exactly(sizeof(packed_header_size)).get0();
        BOOST_REQUIRE(!buf.empty());
        auto [value, read] = vint::deserialize(buf);
        buf.trim_front(read);
        auto offset = be_to_cpu(*ss::unaligned_cast<uint64_t*>(buf.get()));
        BOOST_REQUIRE_EQUAL(offset, second_offset());

        auto file_size = seg->file_size();
        BOOST_REQUIRE_EQUAL(value + read, file_size);
        in.close().get();
    }
}
