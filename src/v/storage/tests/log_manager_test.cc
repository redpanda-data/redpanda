#include "model/fundamental.h"
#include "seastarx.h"
#include "storage/directories.h"
#include "storage/log_manager.h"
#include "storage/log_segment.h"
#include "storage/log_writer.h"
#include "storage/tests/random_batch.h"
#include "utils/file_sanitizer.h"

#include <seastar/core/thread.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>

using namespace storage;

void write_garbage(log_segment_ptr seg) {
    auto batches = test::make_random_batches(model::offset(1), 1);
    auto appender = seg->data_appender(default_priority_class());
    auto b = test::make_buffer(100);
    appender.append(b.get(), b.size()).get();
    appender.flush().get();
    seg->flush().get();
}

void write_batches(log_segment_ptr seg) {
    auto batches = test::make_random_batches(model::offset(1), 1);
    auto appender = seg->data_appender(default_priority_class());
    for (auto& b : batches) {
        storage::write(appender, b).get();
    }
    appender.flush().get();
    seg->flush().get();
}

log_config make_config() {
    return log_config{"test_dir", 1024, log_config::sanitize_files::yes};
}

SEASTAR_THREAD_TEST_CASE(test_can_load_logs) {
    log_manager m(make_config());
    auto stop_manager = defer([&m] { m.stop().get(); });

    auto ntp = model::namespaced_topic_partition{
      model::ns("ns1"),
      model::topic_partition{model::topic("tp1"), model::partition_id(11)}};
    directories::initialize("test_dir/" + ntp.path()).get();
    // Empty file
    auto seg
      = m.make_log_segment(ntp, model::offset(10), model::term_id(1)).get0();
    seg->close().get();

    auto ntp2 = model::namespaced_topic_partition{
      model::ns("ns1"),
      model::topic_partition{model::topic("tp1"), model::partition_id(1)}};
    directories::initialize("test_dir/" + ntp2.path()).get();
    // Empty dir

    auto ntp3 = model::namespaced_topic_partition{
      model::ns("ns1"),
      model::topic_partition{model::topic("tp2"), model::partition_id(33)}};
    directories::initialize("test_dir/" + ntp3.path()).get();
    auto seg3
      = m.make_log_segment(ntp3, model::offset(20), model::term_id(1)).get0();
    write_batches(seg3);
    seg3->close().get();

    auto ntp4 = model::namespaced_topic_partition{
      model::ns("ns2"),
      model::topic_partition{model::topic("tp1"), model::partition_id(50)}};
    directories::initialize("test_dir/" + ntp4.path()).get();
    auto seg4
      = m.make_log_segment(ntp4, model::offset(2), model::term_id(1)).get0();
    write_garbage(seg4);
    seg4->close().get();

    m.load_logs().get();
    BOOST_CHECK_EQUAL(4, m.logs().size());
    for (auto& [ntp, log] : m.logs()) {
        BOOST_CHECK_EQUAL(size_t(ntp == ntp3), log->segments().size());
    }
    BOOST_CHECK(!file_exists(seg->get_filename()).get0());
    BOOST_CHECK(file_exists(seg3->get_filename()).get0());
    BOOST_CHECK(!file_exists(seg4->get_filename()).get0());
    BOOST_CHECK(file_exists(seg4->get_filename() + ".cannotrecover").get0());
}