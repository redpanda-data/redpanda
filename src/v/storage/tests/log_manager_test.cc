#include "model/fundamental.h"
#include "storage/directories.h"
#include "storage/log_manager.h"
#include "storage/log_segment_appender.h"
#include "storage/log_segment_reader.h"
#include "storage/log_writer.h"
#include "storage/tests/random_batch.h"
#include "utils/file_sanitizer.h"

#include <seastar/core/thread.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>

using namespace storage; // NOLINT

void write_garbage(segment_appender_ptr& ptr) {
    auto batches = test::make_random_batches(model::offset(1), 1);
    auto b = test::make_buffer(100);
    ptr->append(b.get(), b.size()).get();
    ptr->flush().get();
}

void write_batches(segment_appender_ptr& seg) {
    auto batches = test::make_random_batches(model::offset(1), 1);
    for (auto& b : batches) {
        storage::write(*seg, b).get();
    }
    seg->flush().get();
}

log_config make_config() {
    return log_config{"test_dir", 1024, log_config::sanitize_files::yes};
}

SEASTAR_THREAD_TEST_CASE(test_can_load_logs) {
    log_manager m(make_config());
    auto stop_manager = ss::defer([&m] { m.stop().get(); });

    auto ntp = model::ntp{
      model::ns("ns1"),
      model::topic_partition{model::topic("tp1"), model::partition_id(11)}};
    directories::initialize("test_dir/" + ntp.path()).get();
    // Empty file
    auto&& [seg, app] = m.make_log_segment(
                           ntp,
                           model::offset(10),
                           model::term_id(1),
                           ss::default_priority_class())
                          .get0();
    seg->close().get();
    app->close().get();

    auto ntp2 = model::ntp{
      model::ns("ns1"),
      model::topic_partition{model::topic("tp1"), model::partition_id(1)}};
    directories::initialize("test_dir/" + ntp2.path()).get();
    // Empty dir

    auto ntp3 = model::ntp{
      model::ns("ns1"),
      model::topic_partition{model::topic("tp2"), model::partition_id(33)}};
    directories::initialize("test_dir/" + ntp3.path()).get();
    auto&& [seg3, app3] = m.make_log_segment(
                             ntp3,
                             model::offset(20),
                             model::term_id(1),
                             ss::default_priority_class())
                            .get0();
    write_batches(app3);
    seg3->close().get();
    app3->close().get();

    auto ntp4 = model::ntp{
      model::ns("ns2"),
      model::topic_partition{model::topic("tp1"), model::partition_id(50)}};
    directories::initialize("test_dir/" + ntp4.path()).get();
    auto&& [seg4, app4] = m.make_log_segment(
                             ntp4,
                             model::offset(2),
                             model::term_id(1),
                             ss::default_priority_class())
                            .get0();
    write_garbage(app4);
    seg4->close().get();
    app4->close().get();

    m.manage(ntp).get();
    m.manage(ntp2).get();
    m.manage(ntp3).get();
    m.manage(ntp4).get();
    BOOST_CHECK_EQUAL(4, m.logs().size());
    for (auto& [ntp, log] : m.logs()) {
        BOOST_CHECK_EQUAL(size_t(ntp == ntp3), log->segments().size());
    }
    BOOST_CHECK(!file_exists(seg->get_filename()).get0());
    BOOST_CHECK(file_exists(seg3->get_filename()).get0());
    BOOST_CHECK(!file_exists(seg4->get_filename()).get0());
    BOOST_CHECK(file_exists(seg4->get_filename() + ".cannotrecover").get0());
}
