#include "model/fundamental.h"
#include "model/record_utils.h"
#include "random/generators.h"
#include "storage/api.h"
#include "storage/directories.h"
#include "storage/disk_log_appender.h"
#include "storage/segment_appender.h"
#include "storage/segment_appender_utils.h"
#include "storage/segment_reader.h"
#include "storage/tests/utils/random_batch.h"
#include "utils/file_sanitizer.h"

#include <seastar/core/thread.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>

using namespace std::chrono_literals; // NOLINT
using namespace storage;              // NOLINT

void write_garbage(segment_appender& ptr) {
    auto b = random_generators::get_bytes(100);
    // NOLINTNEXTLINE
    ptr.append(reinterpret_cast<const char*>(b.data()), b.size()).get();
    ptr.flush().get();
}

void write_batches(ss::lw_shared_ptr<segment> seg) {
    auto batches = test::make_random_batches(
      seg->offsets().base_offset + model::offset(1), 1);
    for (auto& b : batches) {
        b.header().header_crc = model::internal_header_only_crc(b.header());
        (void)seg->append(std::move(b)).get0();
    }
    seg->flush().get();
}

log_config make_config() {
    return log_config{
      log_config::storage_type::disk,
      "test.dir",
      1024,
      debug_sanitize_files::yes};
}

ntp_config config_from_ntp(const model::ntp& ntp) {
    return ntp_config(ntp, "test.dir");
}

SEASTAR_THREAD_TEST_CASE(test_can_load_logs) {
    auto conf = make_config();
    storage::api store(
      storage::kvstore_config(
        1_MiB, 10ms, conf.base_dir, storage::debug_sanitize_files::yes),
      conf);
    store.start().get();
    auto stop_kvstore = ss::defer([&store] { store.stop().get(); });
    auto& m = store.log_mgr();
    std::vector<storage::ntp_config> ntps;
    ntps.reserve(4);
    for (size_t i = 0; i < 4; ++i) {
        ntps.push_back(
          config_from_ntp(model::ntp(fmt::format("ns{}", i), "topic-1", i)));
        directories::initialize(ntps[i].work_directory()).get();
    }
    auto seg = m.make_log_segment(
                  ntps[0],
                  model::offset(10),
                  model::term_id(1),
                  ss::default_priority_class())
                 .get0();
    seg->close().get();

    // auto ntp2 = empty

    auto seg3 = m.make_log_segment(
                   ntps[2],
                   model::offset(20),
                   model::term_id(1),
                   ss::default_priority_class())
                  .get0();
    write_batches(seg3);
    seg3->close().get();

    auto seg4 = m.make_log_segment(
                   ntps[3],
                   model::offset(2),
                   model::term_id(1),
                   ss::default_priority_class())
                  .get0();
    write_garbage(seg4->appender());
    seg4->close().get();

    m.manage(config_from_ntp(ntps[0].ntp())).get();
    m.manage(config_from_ntp(ntps[1].ntp())).get();
    m.manage(config_from_ntp(ntps[2].ntp())).get();
    m.manage(config_from_ntp(ntps[3].ntp())).get();
    BOOST_CHECK_EQUAL(4, m.size());
    BOOST_CHECK_EQUAL(m.get(ntps[0].ntp())->segment_count(), 0);
    BOOST_CHECK_EQUAL(m.get(ntps[1].ntp())->segment_count(), 0);
    BOOST_CHECK_EQUAL(m.get(ntps[2].ntp())->segment_count(), 1);
    BOOST_CHECK_EQUAL(m.get(ntps[3].ntp())->segment_count(), 0);
    BOOST_CHECK(!file_exists(seg->reader().filename()).get0());
    BOOST_CHECK(file_exists(seg3->reader().filename()).get0());
    BOOST_CHECK(!file_exists(seg4->reader().filename()).get0());
    BOOST_CHECK(
      file_exists(seg4->reader().filename() + ".cannotrecover").get0());
}
