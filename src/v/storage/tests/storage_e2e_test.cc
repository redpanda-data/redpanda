#include "storage/tests/storage_test_fixture.h"

void validate_offsets(
  model::offset base,
  const std::vector<model::record_batch_header>& write_headers,
  const std::vector<model::record_batch>& read_batches) {
    auto it = read_batches.begin();
    model::offset next_base = base;
    for (auto const h : write_headers) {
        BOOST_REQUIRE_EQUAL(it->base_offset(), next_base);
        // last offset delta is inclusive (record with this offset belongs to
        // previous batch)
        next_base += (h.last_offset_delta + model::offset(1));
        it++;
    }
}

FIXTURE_TEST(
  test_assinging_offsets_in_single_segment_log, storage_test_fixture) {
    storage::log_manager mgr = make_log_manager();
    auto ntp = make_ntp("default", "test", 0);
    auto log_ptr = mgr.manage(ntp).get0();
    auto headers = append_random_batches(log_ptr, 10, 10);
    log_ptr->flush().get0();
    auto batches = read_and_validate_all_batches(log_ptr);

    BOOST_REQUIRE_EQUAL(headers.size(), batches.size());
    BOOST_REQUIRE_EQUAL(log_ptr->max_offset(), batches.back().end_offset());
    BOOST_REQUIRE_EQUAL(
      log_ptr->committed_offset(), batches.back().end_offset());
    validate_offsets(model::offset(0), headers, batches);
    mgr.stop().get0();
};

// Uncomment when writing after flush will be fixed in appender
#if 0
FIXTURE_TEST(append_twice_to_same_segment, storage_test_fixture) {
    storage::log_manager mgr = make_log_manager();

    auto ntp = make_ntp("default", "test", 0);
    auto log_ptr = mgr.manage(ntp).get0();
    auto headers = append_random_batches(log_ptr, 10, 10);
    log_ptr->flush().get0();
    auto headers_2 = append_random_batches(log_ptr, 10, 10);
    log_ptr->flush().get0();
    std::move(
      std::begin(headers_2), std::end(headers_2), std::back_inserter(headers));
    auto batches = read_and_validate_all_batches(log_ptr);

    BOOST_REQUIRE_EQUAL(headers.size(), batches.size());
    BOOST_REQUIRE_EQUAL(log_ptr->max_offset(), batches.back().end_offset());
    BOOST_REQUIRE_EQUAL(
      log_ptr->committed_offset(), batches.back().end_offset());

    mgr.stop().get0();
};
#endif

FIXTURE_TEST(test_assigning_offsets_in_multiple_segment, storage_test_fixture) {
    auto cfg = default_log_config(test_dir);
    cfg.max_segment_size = 1_kb;
    storage::log_manager mgr = make_log_manager(std::move(cfg));
    auto ntp = make_ntp("default", "test", 0);
    auto log_ptr = mgr.manage(ntp).get0();
    auto headers = append_random_batches(log_ptr, 10, 10);
    log_ptr->flush().get0();
    auto batches = read_and_validate_all_batches(log_ptr);

    BOOST_REQUIRE_EQUAL(headers.size(), batches.size());
    BOOST_REQUIRE_EQUAL(log_ptr->max_offset(), batches.back().end_offset());
    BOOST_REQUIRE_EQUAL(
      log_ptr->committed_offset(), batches.back().end_offset());
    validate_offsets(model::offset(0), headers, batches);
    mgr.stop().get0();
};