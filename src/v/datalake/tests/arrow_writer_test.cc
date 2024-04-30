#include "datalake/arrow_writing_consumer.h"
#include "storage/tests/storage_test_fixture.h"
#include "test_utils/test.h"

#include <seastar/util/defer.hh>

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>
#include <gtest/gtest.h>

FIXTURE_TEST(parquet_writer_fixture, storage_test_fixture) {
    auto cfg = default_log_config(test_dir);
    cfg.max_segment_size = config::mock_binding<size_t>(10 * 1024);
    storage::log_manager mgr = make_log_manager(std::move(cfg));
    info("Configuration: {}", mgr.config());
    auto deferred = ss::defer([&mgr]() mutable { mgr.stop().get0(); });
    auto ntp = model::ntp("default", "test", 0);
    auto log
      = mgr.manage(storage::ntp_config(ntp, mgr.config().base_dir)).get0();

    // Append some linear kv ints.
    int num_batches = 5;
    append_random_batches<linear_int_kv_batch_generator>(log, num_batches);
    log->flush().get0();

    // Validate
    auto batches = read_and_validate_all_batches(log);

    // Consume it
    storage::log_reader_config reader_cfg(
      model::offset(0),
      model::model_limits<model::offset>::max(),
      0,
      4096,
      ss::default_priority_class(),
      std::nullopt,
      std::nullopt,
      std::nullopt);
    auto reader = log->make_reader(reader_cfg).get0();
    datalake::arrow_writing_consumer consumer("/dev/null");
    auto status = reader.consume(std::move(consumer), model::no_timeout).get0();
    BOOST_CHECK(status.ok());

    // auto columns_vec = table->ColumnNames();
    // std::set<std::string> columns(columns_vec.cbegin(), columns_vec.cend());
    // std::set<std::string> expected_columns = {
    //   "Key", "Value", "Timestamp", "Offset"};
    // BOOST_CHECK_EQUAL_COLLECTIONS(
    //   columns.cbegin(),
    //   columns.cend(),
    //   expected_columns.cbegin(),
    //   expected_columns.cend());

    // int expected_rows = 25; // 5 batches with 5 items / batch
    // BOOST_CHECK_EQUAL(table->GetColumnByName("Key")->length(),
    // expected_rows);
}
