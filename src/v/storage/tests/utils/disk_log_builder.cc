#include "storage/tests/utils/disk_log_builder.h"
// util functions to be moved from storage_fixture
// make_ntp, make_dir etc
namespace storage {
disk_log_builder::disk_log_builder(storage::log_config config)
  : _config(config)
  , _mgr(config) {}

// Batch generation
ss::future<> disk_log_builder::add_random_batch(
  model::offset offset,
  int num_records,
  maybe_compress_batches comp,
  log_append_config config) {
    auto buff = ss::circular_buffer<model::record_batch>();
    buff.push_back(test::make_random_batch(offset, num_records, bool(comp)));
    return write(std::move(buff), config);
}

ss::future<> disk_log_builder::add_random_batch(
  model::offset offset, maybe_compress_batches comp, log_append_config config) {
    auto buff = ss::circular_buffer<model::record_batch>();
    buff.push_back(test::make_random_batch(offset, bool(comp)));
    return write(std::move(buff), config);
}

ss::future<> disk_log_builder::add_random_batches(
  model::offset offset,
  int count,
  maybe_compress_batches comp,
  log_append_config config) {
    return write(test::make_random_batches(offset, count, bool(comp)), config);
}

ss::future<> disk_log_builder::add_random_batches(
  model::offset offset, log_append_config config) {
    return write(test::make_random_batches(offset), config);
}

// Log managment
ss::future<> disk_log_builder::start(model::ntp ntp) {
    return _mgr.manage(ntp).then([this](log l) { _log = std::move(l); });
}

ss::future<> disk_log_builder::truncate(model::offset o) {
    return get_log().truncate(o);
}

ss::future<> disk_log_builder::gc(
  model::timestamp collection_upper_bound,
  std::optional<size_t> max_partition_retention_size) {
    return get_log().gc(collection_upper_bound, max_partition_retention_size);
}

ss::future<> disk_log_builder::stop() { return _mgr.stop(); }
// Low lever interface access
// Access log impl
log& disk_log_builder::get_log() {
    vassert(_log.has_value(), "Log is unintialized. Please use start() first");
    return *_log;
}

disk_log_impl& disk_log_builder::get_disk_log_impl() {
    return *reinterpret_cast<disk_log_impl*>(_log->get_impl());
}

segment_set& disk_log_builder::get_log_segments() {
    auto& segment_set = get_disk_log_impl().segments();
    vassert(!segment_set.empty(), "There are no segments in the segment_set");
    return segment_set;
}

segment& disk_log_builder::get_segment(size_t index) {
    auto& segment_set = get_log_segments();
    vassert(
      index < segment_set.size(), "There are no segments in the segment_set");
    return *std::next(segment_set.begin(), index)->get();
}

segment_index& disk_log_builder::get_seg_index_ptr(size_t index) {
    return get_segment(index).index();
}

// Create segments
ss::future<> disk_log_builder::add_segment(
  model::offset offset, model::term_id term, ss::io_priority_class pc) {
    return get_disk_log_impl().new_segment(offset, term, pc);
}

// Configuration getters
const log_config& disk_log_builder::get_log_config() const { return _config; }

// Common interface for appending batches
ss::future<> disk_log_builder::write(
  ss::circular_buffer<model::record_batch> buff,
  const log_append_config& config) {
    auto reader = model::make_memory_record_batch_reader(std::move(buff));
    return std::move(reader)
      .consume(_log->make_appender(config), config.timeout)
      .then([this](auto) { return _log->flush(); });
}

} // namespace storage
