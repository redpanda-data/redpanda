#pragma once
#include "model/fundamental.h"
#include "model/record_batch_reader.h"
#include "seastarx.h"
#include "storage/log_manager.h"

namespace tests {

static inline storage::log_manager make_log_mgr(ss::sstring base_dir) {
    return storage::log_manager(storage::log_config(
      storage::log_config::storage_type::disk,
      std::move(base_dir),
      1_GiB,
      storage::log_config::debug_sanitize_files::yes));
}

static inline ss::future<> persist_log_file(
  ss::sstring base_dir,
  model::ntp file_ntp,
  ss::circular_buffer<model::record_batch> batches) {
    return ss::do_with(
      make_log_mgr(std::move(base_dir)),
      [file_ntp = std::move(file_ntp),
       batches = std::move(batches)](storage::log_manager& mgr) mutable {
          return mgr
            .manage(storage::ntp_config(
              file_ntp,
              fmt::format("{}/{}", mgr.config().base_dir, file_ntp.path())))
            .then([b = std::move(batches)](storage::log log) mutable {
                storage::log_append_config cfg{
                  storage::log_append_config::fsync::yes,
                  ss::default_priority_class(),
                  model::no_timeout};
                auto reader = model::make_memory_record_batch_reader(
                  std::move(b));
                return std::move(reader)
                  .for_each_ref(log.make_appender(cfg), cfg.timeout)
                  .then([log](storage::append_result) mutable {
                      return log.flush();
                  })
                  .finally([log] {});
            })
            .finally([&mgr] { return mgr.stop(); })
            .discard_result();
      });
}

struct to_vector_consumer {
    ss::future<ss::stop_iteration> operator()(model::record_batch&& batch) {
        _batches.push_back(std::move(batch));
        return ss::make_ready_future<ss::stop_iteration>(
          ss::stop_iteration::no);
    }

    ss::circular_buffer<model::record_batch> end_of_stream() {
        return std::move(_batches);
    }

private:
    ss::circular_buffer<model::record_batch> _batches;
};

static inline ss::future<ss::circular_buffer<model::record_batch>>
read_log_file(ss::sstring base_dir, model::ntp file_ntp) {
    return ss::do_with(
      make_log_mgr(std::move(base_dir)),
      [file_ntp = std::move(file_ntp)](storage::log_manager& mgr) mutable {
          return mgr
            .manage(storage::ntp_config(
              file_ntp,
              fmt::format("{}/{}", mgr.config().base_dir, file_ntp.path())))
            .then([](storage::log log) mutable {
                return log
                  .make_reader(storage::log_reader_config(
                    model::offset(0),
                    model::model_limits<model::offset>::max(),
                    ss::default_priority_class()))
                  .then([](model::record_batch_reader reader) {
                      return std::move(reader).consume(
                        to_vector_consumer(), model::no_timeout);
                  });
            })
            .finally([&mgr] { return mgr.stop(); });
      });
}
} // namespace tests
