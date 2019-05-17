#include <seastar/core/align.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/distributed.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sstring.hh>

#include <smf/fbs_typed_buf.h>
#include <smf/human_bytes.h>
#include <smf/log.h>
#include <smf/native_type_utils.h>

#include "hashing/xx.h"
#include "random/fast_prng.h"
#include "wal_core_mapping.h"
#include "wal_generated.h"
#include "wal_segment.h"
#include "wal_segment_record.h"
#include "wal_writer_utils.h"


static constexpr const char *kTopicName = "failure_recovery_topic_test";
class put {
 public:
  put() {
    partition_ = rand_() % std::thread::hardware_concurrency();
    seastar::sstring key = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";
    seastar::sstring value =
      "yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy";

    auto ptr = std::make_unique<wal_put_requestT>();
    ptr->topic = xxhash_64(kTopicName, std::strlen(kTopicName));

    auto idx = std::make_unique<wal_put_partition_recordsT>();
    idx->partition = partition_;
    idx->records.reserve(batch_size);
    for (auto i = 0; i < batch_size; ++i) {
      idx->records.push_back(wal_segment_record::coalesce(
        key.data(), key.size(), value.data(), value.size()));
    }

    ptr->partition_puts.push_back(std::move(idx));

    auto body = smf::native_table_as_buffer<wal_put_request>(*ptr);
    DLOG_INFO("Encoded request is of size: {}", smf::human_bytes(body.size()));
    orig_ =
      std::make_unique<smf::fbs_typed_buf<wal_put_request>>(std::move(body));
  }

  inline std::vector<wal_write_request>
  get_write_requests(const uint32_t runner_core) {
    auto p = orig_->get();
    auto ret = wal_core_mapping::core_assignment(p);
    for (auto &r : ret) {
      auto *x = const_cast<uint32_t *>(&r.runner_core);
      *x = runner_core;
    }
    return ret;
  }

  int32_t
  get_partition() const {
    return partition_;
  }

  const int32_t batch_size = 2;
  const int32_t key_size = 50;
  const int32_t value_size = 50;

 private:
  int32_t partition_;
  fast_prng rand_{};
  std::unique_ptr<smf::fbs_typed_buf<wal_put_request>> orig_ = nullptr;
};

seastar::future<>
write_via_raw_file(seastar::sstring base_dir) {
  static put payload_data{};

  return seastar::open_file_dma(base_dir + "/" + kTopicName,
                                seastar::open_flags::rw |
                                  seastar::open_flags::create |
                                  seastar::open_flags::truncate)
    .then([](seastar::file ff) mutable {
      auto f = seastar::make_lw_shared<seastar::file>(std::move(ff));
      auto sz = seastar::align_up<int64_t>(4096 * 2 /*2 pages*/,
                                           f->disk_write_dma_alignment());
      return f->allocate(0, sz).then([f] {
        // NOTE MUST: only write one page for this test

        auto buf = seastar::allocate_aligned_buffer<char>(
          f->disk_write_dma_alignment(), f->disk_write_dma_alignment());
        std::size_t idx = 0;
        for (auto &w : payload_data.get_write_requests(0)) {
          for (auto it : w) {
            std::memcpy(buf.get() + idx, it->data()->Data(),
                        it->data()->size());
            idx += it->data()->size();
          }
        }
        auto ptr = buf.get();
        return f->dma_write(0, ptr, f->disk_write_dma_alignment())
          .then([b = std::move(buf), f](auto written_bytes) {
            LOG_THROW_IF(written_bytes != f->disk_write_dma_alignment(),
                         "could not write one page");
            return f->flush().then([f] { return f->close().finally([f] {}); });
          });
      });
    });
}

void
add_opts(boost::program_options::options_description_easy_init o) {
  namespace po = boost::program_options;
  o("write-ahead-log-dir", po::value<std::string>(), "log directory");
}

int
main(int args, char **argv, char **env) {
  // set only to debug things
  std::cout.setf(std::ios::unitbuf);
  seastar::app_template app;
  try {
    add_opts(app.add_options());

    return app.run(args, argv, [&] {
      smf::app_run_log_level(seastar::log_level::trace);
      auto &config = app.configuration();
      auto dir = config["write-ahead-log-dir"].as<std::string>();
      LOG_THROW_IF(dir.empty(), "Empty write-ahead-log-dir");
      LOG_INFO("log_segment test dir: {}", dir);
      const auto full_path_file = dir + "/" + kTopicName;
      return write_via_raw_file(dir)
        .then([full_path_file] {
          return file_size_from_allocated_blocks(full_path_file);
        })
        .then([full_path_file](auto p) {
          return recover_failed_wal_file(0, p.first, 0 /*term*/,
                                            seastar::lowres_system_clock::now(),
                                            full_path_file);
        })
        .then([](auto sz) {
          LOG_THROW_IF(
            sz != ((100 /*key=50bytes, value=50bytes*/ + 24 /*header*/) * 2),
            "Has to have 2 payloads, bad size: {}", sz);
          return seastar::make_ready_future<int>(0);
        });
    });
  } catch (const std::exception &e) {
    std::cerr << "Fatal exception: " << e.what() << std::endl;
  }
}
