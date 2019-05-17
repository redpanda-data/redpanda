#include "wal_writer_utils.h"

#include <chrono>
#include <unistd.h>

#include <fmt/format.h>
#include <seastar/core/align.hh>
#include <smf/log.h>
#include <smf/time_utils.h>

#include "ioutil/priority_manager.h"

#include "file_size_utils.h"
#include "wal_pretty_print_utils.h"
#include "wal_reader_node.h"
#include "wal_segment_record.h"


int64_t
system_page_size() {
  static const int64_t pz = ::sysconf(_SC_PAGESIZE);
  return pz;
}

int64_t
wal_file_size_aligned() {
  static const constexpr int64_t kDefaultFileSize =
    static_cast<int64_t>(std::numeric_limits<int>::max()) / 2;

  static const int64_t segment_size =
    seastar::align_up(kDefaultFileSize, system_page_size());
  return segment_size;
}

seastar::sstring
wal_file_name(const seastar::sstring &work_dir, int64_t epoch, int64_t term) {
  DLOG_THROW_IF(work_dir[work_dir.size() - 1] == '/',
                "Work dirrectory cannot end in /");
  return fmt::format("{}/{}.{}.wal", work_dir, epoch, term);
}

seastar::future<std::pair<int64_t, struct stat>>
file_size_from_allocated_blocks(seastar::sstring pathname) {
  return seastar::open_file_dma(pathname, seastar::open_flags::ro)
    .then([](seastar::file ff) {
      auto f = seastar::make_lw_shared<seastar::file>(std::move(ff));
      // actually seek to the end of the file vs stat() which returns the
      // fdatasync and not the actual number of blocks allocated
      return f->stat().then([f](auto st) {
        int64_t sz = st.st_blocks * f->disk_read_dma_alignment();
        return f->close().then([f /*kept to ommit finally()*/, sz, st] {
          std::pair<int64_t, struct stat> p(sz, st);
          return seastar::make_ready_future<decltype(p)>(std::move(p));
        });
      });
    });
}

class wal_segment_healer final : public wal_reader_node {
 public:
  wal_segment_healer(int64_t epoch, int64_t term, int64_t sz,
                     seastar::lowres_system_clock::time_point modified,
                     seastar::sstring filename)
    : wal_reader_node(epoch, term, sz, modified, filename) {
    file_flags_ = file_flags_ | seastar::open_flags::rw;
  }
  virtual ~wal_segment_healer(){};

  seastar::future<>
  heal() {
    using stop_t = seastar::stop_iteration;
    return truncate_to_max_metadata_size()
      .then([this] {
        if (file_size_ == 0) {
          exit_recovery_ = true;
          is_recovered_ = true;
          return seastar::make_ready_future<>();
        }
        return seastar::do_with(
                 create_pager(),
                 [this](wal_disk_pager &p) {
                   auto const id = global_file_id();
                   auto pager = std::ref(p);
                   return seastar::repeat([this, pager, id]() mutable {
                     DLOG_TRACE("Recovering... file {} offset {}", id,
                                last_valid_offset_);
                     if (last_valid_offset_ >= file_size_) {
                       return seastar::make_ready_future<stop_t>(stop_t::yes);
                     }
                     auto idx = std::make_unique<wal_read_reply>(
                       0 /*ns*/, 0 /*topic*/, 0 /*partition*/,
                       starting_epoch + last_valid_offset_,
                       true /*validate offsets*/);
                     auto ptr = idx.get();

                     return copy_one_record(ptr, pager)
                       .then([this, idx = std::move(idx)](auto next) {
                         last_valid_offset_ = idx->next_epoch();
                         return seastar::make_ready_future<stop_t>(next);
                       })
                       .handle_exception([](auto eptr) {
                         LOG_INFO("Recovering exception...: {}", eptr);
                         return seastar::make_ready_future<stop_t>(stop_t::yes);
                       });
                   });
                 })
          .then([this] {
            update_recovery_state();
            return truncate_to_valid_offset();
          });
      })
      .handle_exception([this](auto eptr) {
        LOG_ERROR("Failed to recover {} : {}", filename, eptr);
        is_recovered_ = false;
        return seastar::make_ready_future<>();
      });
  }
  bool
  is_recovered() const {
    return is_recovered_;
  }
  bool
  is_empty() const {
    return file_size_ == 0;
  }

 private:
  inline wal_disk_pager
  create_pager() {
    int32_t first_page = 0;
    int32_t last_page = offset_to_page(file_size_, 4096);
    return wal_disk_pager(
      page_cache_request{global_file_id(), first_page, last_page, last_page,
                         file_, seastar::default_priority_class()});
  }

  seastar::future<>
  truncate_to_max_metadata_size() {
    return file_->size().then([this](auto registered_size) {
      file_size_ = registered_size;
      // This case covers a file simply fallocated and *NEVER* written
      return file_->truncate(registered_size);
    });
  }

  void
  update_recovery_state() {
    exit_recovery_ = true;
    if (last_valid_offset_ != 0) {
      is_recovered_ = true;
      file_size_ = last_valid_offset_;
    }
  }
  seastar::future<>
  truncate_to_valid_offset() {
    if (last_valid_offset_ == 0) { return seastar::make_ready_future<>(); }
    file_size_ = last_valid_offset_;
    DLOG_TRACE("Recovered file to offset:{}", last_valid_offset_);
    return file_->truncate(last_valid_offset_);
  }

  int64_t last_valid_offset_{0};
  bool is_recovered_{false};
  bool exit_recovery_{false};
};

seastar::future<int64_t>
recover_failed_wal_file(int64_t epoch, int64_t term, int64_t sz,
                        seastar::lowres_system_clock::time_point modified,
                        seastar::sstring name) {
  auto n =
    seastar::make_shared<wal_segment_healer>(epoch, term, sz, modified, name);
  return n->open().then([n] {
    return n->heal().then([n] {
      int64_t real_size = 0;
      bool should_rename = false;
      if (!n->is_recovered()) {
        LOG_ERROR("Failed to recover file: {}, artificially returning size 0",
                  n->filename);
        should_rename = true;
        real_size = 0;
      } else {
        real_size = n->file_size();
        if (n->is_empty()) {
          // deletes after closing
          n->mark_for_deletion();
        }
      }
      return n->close().then([n /*kept to ommit finally()*/, name = n->filename,
                              should_rename, real_size] {
        if (should_rename) {
          LOG_INFO("Renaming file: {} to {}.cannotrecover", name, name);
          seastar::rename_file(name, name + ".cannotrecover").then([real_size] {
            return seastar::make_ready_future<int64_t>(real_size);
          });
        }
        return seastar::make_ready_future<int64_t>(real_size);
      });
    });
  });
}

