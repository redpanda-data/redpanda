#include "wal_nstpidx_repair.h"

#include <smf/log.h>

// filesystem
#include "directory_walker.h"
#include "wal_name_extractor_utils.h"
#include "wal_writer_utils.h"

using set_t = wal_nstpidx_repair::set_t;
static inline seastar::lowres_system_clock::time_point
from_timespec(const struct timespec& ts) {
    namespace c = std::chrono; // NOLINT
    using sc = seastar::lowres_system_clock;
    auto d = c::seconds{ts.tv_sec} + c::nanoseconds{ts.tv_nsec};
    sc::time_point tp{c::duration_cast<sc::duration>(d)};
    return tp;
}

seastar::future<set_t> wal_nstpidx_repair::repair(seastar::sstring dir) {
    // private ctor
    std::unique_ptr<wal_nstpidx_repair> rindexer(new wal_nstpidx_repair(dir));
    return seastar::do_with(std::move(rindexer), [dir](auto& indexer) {
        auto idx = indexer.get();
        return directory_walker::walk(
                 dir,
                 [idx](seastar::directory_entry de) {
                     return idx->visit(std::move(de));
                 })
          .then([&indexer] {
              return seastar::make_ready_future<set_t>(
                std::move(indexer->_files));
          });
    });
};

seastar::future<> wal_nstpidx_repair::visit(seastar::directory_entry de) {
    if (!wal_name_extractor_utils::is_wal_segment(de.name)) {
        return seastar::make_ready_future<>();
    }

    auto [epoch, term]
      = wal_name_extractor_utils::wal_segment_extract_epoch_term(de.name);
    auto full_path = work_dir + "/" + de.name;

    return file_size_from_allocated_blocks(full_path).then(
      [=, epoch = epoch, term = term](auto p) mutable {
          auto [block_size, st] = p;
          DLOG_TRACE(
            "{} block size: {} vs aligned: {}",
            full_path,
            block_size,
            wal_file_size_aligned());
          if (st.st_size == 0) {
              LOG_INFO("Removing empty log file: {}", full_path);
              return seastar::remove_file(full_path);
          } else if (block_size >= wal_file_size_aligned()) {
              LOG_INFO(
                "Recovering unsafe log-segment: {}, fallocated-size: {} ",
                block_size,
                full_path);
              return recover_failed_wal_file(
                       epoch,
                       term,
                       st.st_size,
                       from_timespec(st.st_mtim),
                       full_path)
                .then([=, st = st](auto sz) mutable {
                    if (sz != 0) {
                        st.st_size = sz;
                        _files.insert(
                          wal_nstpidx_repair::item{epoch,
                                                   term,
                                                   st.st_size,
                                                   de.name,
                                                   from_timespec(st.st_mtim)});
                    }
                    return seastar::make_ready_future<>();
                });
          }
          _files.insert(wal_nstpidx_repair::item{
            epoch, term, st.st_size, de.name, from_timespec(st.st_mtim)});
          return seastar::make_ready_future<>();
      });
}
