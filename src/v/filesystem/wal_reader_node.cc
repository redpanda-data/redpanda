#include "filesystem/wal_reader_node.h"

#include "filesystem/constants.h"
#include "filesystem/file_size_utils.h"
#include "filesystem/page_cache.h"
#include "filesystem/wal_disk_pager.h"
#include "filesystem/wal_pretty_print_utils.h"
#include "filesystem/wal_segment_record.h"
#include "hashing/jump_consistent_hash.h"
#include "hashing/xx.h"
#include "ioutil/priority_manager.h"

#include <seastar/core/byteorder.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/prefetch.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>

#include <smf/fbs_typed_buf.h>
#include <smf/human_bytes.h>
#include <smf/log.h>
#include <smf/macros.h>

#include <gsl/span>

#include <cstddef>
#include <cstdint>
#include <ctime>
#include <memory>
#include <system_error>
#include <utility>

constexpr const int32_t kReadPageSize = 4096;
// -- static helper funcs
//
inline int64_t round_up(int64_t file_size) {
    int64_t ret = file_size / kReadPageSize;
    if (file_size % kReadPageSize != 0) {
        ret += 1;
    }
    return ret;
}

inline wal_read_errno validate_header(const wal_header& hdr) {
    // checksum checks both key and value.
    if (hdr.magic() != kWalHeaderMagicNumber) {
        return wal_read_errno::wal_read_errno_invalid_magic;
    }
    if (hdr.version() != kWalHeaderVersion) {
        return wal_read_errno::wal_read_errno_invalid_version;
    }
    if (wal_segment_record::record_size(hdr) == 0) {
        // generic
        return wal_read_errno::wal_read_errno_invalid_header;
    }
    return wal_read_errno::wal_read_errno_none;
}

inline wal_read_errno
validate_payload(const wal_header& hdr, const uint8_t* buf, int32_t buf_size) {
    if (buf_size != wal_segment_record::record_size(hdr)) {
        return wal_read_errno::wal_read_errno_missmatching_header_payload_size;
    }
    if (
      wal_segment_record::checksum((const char*)buf, buf_size)
      != hdr.checksum()) {
        return wal_read_errno::wal_read_errno_missmatching_payload_checksum;
    }
    return wal_read_errno::wal_read_errno_none;
}

/// \brief returns bytes copied to \buf
///
/// There are 2 offsets. The consuming-from and the producing-to
/// one comes form the files, one is being advanced by this function
static int64_t copy_page_data(
  wal_reader_node::read_exactly_req& req,
  const page_cache_result* result,
  int32_t page_no) {
    const int64_t page_offset = req.consume_offset() % kReadPageSize;

    DLOG_THROW_IF(result == nullptr, "bad range");
    DLOG_TRACE(
      "relative_offset: {}, consume_offset: {}, page begin: {}, page: "
      "{}, disk_page bytes: {}, page_offset: {}",
      req.relative_offset,
      req.consume_offset(),
      page_no * kReadPageSize,
      page_no,
      result->data.size(),
      page_offset);

    const char* src = result->get_page_data(page_no) + page_offset;
    char* dst = reinterpret_cast<char*>(req.dest());

    std::ptrdiff_t ptdf = src - result->data.data();
    const int64_t step_size = std::min<int64_t>(
      result->data.size() - ptdf, req.size_left_to_consume());

    DLOG_TRACE(
      "page_offset: {}, step_size: {}, space_left_to_consume(): {}, "
      "disk_page.size: {}",
      page_offset,
      step_size,
      req.size_left_to_consume(),
      result->data.size());

    if (step_size == 0) {
        // nothing left to copy
        LOG_WARN("step_size to copy is 0. invalid memcpy");
        return 0;
    }
    std::memcpy(dst, src, step_size);
    // we aren't yet at the end, prefetch the next byte kReadPageSize
    if (ptdf - step_size > 0) {
        prefetch<const char, 0>(src + step_size + 1);
    }
    return step_size;
}

// -- wal_reader_node impl
//

wal_reader_node::wal_reader_node(
  int64_t epoch,
  int64_t term_id,
  int64_t initial_size,
  lowres_system_clock::time_point modified,
  sstring name)
  // needed signed for comparisons
  : starting_epoch(epoch)
  , term(term_id)
  , filename(name)
  , last_modified_(modified) {
    file_size_ = initial_size;
    number_of_pages_ = round_up(file_size_);
}

wal_reader_node::wal_reader_node(wal_reader_node&& o) noexcept
  : starting_epoch(o.starting_epoch)
  , term(o.term)
  , filename(std::move(o.filename))
  , _file(std::move(o._file))
  , file_size_(o.file_size_)
  , file_flags_(o.file_flags_)
  , marked_for_delete_(o.marked_for_delete_)
  , last_modified_(std::move(o.last_modified_))
  , number_of_pages_(o.number_of_pages_)
  , global_file_id_(o.global_file_id_)
  , _rgate(std::move(o._rgate)) {
}

wal_reader_node::~wal_reader_node() {
}

future<> wal_reader_node::close() {
    auto f = _file;
    auto del = marked_for_delete_;
    auto name = filename;
    auto fileid = global_file_id_;
    return _rgate.close().then([fileid, f, del, name] {
        return f->close().finally([fileid, f, del, name] {
            if (del) {
                LOG_INFO("Removing file: {}, id:{} from cache", name, fileid);
                page_cache::get().remove_file(fileid).then([name] {
                    LOG_INFO("Removing file from filesystem: {}", name);
                    return remove_file(name);
                });
            }
            return make_ready_future<>();
        });
    });
}

future<> wal_reader_node::open_node() {
    return open_file_dma(filename, file_flags_)
      .then([this](file ff) {
          _file = make_lw_shared<file>(std::move(ff));
          return make_ready_future<>();
      });
}

void wal_reader_node::update_file_size(int64_t newsize) {
    // size must be monotonically increasing
    if (newsize <= file_size_)
        return;

    const auto old_fs_sz = file_size_;
    last_modified_ = lowres_system_clock::now();
    file_size_ = newsize;

    if (_file) {
        std::set<int32_t> pages_to_evict;
        number_of_pages_ = round_up(file_size_);
        for (auto i = old_fs_sz; i < file_size_; i += kReadPageSize) {
            auto page = offset_to_page(i, kReadPageSize);
            // The reason this is needed, is that we are partially writing
            // the tailing page, at times multiple times
            pages_to_evict.insert(page);
        }
        if (!pages_to_evict.empty()) {
            page_cache::get().evict_pages(
              global_file_id_, std::move(pages_to_evict));
        }
    }
}
future<> wal_reader_node::open() {
    return open_node().then([this] {
        // must happen after actual read
        update_file_size(file_size_);
        return _file->stat().then([this](auto stat) {
            // https://www.gnu.org/software/libc/manual/html_node/Attribute-Meanings.html
            // inode and device id create a globally unique file id
            global_file_id_ = xxhash_32(std::array{stat.st_ino, stat.st_dev});
            DLOG_TRACE(
              "{}: inode: {}, device: {}, file_id: {}",
              filename,
              stat.st_ino,
              stat.st_dev,
              global_file_id_);
        });
    });
}

future<>
wal_reader_node::get(wal_read_reply* retval, wal_read_request r) {
    DLOG_THROW_IF(retval == nullptr, "Cannot return data for null");
    auto offset = retval->next_epoch();
    if (offset >= starting_epoch && offset < ending_epoch()) {
        return with_gate(
          _rgate, [this, retval, r] { return do_read(retval, std::move(r)); });
    }
    LOG_ERROR_IF(
      retval->on_disk_size() != 0,
      "{}: Offset out of range. file size: {}, request On disk "
      "size:{} offset {} is >= {} or < {}",
      filename,
      file_size_,
      retval->on_disk_size(),
      offset,
      ending_epoch(),
      starting_epoch);
    retval->set_error(wal_read_errno::wal_read_errno_invalid_offset);
    return make_ready_future<>();
}

future<stop_iteration> wal_reader_node::copy_one_record(
  wal_read_reply* retval, std::reference_wrapper<wal_disk_pager> pager) {
    using stop_t = stop_iteration;

    auto roffset = relative_offset(retval);
    auto record = std::make_unique<wal_binary_recordT>();
    auto ptr = record.get();
    // 1. copy the header
    ptr->data.resize(kWalHeaderSize);
    return copy_exactly(
             read_exactly_req(ptr->data.data(), roffset, kWalHeaderSize), pager)
      .then([this, retval, ptr, pager, roffset](auto iter) {
          if (iter == stop_t::yes) {
              return make_ready_future<stop_t>(iter);
          }
          // validate header
          auto hdr = wal_header();
          std::memcpy(&hdr, ptr->data.data(), sizeof(hdr));
          auto err = validate_header(hdr);
          if (err != wal_read_errno::wal_read_errno_none) {
              retval->set_error(err);
              DLOG_TRACE("Bad header: {}", EnumNamewal_read_errno(err));
              return make_ready_future<stop_t>(stop_t::yes);
          }
          // 2. copy the body
          auto bodysz = wal_segment_record::record_size(hdr);
          ptr->data.resize(kWalHeaderSize + bodysz);
          return copy_exactly(
            read_exactly_req(
              ptr->data.data() + kWalHeaderSize,
              roffset + kWalHeaderSize,
              bodysz),
            pager);
      })
      .then([ptr, retval, record = std::move(record)](auto iter) mutable {
          if (iter == stop_t::yes) {
              return make_ready_future<stop_t>(iter);
          }
          if (retval->validate_checksum) {
              auto hdr = wal_header();
              std::memcpy(&hdr, ptr->data.data(), kWalHeaderSize);
              auto bodysz = wal_segment_record::record_size(hdr);
              // 3. validate payload
              auto err = validate_payload(
                hdr, ptr->data.data() + kWalHeaderSize, bodysz);
              if (err != wal_read_errno::wal_read_errno_none) {
                  retval->set_error(err);
                  DLOG_TRACE("Bad payload: {}", EnumNamewal_read_errno(err));
                  return make_ready_future<stop_t>(stop_t::yes);
              }
          }
          // 4. success
          retval->add_record(std::move(record));
          // keep going!
          return make_ready_future<stop_t>(stop_t::no);
      });
}

future<stop_iteration> wal_reader_node::copy_exactly(
  wal_reader_node::read_exactly_req req,
  std::reference_wrapper<wal_disk_pager> pager) {
    using stop_t = stop_iteration;
    // check if we can do the fast path - stack copy right now
    auto pno = offset_to_page(req.consume_offset(), kReadPageSize);
    if (pager.get().is_page_in_result_range(pno)) {
        auto copied_bytes = copy_page_data(req, pager.get().range(), pno);
        if (copied_bytes == 0) {
            return make_ready_future<stop_t>(stop_t::yes);
        }
        req.consumed += copied_bytes;
        if (req.size_left_to_consume() <= 0) {
            return make_ready_future<stop_t>(stop_t::no);
        }
    }

    return do_with(std::move(req), [pager](auto& r) {
        return repeat([&r, pager]() mutable {
                   if (r.size_left_to_consume() <= 0) {
                       return make_ready_future<stop_t>(stop_t::yes);
                   }
                   auto pageno = offset_to_page(
                     r.consume_offset(), kReadPageSize);
                   return pager.get().fetch(pageno).then([&r, pager, pageno](
                                                           auto range) mutable {
                       DLOG_THROW_IF(
                         !range,
                         "This means that the caller asked for out of bounds");
                       DLOG_THROW_IF(
                         !range->is_page_in_range(pageno),
                         "Page({}) not in range: {}. req:{}",
                         pageno,
                         *range,
                         pager.get().request());
                       auto copied_bytes = copy_page_data(r, range, pageno);
                       if (copied_bytes == 0) {
                           return make_ready_future<stop_t>(
                             stop_t::yes);
                       }
                       r.consumed += copied_bytes;
                       return make_ready_future<stop_t>(stop_t::no);
                   });
               })
          .then([&r] {
              DLOG_TRACE(
                "ReadExactly: left to consume: {}. Copied bytes: {}",
                r.size_left_to_consume(),
                r.consumed);
              // Here we have to invert the logic above.
              // We do not want our parents to stop reading unless
              // we had encountered an error and we couldn't copy what we
              // promised
              if (r.size_left_to_consume() <= 0) {
                  return make_ready_future<stop_t>(stop_t::no);
              }
              return make_ready_future<stop_t>(stop_t::yes);
          });
    });
}

future<>
wal_reader_node::do_read(wal_read_reply* retval, wal_read_request r) {
    auto roffset = relative_offset(retval);
    if (roffset < 0) {
        LOG_ERROR(
          "offset: {} is below bounds ({}-{}]",
          retval->next_epoch(),
          starting_epoch,
          ending_epoch());
        return make_ready_future<>();
    }
    auto remaining_req_size = r.req->max_bytes() - retval->on_disk_size();
    if (__builtin_expect(remaining_req_size <= 0, false)) {
        LOG_WARN("Asked to read data but request is already fullfilled");
        return make_ready_future<>();
    }
    if (remaining_req_size > file_size_) {
        remaining_req_size -= remaining_req_size % file_size_;
    }
    return do_with(
      wal_disk_pager(page_cache_request{
        global_file_id_,
        offset_to_page(roffset, kReadPageSize),
        offset_to_page(roffset + remaining_req_size, kReadPageSize),
        number_of_pages_,
        _file,
        priority_manager::get().streaming_read_priority()}),
      [this,
       retval,
       remaining_req_size,
       max_bytes = r.req->max_bytes(),
       initial_size = retval->on_disk_size()](wal_disk_pager& real_pager) {
          std::reference_wrapper<wal_disk_pager> pager(real_pager);
          return repeat([=] {
              using stop_t = stop_iteration;
              if (retval->on_disk_size() - initial_size >= remaining_req_size) {
                  return make_ready_future<stop_t>(stop_t::yes);
              }
              if (
                retval->next_epoch() < starting_epoch
                || retval->next_epoch() >= ending_epoch()) {
                  return make_ready_future<stop_t>(stop_t::yes);
              }
              return copy_one_record(retval, pager);
          });
      });
}
