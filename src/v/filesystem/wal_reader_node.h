#pragma once
#include <memory>
#include <set>

#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sstring.hh>

#include "wal_disk_pager.h"
#include "wal_generated.h"
#include "wal_opts.h"
#include "wal_requests.h"

class wal_reader_node {
 public:
  SMF_DISALLOW_COPY_AND_ASSIGN(wal_reader_node);
  wal_reader_node(int64_t starting_file_epoch, int64_t term,
                  int64_t initial_size,
                  seastar::lowres_system_clock::time_point last_modified,
                  seastar::sstring filename);
  wal_reader_node(wal_reader_node &&) noexcept;
  virtual ~wal_reader_node();

  const int64_t starting_epoch;
  const int64_t term;
  const seastar::sstring filename;

  /// \brief flushes, truncates and if marked for deletion
  /// removes the file as well
  virtual seastar::future<> close();

  /// \brief opens the file & caches stat(2)
  virtual seastar::future<> open();

  /// \brief fill in the retval ptr with partial data that resides in this
  /// reader for *this* underlying file.
  /// Note: the read request *might* be larger than this file or only cover a
  /// range
  virtual seastar::future<> get(wal_read_reply *retval,
                                wal_read_request r) final;

  /// \brief returns the last time this file was modified, usually
  /// a cached value of stat('file').st_mtim
  inline virtual seastar::lowres_system_clock::time_point
  modified_time() const final {
    return last_modified_;
  }

  /// \brief the file as passed in or modified by \ref update_file_size()
  inline virtual int64_t
  file_size() const final {
    return file_size_;
  }

  /// \brief returns the max offset this file can handle
  inline virtual int64_t
  ending_epoch() const final {
    return starting_epoch + file_size_;
  }

  /// \brief marks the file so that upon successful close()
  /// it will also get deleted
  virtual void
  mark_for_deletion() final {
    marked_for_delete_ = true;
  }

  /// \brief hook for usually the last active-currently-being-writen-to
  /// log file - this is updated *only* after a flush so the reader can read
  /// up to the flush and require no extra caching
  virtual void update_file_size(int64_t newsize) final;

  /// \brief reads exactly *into* \dest_buf
  /// starting at \_offset
  /// and \_size number of bytes
  struct read_exactly_req {
    SMF_DISALLOW_COPY_AND_ASSIGN(read_exactly_req);
    read_exactly_req(uint8_t *dest_buf, int64_t rel_offset, int64_t _size)
      : relative_offset(rel_offset), size(_size), buf(dest_buf) {}
    read_exactly_req(read_exactly_req &&o) noexcept
      : relative_offset(o.relative_offset), size(o.size), buf(std::move(o.buf)),
        consumed(o.consumed) {}
    read_exactly_req &
    operator=(read_exactly_req &&o) noexcept {
      if (this != &o) {
        this->~read_exactly_req();
        new (this) read_exactly_req(std::move(o));
      }
      return *this;
    }

    /// \brief relative offset to `starting_epoch` of this file
    const int64_t relative_offset;

    /// \brief exact size to copy into @dest_buf
    const int64_t size;

    inline int64_t
    consume_offset() const {
      return relative_offset + consumed;
    }
    inline int64_t
    size_left_to_consume() const {
      return size - consumed;
    }
    inline uint8_t *
    dest() const {
      return buf + consumed;
    }
    /// \brief where the data goes
    uint8_t *buf;
    /// \brief how much of @size has been copied into @buf
    int64_t consumed{0};
  };

 private:
  seastar::future<> open_node();
  /// \brief - return buffer for offset with size
  seastar::future<> do_read(wal_read_reply *retval, wal_read_request r);
  /// \brief copies into the @read_exactly_req.buf data from the
  /// @wal_disk_pager
  seastar::future<seastar::stop_iteration>
  copy_exactly(read_exactly_req r, std::reference_wrapper<wal_disk_pager>);
  /// \brief returns the reply's current progress minus this epoch
  int64_t
  relative_offset(const wal_read_reply *r) const {
    return r->next_epoch() - starting_epoch;
  }

 protected:
  /// \brief used in a seastar::repeat loop
  /// copies one record from @wal_disk_pager into @retval
  ///
  /// Pipeline:
  ///    1. copy header - 16 bytes
  ///    2. verify header
  ///    3. copy payload
  ///    4. optionally verify payload
  virtual seastar::future<seastar::stop_iteration>
  copy_one_record(wal_read_reply *retval,
                  std::reference_wrapper<wal_disk_pager>) final;

  inline uint32_t
  global_file_id() const {
    return global_file_id_;
  }

  /// \brief needed as a shared ptr in that this file *might*
  /// go out of scope while still fetching a data from the page cache
  /// as we close the log / compact it or remove it.
  seastar::lw_shared_ptr<seastar::file> file_ = nullptr;
  int64_t file_size_{0};
  seastar::open_flags file_flags_ = seastar::open_flags::ro;

 private:
  /// \brief marks file for deletion
  bool marked_for_delete_{false};
  /// \brief last time modified
  seastar::lowres_system_clock::time_point last_modified_;
  /// \brief file_size_ / 4096
  int32_t number_of_pages_{0};
  /// \brief hash of inode (st_ino) and device id (st_dev)
  uint32_t global_file_id_{0};
  /// \brief reader gate, ensures that all reads finish before we exit the
  /// reader!
  seastar::gate rgate_;
};

