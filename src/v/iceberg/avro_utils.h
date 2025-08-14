/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#pragma once

#include "bytes/iobuf.h"
#include "container/chunked_vector.h"

#include <seastar/core/temporary_buffer.hh>

#include <avro/DataFile.hh>
#include <avro/Stream.hh>

namespace iceberg {

// Near-identical implementation of avro::MemoryOutputStream, but backed by
// temporary buffers that callers can use to construct an iobuf.
class avro_iobuf_ostream : public avro::OutputStream {
public:
    using buf_container_t = chunked_vector<ss::temporary_buffer<char>>;
    explicit avro_iobuf_ostream(
      size_t chunk_size, buf_container_t* bufs, size_t* byte_count)
      : chunk_size_(chunk_size)
      , bufs_(bufs)
      , available_(0)
      , byte_count_(byte_count) {}
    ~avro_iobuf_ostream() override = default;

    // If there's no available space in the buffer, allocates `chunk_size_`
    // bytes at the end of the buffer.
    //
    // Returns the current position in the buffer, and the available remaining
    // space.
    bool next(uint8_t** data, size_t* len) final {
        if (available_ == 0) {
            bufs_->emplace_back(chunk_size_);
            available_ = chunk_size_;
        }
        auto& back_frag = bufs_->back();
        *data = reinterpret_cast<uint8_t*>(
          back_frag.share(chunk_size_ - available_, available_).get_write());
        *len = available_;
        *byte_count_ += available_;
        available_ = 0;
        return true;
    }

    void backup(size_t len) final {
        available_ += len;
        *byte_count_ -= len;
    }

    uint64_t byteCount() const final { return *byte_count_; }

    void flush() final {}

private:
    // Size in bytes with which to allocate new fragments.
    const size_t chunk_size_;

    buf_container_t* bufs_;
    size_t available_;

    // Total number of bytes.
    size_t* byte_count_;
};

// InputStream implementation that takes an iobuf as input.
// Iterates through the fragments of the given iobuf.
class avro_iobuf_istream : public avro::InputStream {
public:
    explicit avro_iobuf_istream(iobuf buf)
      : buf_(std::move(buf))
      , cur_frag_(buf_.begin())
      , cur_frag_pos_(0)
      , cur_pos_(0) {}

    // Returns the contiguous chunk of memory and the length of that memory.
    bool next(const uint8_t** data, size_t* len) final {
        if (cur_frag_ == buf_.end()) {
            return false;
        }
        auto left_in_frag = cur_frag_->size() - cur_frag_pos_;
        while (left_in_frag == 0) {
            ++cur_frag_;
            cur_frag_pos_ = 0;
            if (cur_frag_ == buf_.end()) {
                return false;
            }
            left_in_frag = cur_frag_->size();
        }
        *data = reinterpret_cast<const uint8_t*>(
          cur_frag_->share(cur_frag_pos_, left_in_frag).get());
        *len = left_in_frag;
        cur_frag_pos_ = cur_frag_->size();
        cur_pos_ += cur_frag_->size();
        return true;
    }

    // Allows for returning data in cur_frag_ *only*.
    void backup(size_t len) final {
        cur_pos_ -= len;
        if (cur_frag_ == buf_.end()) {
            throw std::runtime_error(
              "invalid `backup` call in avro_iobuf_istream after `next` has "
              "returned false");
        }
        if (cur_frag_pos_ < len) {
            throw std::runtime_error(fmt::format(
              "invalid `backup` call in avro_iobuf_istream - trying to backup "
              "more than the last call to `next` current_fragment_position: "
              "{}, backup_len: {}",
              cur_frag_pos_,
              len));
        }
        cur_frag_pos_ -= len;
    }

    void skip(size_t len) final {
        if (cur_frag_ == buf_.end()) {
            return;
        }
        cur_pos_ += len;
        while (cur_frag_->size() - cur_frag_pos_ > len) {
            len -= cur_frag_->size() - cur_frag_pos_;
            ++cur_frag_;
            if (cur_frag_ == buf_.end()) {
                return;
            }
            cur_frag_pos_ = 0;
        }
        cur_frag_pos_ += len;
    }

    size_t byteCount() const final { return cur_pos_; }

private:
    iobuf buf_;
    iobuf::iterator cur_frag_;
    size_t cur_frag_pos_;
    size_t cur_pos_;
};

} // namespace iceberg
