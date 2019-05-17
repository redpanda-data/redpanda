#include "wal_segment_indexer.h"

#include <seastar/core/sstring.hh>
#include <seastar/core/temporary_buffer.hh>
#include <smf/macros.h>
#include <smf/native_type_utils.h>

#include "wal_segment_record.h"


struct key_hash_functor {
  bool
  operator()(const uint64_t &hash,
             const std::unique_ptr<wal_segment_index_key_entryT> &e) const {
    return hash < e->hash;
  }
  bool
  operator()(const std::unique_ptr<wal_segment_index_key_entryT> &e,
             const uint64_t &hash) const {
    return e->hash < hash;
  }
};

static std::unique_ptr<wal_binary_recordT>
binary_index(uint64_t xorkey, int64_t largest_offset, int64_t lens_bytes,
             wal_segment_indexer::wal_segment_index_key_entry_set &set) {
  // record that is going to go in the indexed WAL
  wal_segment_index_fragmentT frag;
  frag.largest_offset = largest_offset;
  frag.lens_bytes = lens_bytes;
  frag.keys.reserve(set.size());
  while (!set.empty()) {
    std::unique_ptr<wal_segment_index_key_entryT> tmp =
      std::move(set.extract(set.begin()).value());
    frag.keys.push_back(std::move(tmp));
  }

  seastar::temporary_buffer<char> buf =
    smf::native_table_as_buffer<wal_segment_index_fragment>(frag);

  seastar::sstring key = seastar::to_sstring(xorkey);
  auto ret = wal_segment_record::coalesce(
    key.data(), key.size(), buf.get(), buf.size(),
    wal_compression_type::wal_compression_type_lz4);

  return ret;
}

std::pair<wal_segment_index_key_entryT *, bool>
wal_segment_indexer::get_or_create(int64_t offset, const wal_binary_record *r) {
  const char *record_begin = reinterpret_cast<const char *>(r->data()->Data());
  wal_header hdr;
  std::memcpy(&hdr, record_begin, kWalHeaderSize);
  const char *key_begin = record_begin + kWalHeaderSize;
  auto xx = xxhash_64(key_begin, hdr.key_size());

  if (auto it =
        std::lower_bound(data_.begin(), data_.end(), xx, key_hash_functor{});
      it != data_.end()) {
    const int32_t key_size = it->get()->key.size();
    if (SMF_UNLIKELY(
          (key_size != hdr.key_size() ||
           std::strncmp(key_begin,
                        reinterpret_cast<const char *>((*it)->key.data()),
                        hdr.key_size()) != 0))) {
      // Worst case O(2 * ln N)
      // 1) In this case we pay the cost of 2 lookups. The first one which was
      //    done via hashes above
      // 2) This one will then disambiguate via strncmp *only* in the case the
      //    hashes match
      // We accept this complexity because we don't want to allocate key space
      // for every record
      //
      auto tmp = std::make_unique<wal_segment_index_key_entryT>();
      tmp->offset = offset;
      tmp->hash = xx;
      tmp->key.resize(hdr.key_size());
      std::memcpy(tmp->key.data(), key_begin, hdr.key_size());
      it = data_.find(tmp);
      if (it == data_.end()) {
        // we didn't find it on the second index
        auto ptr = tmp.get();
        data_.insert(std::move(tmp));
        return {ptr, true};
      }
    }
    return {it->get(), false};
  }

  // we didn't find it
  auto tmp = std::make_unique<wal_segment_index_key_entryT>();
  tmp->offset = offset;
  tmp->hash = xx;
  tmp->key.resize(hdr.key_size());
  std::memcpy(tmp->key.data(), key_begin, hdr.key_size());
  auto ptr = tmp.get();
  data_.insert(std::move(tmp));
  return {ptr, true};
}

seastar::future<>
wal_segment_indexer::index(int64_t offset, const wal_binary_record *r) {
  is_flushed_ = false;
  largest_offset_seen_ = std::max(offset, largest_offset_seen_);
  auto [ptr, created] = get_or_create(offset, r);
  if (created) {
    size_ += ptr->key.size();
    xorwalkey ^= ptr->hash;
    lens_bytes_ += r->data()->size();
  } else {
    ptr->offset = std::max(ptr->offset, offset);
  }
  // make sure to rotate if we are full
  if (size_ < kMaxPartialIndexKeysSize) {
    return seastar::make_ready_future<>();
  }
  return flush_index();
}

void
wal_segment_indexer::reset() {
  // fully reset
  size_ = 0;
  xorwalkey = 0;
  lens_bytes_ = 0;
  data_.clear();
}
seastar::future<>
wal_segment_indexer::flush_index() {
  if (is_flushed_) { return seastar::make_ready_future<>(); }
  auto record =
    binary_index(xorwalkey, largest_offset_seen_, lens_bytes_, data_);
  // after we drain it into tmp vars, we can reset
  reset();

  // write the log entry
  auto tmp = record.get();
  auto src = reinterpret_cast<const char *>(tmp->data.data());
  return index_->append(src, tmp->data.size())
    .finally([this, r = std::move(record)] { is_flushed_ = true; });
}
seastar::future<>
wal_segment_indexer::close() {
  if (index_) {
    return flush_index().then([this] { return index_->close(); });
  }
  return seastar::make_ready_future<>();
}
seastar::future<>
wal_segment_indexer::open() {
  index_ = std::make_unique<wal_segment>(filename, priority,
                                         wopts.max_log_segment_size,
                                         wopts.max_bytes_in_writer_cache);
  return index_->open();
}

