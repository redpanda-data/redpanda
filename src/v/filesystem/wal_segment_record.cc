#include "wal_segment_record.h"

#include <sys/sdt.h>

#include <smf/compression.h>
#include <smf/log.h>
#include <smf/macros.h>
#include <smf/time_utils.h>

#include "hashing/jump_consistent_hash.h"
#include "hashing/xx.h"
#include "wal_generated.h"

namespace v {
/// \brief useful for failure recovery of invalid log segments
static constexpr int16_t kWalHeaderMagicNumber = 0xcafe;
static constexpr int8_t kWalHeaderVersion = 1;
// stateless compressors

static thread_local auto lz4 = smf::codec::make_unique(
  smf::codec_type::lz4, smf::compression_level::fastest);

static thread_local auto zstd = smf::codec::make_unique(
  smf::codec_type::zstd, smf::compression_level::fastest);

static constexpr int32_t kWalHeaderSize = sizeof(wal_header);

int32_t
wal_segment_record::checksum(const char *p, std::size_t s) {
  return std::numeric_limits<int32_t>::max() & xxhash_64(p, s);
}

wal_header
wal_segment_record::header_for(const char *key, int32_t key_size,
                               const char *val, int32_t val_size,
                               wal_compression_type ctype) {
  wal_header hdr;
  hdr.mutate_version(kWalHeaderVersion);
  hdr.mutate_magic(kWalHeaderMagicNumber);
  hdr.mutate_compression(ctype);
  hdr.mutate_key_size(key_size);
  hdr.mutate_value_size(val_size);
  hdr.mutate_time_millis(smf::lowres_time_now_millis());
  incremental_xxhash64 incx;
  incx.update(key, key_size);
  incx.update(val, val_size);
  hdr.mutate_checksum(std::numeric_limits<int32_t>::max() & incx.digest());
  return hdr;
}

std::unique_ptr<wal_binary_recordT>
wal_segment_record::coalesce(const char *key, int32_t key_size, const char *val,
                             int32_t val_size, wal_compression_type ctype) {
  DTRACE_PROBE(redpanda, wal_segment_record_coalesce);

  seastar::temporary_buffer<char> tmp(0);
  // 2. copy value - compress if required and reset size
  if (ctype == wal_compression_type::wal_compression_type_lz4) {
    DTRACE_PROBE(redpanda, wal_segment_record_coalesce_lz4);
    tmp = lz4->compress(val, val_size);
  } else if (ctype == wal_compression_type::wal_compression_type_zstd) {
    DTRACE_PROBE(redpanda, wal_segment_record_coalesce_zstd);
    tmp = zstd->compress(val, val_size);
  }

  // allocate once! and zero out memory - use memcpy after this line
  //
  auto retval = std::make_unique<wal_binary_recordT>();
  const char *real_val = tmp.size() == 0 ? val : tmp.get();

  if (tmp.size() == 0) {
    retval->data.resize(kWalHeaderSize + key_size + val_size);
  } else {
    retval->data.resize(kWalHeaderSize + key_size + tmp.size());
    real_val = tmp.get_write();
    val_size = tmp.size();
  }

  // copy the key
  std::memcpy(retval->data.data() + kWalHeaderSize, key, key_size);
  // copy the value
  std::memcpy(retval->data.data() + kWalHeaderSize + key_size, real_val,
              val_size);

  auto hdr =
    wal_segment_record::header_for(key, key_size, real_val, val_size, ctype);

  // 4. copy the header
  std::memcpy(retval->data.data(), (char *)&hdr, kWalHeaderSize);
  return std::move(retval);
}

std::pair<seastar::temporary_buffer<char>, seastar::temporary_buffer<char>>
wal_segment_record::extract_from_bin(const char *begin, int32_t sz) {
  // copy the key
  wal_header hdr;
  std::memcpy(&hdr, begin, kWalHeaderSize);
  auto full_size = wal_segment_record::record_size(hdr);
  LOG_THROW_IF(
    full_size + kWalHeaderSize != sz,
    "passed in an incomplete buffer, required: {}, passed in a size of: {}",
    full_size + kWalHeaderSize, sz);
  seastar::temporary_buffer<char> keybuf(hdr.key_size());
  // copy the key
  std::memcpy(keybuf.get_write(), begin + kWalHeaderSize, keybuf.size());

  // copy the value
  seastar::temporary_buffer<char> valbuf(hdr.value_size());
  if (hdr.compression() == wal_compression_type::wal_compression_type_lz4) {
    valbuf =
      lz4->uncompress(begin + kWalHeaderSize + keybuf.size(), hdr.value_size());
  } else if (hdr.compression() ==
             wal_compression_type::wal_compression_type_zstd) {
    valbuf = zstd->uncompress(begin + kWalHeaderSize + keybuf.size(),
                              hdr.value_size());
  } else {
    // copy the val
    std::memcpy(valbuf.get_write(), begin + kWalHeaderSize + keybuf.size(),
                valbuf.size());
  }
  return {std::move(keybuf), std::move(valbuf)};
}

}  // namespace v
