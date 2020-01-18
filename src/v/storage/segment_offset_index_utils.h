#include "storage/segment_offset_index.h"

#include <seastar/core/temporary_buffer.hh>

#include <vector>

namespace storage {

std::vector<std::pair<uint32_t, uint32_t>>
offset_index_from_buf(ss::temporary_buffer<char> buf);

ss::temporary_buffer<char>
offset_index_to_buf(const std::vector<std::pair<uint32_t, uint32_t>>& v);

} // namespace storage
