#pragma once

#include <seastar/core/temporary_buffer.hh>

#include "chain_replication.smf.fb.h"

namespace smf {

/// \brief smf specialization point
/// reserved the size of type
/// To read the default of 1MB of data, there is a *mesured* 80KB of overhead
/// due to struct padding / alignment / etc from the raw size of in memory data
seastar::temporary_buffer<char>
native_table_as_buffer(const v::chains::chain_get_replyT &r);

}  // namespace smf
