#pragma once

#include "seastarx.h"

#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sstring.hh>

#include <sys/stat.h>
#include <sys/types.h>

#include <unistd.h>

/// \brief alias ::sysconf(_SC_PAGESIZE)
int64_t system_page_size();

/// \brief return 64MB aligned to ::sysconf(_SC_PAGESIZE)
int64_t wal_file_size_aligned();

/// \brief return write ahead name w/ given prefix and current epoch
sstring
wal_file_name(const sstring& directory, int64_t epoch, int64_t term);

/// \brief performs failure recovery of log segments
/// \returns actual size
future<int64_t> recover_failed_wal_file(
  int64_t epoch,
  int64_t term,
  int64_t sz,
  lowres_system_clock::time_point modified,
  sstring name);

/// \brief returns the number of blocks allocated for this file
/// - needed because fallocate only touches blocks not the contents
/// so stat(file).st_size is not the correct size for fallocated files
///
future<std::pair<int64_t, struct stat>>
file_size_from_allocated_blocks(sstring file_name);
