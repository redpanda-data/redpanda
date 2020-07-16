#pragma once

#include "utils/named_type.h"

namespace model {

using record_batch_type = named_type<int8_t, struct model_record_batch_type>;

constexpr std::array<record_batch_type, 8> well_known_record_batch_types{
  record_batch_type(),  // unknown - used for debugging
  record_batch_type(1), // raft::data
  record_batch_type(2), // raft::configuration
  record_batch_type(3), // controller::*
  record_batch_type(4), // kvstore::*
  record_batch_type(5), // checkpoint - used to achieve linearizable reads
  record_batch_type(6), // controller topic command batch type
  record_batch_type(7), // ghost - used to fill gaps in raft recovery
};
} // namespace model