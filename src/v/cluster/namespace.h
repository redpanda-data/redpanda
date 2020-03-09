#pragma once
#include "model/fundamental.h"

namespace cluster {

inline const model::ntp controller_ntp{
  model::ns("redpanda"),
  model::topic_partition{
    model::topic("controller"),
    model::partition_id(0),
  },
};

inline const model::ns kafka_namespace("kafka");

} // namespace cluster
