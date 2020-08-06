#pragma once
#include "model/fundamental.h"

#include <system_error>

namespace model {

std::error_code validate_kafka_topic_name(const model::topic&);

} // namespace model
