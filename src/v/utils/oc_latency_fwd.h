#pragma once

#include "base/seastarx.h"
#include "base/vassert.h"
#include "utils/log_hist.h"

struct oc_tracker;

using shared_tracker = std::shared_ptr<oc_tracker>;

using tracker_vector = std::vector<shared_tracker>;

void record(const tracker_vector& tv, const char* what);
