#pragma once
#include "seastarx.h"

#include <seastar/core/reactor.hh>

// manage SMP scheduling groups. These scheduling groups are global, so one
// instance of this class can be created at the top level and passed down into
// any server and any shard that needs to schedule continuations into a given
// group.
class smp_groups {
public:
    smp_groups() {
    }
    future<> create_groups() {
        smp_service_group_config smp_sg_config;
        smp_sg_config.max_nonlocal_requests = 5000;

        return create_smp_service_group(smp_sg_config)
          .then([this](smp_service_group sg) {
              _raft = std::make_unique<smp_service_group>(std::move(sg));
          })
          .then(
            [smp_sg_config] { return create_smp_service_group(smp_sg_config); })
          .then([this](smp_service_group sg) {
              _kafka = std::make_unique<smp_service_group>(std::move(sg));
          });
    }
    smp_service_group raft_smp_sg() {
        return *_raft;
    }
    smp_service_group kafka_smp_sg() {
        return *_kafka;
    }

    future<> destroy_groups() {
        return destroy_smp_service_group(*_kafka).then(
          [this] { return destroy_smp_service_group(*_raft); });
    }

private:
    std::unique_ptr<smp_service_group> _raft;
    std::unique_ptr<smp_service_group> _kafka;
};
