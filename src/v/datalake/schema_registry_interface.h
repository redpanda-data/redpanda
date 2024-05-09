#pragma once

#include "cloud_storage/types.h"
#include "pandaproxy/schema_registry/error.h"
#include "pandaproxy/schema_registry/exceptions.h"
#include "pandaproxy/schema_registry/types.h"
#include "wasm/schema_registry.h"

#include <seastar/core/future.hh>
#include <seastar/core/loop.hh>
#include <seastar/util/file.hh>

#include <exception>
#include <stdexcept>
#include <string>

namespace datalake {

/*
This interface solves two problems:
1. Dependency cycle between schema_registry, cluster, and datalake
2. Initialization order: partition manager is initialized before schema registry

partition_manager creates a schema_registry_reader shared pointer and passes
that to the individual partitions, where it is available to
ntp_archiver_service.

However, when partition_manager is initialized, the schema_registry isn't yet
intialized. Once the schema_registry is set up, it is passed in to the
schema_registry_reader via the set_schema_registry function on that class.
*/

// TODO: protobuf schemas can refer to other schemas via includes. We currently
// don't support recursively loading schemas.
struct schema_info {
    std::string schema;
};

class schema_registry_interface {
public:
    virtual ~schema_registry_interface() = default;
    virtual ss::future<schema_info> get_raw_topic_schema(std::string topic) = 0;
};

class dummy_schema_registry : public schema_registry_interface {
public:
    ss::future<schema_info>
    get_raw_topic_schema(std::string /* topic */) override {
        co_return schema_info{
          .schema = "",
        };
    }
};

} // namespace datalake
