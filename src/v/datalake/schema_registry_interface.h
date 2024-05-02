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

// Todo: protobuf schemas can refer to other schemas via includes. We currently
// don't support this.
struct schema_info {
    std::string schema;
    std::string message_name;
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
          .message_name = "",
        };
    }
};

} // namespace datalake
