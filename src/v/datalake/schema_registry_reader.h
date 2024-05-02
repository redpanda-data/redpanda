#pragma once
#include "datalake/schema_registry_interface.h"

namespace datalake {

class schema_registry_reader : public schema_registry_interface {
public:
    explicit schema_registry_reader() {}

    void set_schema_registry(
      pandaproxy::schema_registry::api* pandaproxy_schema_registry) {
        _pandaproxy_schema_registry = pandaproxy_schema_registry;
        _schema_registry = wasm::schema_registry::make_default(
          pandaproxy_schema_registry);
        _initialized = true;
    }

    ss::future<schema_info>
    get_raw_topic_schema(std::string topic_name) override {
        schema_info default_return_value = {
          .schema = "",
          .message_name = "",
        };

        if (!_initialized) {
            co_return default_return_value;
        }

        pandaproxy::schema_registry::subject value_subject{
          std::string(topic_name) + "-value"};

        try {
            auto value_schema = co_await _schema_registry->get_subject_schema(
              value_subject, std::nullopt);

            if (
              value_schema.schema.type()
              != pandaproxy::schema_registry::schema_type::protobuf) {
                co_return default_return_value;
            }
            std::string value_schema_string = value_schema.schema.def().raw()();

            co_return schema_info{
              .schema = value_schema_string,
              .message_name = "",
            };
        } catch (const pandaproxy::schema_registry::exception& e) {
            if (
              e.code().value()
              == int(
                pandaproxy::schema_registry::error_code::subject_not_found)) {
            } else {
            }
            co_return default_return_value;
        }
    }

    bool _initialized = false;
    std::unique_ptr<wasm::schema_registry> _schema_registry;
    pandaproxy::schema_registry::api* _pandaproxy_schema_registry;
};

} // namespace datalake
