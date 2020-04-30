#pragma once

#include "config/property.h"
#include "seastarx.h"
#include "utils/to_string.h"

#include <fmt/format.h>

#include <unordered_map>

namespace config {
class config_store {
public:
    base_property& get(const std::string_view& name) {
        return *_properties.at(name);
    }

    virtual void read_yaml(const YAML::Node& root_node) {
        for (auto const& [name, property] : _properties) {
            if (property->is_required() == required::no) {
                continue;
            }
            ss::sstring name_str(name.data());
            if (!root_node[name_str]) {
                throw std::invalid_argument(
                  fmt::format("Property {} is required", name));
            }
        }

        for (auto const& node : root_node) {
            auto name = node.first.as<ss::sstring>();
            auto found = _properties.find(name);
            if (found == _properties.end()) {
                throw std::invalid_argument(
                  fmt::format("Unknown property {}", name));
            }
            found->second->set_value(node.second);
        }
    }

    template<typename Func>
    void for_each(Func&& f) const {
        for (auto const& [_, property] : _properties) {
            f(*property);
        }
    }

    const std::vector<validation_error> validate() {
        std::vector<validation_error> errors;
        for_each([&errors](const base_property& p) {
            if (auto err = p.validate()) {
                errors.push_back(std::move(*err));
            }
        });
        return errors;
    }

    void to_json(rapidjson::Writer<rapidjson::StringBuffer>& w) const {
        w.StartObject();

        for (const auto& [name, property] : _properties) {
            w.Key(name.data(), name.size());
            property->to_json(w);
        }

        w.EndObject();
    }

    virtual ~config_store() noexcept = default;

private:
    friend class base_property;
    std::unordered_map<std::string_view, base_property*> _properties;
};
}; // namespace config

namespace std {
template<typename... Types>
static inline ostream& operator<<(ostream& o, const config::config_store& c) {
    o << "{ ";
    c.for_each([&o](const auto& property) { o << property << " "; });
    o << "}";
    return o;
}
} // namespace std

namespace YAML {
template<>
struct convert<ss::sstring> {
    static Node encode(const ss::sstring& rhs) { return Node(rhs.c_str()); }
    static bool decode(const Node& node, ss::sstring& rhs) {
        if (!node.IsScalar()) {
            return false;
        }
        rhs = node.as<std::string>();
        return true;
    }
};
}; // namespace YAML
