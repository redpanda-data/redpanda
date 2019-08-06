#include "redpanda/config/base_property.h"

#include "redpanda/config/config_store.h"

namespace config {
base_property::base_property(
  config_store& conf,
  std::string_view name,
  std::string_view desc,
  required req)
  : _name(name)
  , _desc(desc)
  , _required(req) {
    conf._properties.emplace(name, this);
}

std::ostream& operator<<(std::ostream& o, const base_property& p) {
    p.print(o);
    return o;
}

}; // namespace config