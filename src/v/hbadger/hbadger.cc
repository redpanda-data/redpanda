#include "hbadger.h"

#ifdef HONEY_BADGER

#include <map>
#include <sstream>
#include <string>

namespace v {
honey_badger::honey_badger() {
  lua_.open_libraries(sol::lib::base, sol::lib::ffi, sol::lib::jit);
  lua_.script_file(kHoneyBadgerScriptName);
}
void
honey_badger::hbadger(const char *filename, int line, const char *module,
                      const char *func) {
  auto hfn = lua_["honey_badger_fn"];
  auto sol_ret = hfn(filename, line, module, func);
  if (!sol_ret.valid()) { return; }
  std::tuple<bool, int, std::string, std::string> ret;
  ret = sol_ret;
  auto [is_error, error_code, category, deets] = std::move(ret);
  if (is_error) {
    std::stringstream ss;
    ss << "honey_badger_failure{Code: " << error_code
       << ", Category: " << category << ", Details: " << deets << "}";
    throw std::runtime_error(ss.str());
  }
}

}  // namespace v

#else
namespace v {
// makes compiler happy
void
dummy_badger::d() {}
}  // namespace v
#endif
