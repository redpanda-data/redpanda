#pragma once

#ifdef HONEY_BADGER
#include <sol.hpp>
constexpr static const char* kHoneyBadgerScriptName = "honey_badger.lua";
class honey_badger {
public:
    honey_badger();
    ~honey_badger() = default;

    void hbadger(
      const char* filename, int line, const char* module, const char* func);

    static honey_badger& get() {
        static thread_local honey_badger h;
        return h;
    }

private:
    sol::state _lua;
};
#define HBADGER(module, func)                                                  \
    honey_badger::get().hbadger(__FILE__, __LINE__, #module, #func)
#else
struct dummy_badger {
    static void d();
};
#define HBADGER(module, func) ((void)0)
#endif
