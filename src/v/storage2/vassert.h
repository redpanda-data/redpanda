#pragma once

#include <seastar/util/log.hh>

#ifndef vassert_h
#define vassert_h

namespace detail {
extern seastar::logger vassertlog;
} // namespace

#define vassert_none()

#define vassert_nomsg(x)                                  \
    do {                                                  \
        if (__builtin_expect(!(x), false)) {                \
            ::detail::vassertlog.error(                   \
              "Assert failure: ({}:{}): "#x,              \
              __FILE__,                                   \
              __LINE__);                                  \
            std::terminate();                             \
        }                                                 \
    } while (0)

#define vassert_msg(x, msg)                               \
    do {                                                  \
        if (__builtin_expect(!(x), false)) {                \
            ::detail::vassertlog.error(                   \
              "Assert failure: ({}:{}) \""#x"\": {}",     \
              __FILE__,                                   \
              __LINE__,                                   \
              msg);                                       \
            std::terminate();                             \
        }                                                 \
    } while (0)

#define V_CHOOSE_MACRO(x,A,B,FUNC, ...)  FUNC 

#define vassert(...) V_CHOOSE_MACRO(,##__VA_ARGS__,\
                       vassert_msg(__VA_ARGS__),\
                       vassert_nomsg(__VA_ARGS__),\
                       vassert_none(__VA_ARGS__)\
                     )
#endif 