#pragma once

#include <seastar/util/log.hh>

namespace detail {
struct dummyassert {
    static inline ss::logger l{"assert"};
};
static dummyassert g_assert_log;
} // namespace detail

/** Meant to be used in the same way as assert(condition, msg);
 * which means we use the negative conditional.
 * i.e.:
 *
 * open_fileset::~open_fileset() noexcept {
 *   vassert(_closed, "fileset not closed");
 * }
 *
 */
#define vassert(x, msg, args...)                                               \
    do {                                                                       \
        /*The !(x) is not an error. see description above*/                    \
        if (__builtin_expect(!(x), false)) {                                   \
            ::detail::g_assert_log.l.error(                                    \
              "Assert failure: ({}:{}) '" #x "' " msg,                         \
              __FILE__,                                                        \
              __LINE__,                                                        \
              ##args);                                                         \
            std::terminate();                                                  \
        }                                                                      \
    } while (0)
