#pragma once

#include "utils/fragbuf.h"

namespace demo {
struct simple_request {
    fragbuf data;
};
struct simple_reply {
    fragbuf data;
};

struct c1 {
    int32_t a{42}, b{1}, c{66}, d{-8};
};
struct c2 {
    c1 _a;
    int32_t b{1}, c{66}, d{-8}, e{0};
};
struct c3 {
    c2 _a;
    int32_t b{1}, c{66}, d{-8}, e{0};
};
struct c4 {
    c3 _a;
    int32_t b{1}, c{66}, d{-8}, e{0};
};
struct c5 {
    c4 _a;
    int32_t b{1}, c{66}, d{-8}, e{0};
};

struct complex_request {
    struct payload {
        c1 _one;   // 4 fields
        c2 _two;   // 8 fields
        c3 _three; // 16 fields
        c4 _four;  // 32 fields
        c5 _five;  // 64 fields
    };
    // 128 fields
    payload data;
};
struct complex_reply {
    int32_t x{-1};
};

struct i1 {
    c1 x;
    fragbuf y;
};
struct i2 {
    i1 x;
    fragbuf y;
};

struct i3 {
    i2 x;
    fragbuf y;
};
struct interspersed_request {
    struct payload {
        i1 _one;
        i2 _two;
        i3 _three;
    };
    payload data;
    fragbuf x;
    fragbuf y;
};
struct interspersed_reply {
    int32_t x{-1};
};

} // namespace demo
