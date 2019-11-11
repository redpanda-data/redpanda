#pragma once
#include "bytes/iobuf.h"
#include "random/fast_prng.h"
#include "random/generators.h"

static const constexpr size_t characters_per_append = 10;

void append_sequence(iobuf& buf, size_t count) {
    for (size_t i = 0; i < count; i++) {
        auto str = random_generators::gen_alphanum_string(
          characters_per_append);
        buf.append(str.data(), str.size());
    }
}
