// Antithesis coverage instrumentation hooks.
//
// This translation unit provides the __sanitizer_cov_trace_pc_guard and
// __sanitizer_cov_trace_pc_guard_init symbols required when building with
// -fsanitize-coverage=trace-pc-guard. The antithesis_instrumentation.h
// header forwards these to libvoidstar.so (loaded via dlopen) when running
// inside the Antithesis environment, and provides no-op fallbacks otherwise.
//
// This must be linked into any binary built with --config=antithesis.

#include "antithesis_instrumentation.h"
