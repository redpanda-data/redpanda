#pragma once

static inline bool likely(bool cond) { return __builtin_expect(cond, true); }

static inline bool unlikely(bool cond) { return __builtin_expect(cond, false); }
