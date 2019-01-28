#pragma once
#include <chrono>
#include <cstdint>
#include <memory>
#include <ostream>

namespace v {
template <std::size_t Period = 10>
class ema {
 public:
  struct time_measure {
    explicit time_measure(ema *ptr)
      : e(ptr), begin(std::chrono::high_resolution_clock::now()) {}
    time_measure(time_measure &) = delete;
    ~time_measure() {
      namespace ch = std::chrono;  // NOLINT
      e->record_ema(ch::duration_cast<ch::nanoseconds>(
                      ch::high_resolution_clock::now() - begin)
                      .count());
    }
    ema *e;
    const std::chrono::high_resolution_clock::time_point begin;
  };

  static constexpr double
  alpha() {
    return double(2) / static_cast<double>(Period + 1);
  }
  static constexpr double
  one_minus_alpha() {
    return 1 - alpha();
  }
  // -- main api
  inline void
  record_ema(uint64_t nanos) {
    ema_ = alpha() * nanos + (one_minus_alpha() * ema_);
  }
  inline double
  get() const {
    return ema_;
  }
  std::unique_ptr<time_measure>
  measure() {
    return std::make_unique<time_measure>(this);
  }

 private:
  double ema_{0};
};
}  // namespace v

namespace std {
template <std::size_t Period>
inline std::ostream &
operator<<(ostream &o, const ::v::ema<Period> &e) {
  auto const orig_precision = o.precision();
  return o << std::fixed << std::setprecision(3) << "v::ema<" << Period << ">("
           << e.get() << "ns)" << std::setprecision(orig_precision);
}
}  // namespace std
