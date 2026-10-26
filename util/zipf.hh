#pragma once

#include <cmath>
#include <cstdint>

#include "util/random.hh"

// Zipf distribution over [1, n] with P(k) ∝ 1/k^exponent, sampled by
// rejection-inversion (Hörmann & Derflinger) — the algorithm behind
// org.apache.commons.math3.distribution.ZipfDistribution#sample (3.6.1), which
// the original uses with exponent 1.
// O(1) construction, so a per-neighbourhood instance is cheap.
class ZipfRejectionInversion {
public:
  ZipfRejectionInversion(uint64_t n, double exponent)
      : n_(n), exponent_(exponent),
        h_integral_x1_(h_integral(1.5) - 1.0),
        h_integral_n_(h_integral(static_cast<double>(n) + 0.5)),
        s_(2.0 - h_integral_inverse(h_integral(2.5) - h(2.0))) {}

  uint64_t operator()(Xoroshiro128Plus& rnd) const {
    for (;;) {
      double u = h_integral_n_ +
                 next_double(rnd) * (h_integral_x1_ - h_integral_n_);
      double x = h_integral_inverse(u);
      double kd = std::floor(x + 0.5);
      if (kd < 1.0) kd = 1.0;
      else if (kd > static_cast<double>(n_)) kd = static_cast<double>(n_);
      if (kd - x <= s_ || u >= h_integral(kd + 0.5) - h(kd))
        return static_cast<uint64_t>(kd);
    }
  }

private:
  uint64_t n_;
  double exponent_;
  double h_integral_x1_, h_integral_n_, s_;

  static double next_double(Xoroshiro128Plus& rnd) {
    return static_cast<double>(rnd.next() >> 11) * 0x1.0p-53;  // [0, 1)
  }
  // H(x) = ((x^(1-e)) - 1) / (1-e), with the e == 1 limit log(x).
  double h_integral(double x) const {
    double log_x = std::log(x);
    return helper2((1.0 - exponent_) * log_x) * log_x;
  }
  double h(double x) const { return std::exp(-exponent_ * std::log(x)); }
  double h_integral_inverse(double x) const {
    double t = x * (1.0 - exponent_);
    if (t < -1.0) t = -1.0;  // limit for numerical stability
    return std::exp(helper1(t) * x);
  }
  // log1p(x)/x and expm1(x)/x, with their Taylor expansions near 0.
  static double helper1(double x) {
    if (std::fabs(x) > 1e-8) return std::log1p(x) / x;
    return 1.0 - x * (0.5 - x * (1.0 / 3.0 - 0.25 * x));
  }
  static double helper2(double x) {
    if (std::fabs(x) > 1e-8) return std::expm1(x) / x;
    return 1.0 + x * 0.5 * (1.0 + x * (1.0 / 3.0) * (1.0 + 0.25 * x));
  }
};
