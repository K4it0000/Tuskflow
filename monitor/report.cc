#include "monitor/report.hh"

#include <unistd.h>

#include <algorithm>
#include <cmath>
#include <cstdio>
#include <fstream>
#include <string>

namespace tuskflow {

size_t rss_bytes() {
  long pages = 0, resident = 0;
  FILE* f = std::fopen("/proc/self/statm", "r");
  if (f == nullptr) return 0;
  if (std::fscanf(f, "%ld %ld", &pages, &resident) != 2) resident = 0;
  std::fclose(f);
  return static_cast<size_t>(resident) * static_cast<size_t>(sysconf(_SC_PAGESIZE));
}
size_t peak_rss_bytes() {  // VmHWM
  std::ifstream in("/proc/self/status");
  std::string line;
  while (std::getline(in, line))
    if (line.rfind("VmHWM:", 0) == 0) return std::stoull(line.substr(6)) * 1024;
  return 0;
}

// MetricsMonitor.getPercentileFromSorted
double percentile_sorted(const std::vector<double>& m, double p) {
  if (m.empty()) return 0.0;
  double n = static_cast<double>(m.size());
  double pos = p * (n + 1) / 100;
  double fpos = std::floor(pos);
  int int_pos = static_cast<int>(fpos);
  double dif = pos - fpos;
  if (pos < 1) return m.front();
  if (pos >= n) return m.back();
  double lower = m[int_pos - 1];
  double upper = m[int_pos];
  return lower + dif * (upper - lower);
}
void print_percentiles(std::vector<double> m, const char* identifier) {
  if (m.empty()) return;
  std::sort(m.begin(), m.end());
  double sum = 0;
  for (double v : m) sum += v;
  std::printf("%s: 5th %10.3f 25th %10.3f 50th %10.3f 75th %10.3f 95th %10.3f "
              "99th %10.3f [avg %10.3f min %10.3f max %10.3f]\n",
              identifier, percentile_sorted(m, 5), percentile_sorted(m, 25),
              percentile_sorted(m, 50), percentile_sorted(m, 75),
              percentile_sorted(m, 95), percentile_sorted(m, 99),
              sum / static_cast<double>(m.size()), m.front(), m.back());
}

}  // namespace tuskflow
