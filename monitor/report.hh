#pragma once

// Helpers for the final report: memory use and MetricsMonitor's percentiles.

#include <cstddef>
#include <vector>

namespace tuskflow {

size_t rss_bytes();
size_t peak_rss_bytes();  // VmHWM
// MetricsMonitor.getPercentileFromSorted
double percentile_sorted(const std::vector<double>& m, double p);
// MetricsMonitor.printMeasurementsWithIdentifier
void print_percentiles(std::vector<double> m, const char* identifier);

}  // namespace tuskflow
