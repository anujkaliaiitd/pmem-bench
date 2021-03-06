#include "bench.h"

void bench_seq_write_latency(uint8_t *pbuf) {
  double freq_ghz = measure_rdtsc_freq();

  static constexpr bool kMeasurePercentiles = true;

  // Update the source data for every write. Not doing so decreases latency.
  static constexpr bool kChangeWriteSource = false;

  static constexpr size_t kWriteBytes = MB(64);
  static constexpr size_t kMinIters = 50000;
  static constexpr size_t kMinWriteSz = 64;
  static constexpr size_t kMaxWriteSz = KB(64);

  size_t file_offset = 0;

  static_assert(kWriteBytes / kMinWriteSz >= kMinIters, "");
  std::vector<size_t> latency_vec;
  latency_vec.reserve(kWriteBytes / kMinWriteSz);

  size_t *data = reinterpret_cast<size_t *>(memalign(4096, kMaxWriteSz));
  memset(data, 31, kMaxWriteSz);

  for (size_t msr = 0; msr < 100; msr++) {
    printf("size avg_ns 50_ns 999_ns\n");
    std::ostringstream verify_tsc_str;  // Compare tsc results with realtime

    for (size_t wr_size = kMinWriteSz; wr_size <= kMaxWriteSz; wr_size *= 2) {
      struct timespec start_time;
      clock_gettime(CLOCK_REALTIME, &start_time);

      latency_vec.clear();
      file_offset = roundup<256>(file_offset);
      const size_t num_iters = kWriteBytes / wr_size <= kMinIters
                                   ? kMinIters
                                   : kWriteBytes / wr_size;

      for (size_t i = 0; i < num_iters; i++) {
        if (kChangeWriteSource) {
          for (size_t cl = 0; cl < wr_size / 64; cl++) data[cl * 8]++;
        }

        size_t start_tsc;
        if (kMeasurePercentiles) start_tsc = timer::Start();
        pmem_memmove_persist(&pbuf[file_offset], data, wr_size);

        if (kMeasurePercentiles) {
          latency_vec.push_back(timer::Stop() - start_tsc);
        }

        file_offset += wr_size;
        if (file_offset + wr_size >= kPmemFileSize) file_offset = 0;
      }

      size_t ns_avg_realtime = ns_since(start_time) / num_iters;

      if (kMeasurePercentiles) {
        std::sort(latency_vec.begin(), latency_vec.end());
        printf("%zu %zu %.1f %.1f\n", wr_size, ns_avg_realtime,
               latency_vec.at(num_iters * .50) / freq_ghz,
               latency_vec.at(num_iters * .999) / freq_ghz);

        size_t ns_avg_rdtsc =
            std::accumulate(latency_vec.begin(), latency_vec.end(), 0.0) /
            (latency_vec.size() * freq_ghz);
        verify_tsc_str << wr_size << ": Avg latency (ns) " << ns_avg_realtime
                       << " (realtime) " << ns_avg_rdtsc << " (rdtsc) "
                       << (ns_avg_realtime - ns_avg_rdtsc) << " (delta). offst "
                       << file_offset << "\n";
      } else {
        printf("%zu %zu -1.0 -1.0\n", wr_size, ns_avg_realtime);
      }
    }

    printf("Fences verification:\n%s\n", verify_tsc_str.str().c_str());
  }
}
