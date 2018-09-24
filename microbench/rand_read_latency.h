#include <city.h>
#include "../common.h"
#include "main.h"

void bench_rand_read_latency(uint8_t *pbuf) {
  double freq_ghz = measure_rdtsc_freq();

  static constexpr size_t kReadBytes = MB(128);
  static constexpr size_t kMinIters = 50000;
  static constexpr size_t kMinReadSz = 64;
  static constexpr size_t kMaxReadSz = KB(64);

  size_t file_offset = 0;
  pcg64_fast pcg(pcg_extras::seed_seq_from<std::random_device>{});

  static_assert(kReadBytes / kMinReadSz >= kMinIters, "");
  std::vector<size_t> latency_vec;
  latency_vec.reserve(kReadBytes / kMinReadSz);

  size_t sum = 0;
  uint8_t *data = reinterpret_cast<uint8_t *>(memalign(4096, kMaxReadSz));

  for (size_t msr = 0; msr < 10; msr++) {
    printf("size avg_ns 50_ns 999_ns\n");
    std::ostringstream verify_tsc_str;  // Compare tsc results with realtime

    for (size_t size = kMinReadSz; size <= kMaxReadSz; size *= 2) {
      struct timespec start_time;
      clock_gettime(CLOCK_REALTIME, &start_time);

      latency_vec.clear();
      const size_t num_iters =
          kReadBytes / size <= kMinIters ? kMinIters : kReadBytes / size;

      for (size_t i = 0; i < num_iters; i++) {
        size_t rand =
            CityHash64(reinterpret_cast<char *>(&sum), sizeof(sum)) + pcg();

        file_offset = roundup<64>(rand % kPmemFileSize);

        size_t start_tsc = timer::Start();
        memcpy(data, &pbuf[file_offset], size);
        for (size_t j = 0; j < size; j += 64) sum += data[j];

        latency_vec.push_back(timer::Stop() - start_tsc);
      }

      size_t ns_avg_realtime = ns_since(start_time) / num_iters;
      size_t ns_avg_rdtsc =
          std::accumulate(latency_vec.begin(), latency_vec.end(), 0.0) /
          (latency_vec.size() * freq_ghz);
      verify_tsc_str << size << ": Average latency (ns) " << ns_avg_realtime
                     << " (realtime) " << ns_avg_rdtsc << " (rdtsc) "
                     << (ns_avg_realtime - ns_avg_rdtsc) << " (delta) "
                     << "\n";

      std::sort(latency_vec.begin(), latency_vec.end());
      printf("%zu %zu %.1f %.1f\n", size, ns_avg_realtime,
             latency_vec.at(num_iters * .50) / freq_ghz,
             latency_vec.at(num_iters * .999) / freq_ghz);
    }

    printf("Fences verification:\n%s\n", verify_tsc_str.str().c_str());
  }
}
