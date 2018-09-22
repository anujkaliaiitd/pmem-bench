#include "../common.h"
#include "main.h"

void bench_rand_write_latency(uint8_t *pbuf) {
  static constexpr size_t kNumIters = 1000000;
  static constexpr size_t kMinWriteSz = 64;
  static constexpr size_t kMaxWriteSz = KB(64);

  pcg64_fast pcg(pcg_extras::seed_seq_from<std::random_device>{});
  double freq_ghz = measure_rdtsc_freq();

  size_t file_offset;
  std::vector<size_t> latency_vec;
  latency_vec.reserve(kNumIters);

  uint8_t *data = reinterpret_cast<uint8_t *>(memalign(4096, kMaxWriteSz));

  for (size_t msr = 0; msr < 10; msr++) {
    printf("size 50_ns 99_ns 999_ns\n");

    for (size_t size = kMinWriteSz; size <= kMaxWriteSz; size *= 2) {
      latency_vec.clear();

      for (size_t i = 0; i < kNumIters; i++) {
        file_offset = roundup<64>(pcg() % kPmemFileSize);

        size_t start_tsc = rdtsc();
        mfence();
        pmem_memmove_persist(&pbuf[file_offset], data, size);
        mfence();

        latency_vec.push_back(rdtsc() - start_tsc);
      }

      std::sort(latency_vec.begin(), latency_vec.end());
      printf("%zu %.1f %.1f %.1f\n", size,
             latency_vec.at(kNumIters * .5) / freq_ghz,
             latency_vec.at(kNumIters * .99) / freq_ghz,
             latency_vec.at(kNumIters * .999) / freq_ghz);
    }
  }
}
