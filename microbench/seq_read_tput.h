#include "bench.h"

void bench_seq_read_tput(uint8_t *pbuf, size_t thread_id, size_t num_threads) {
  static constexpr size_t kReadSize = MB(256);
  auto *buf = new uint8_t[kReadSize];

  pcg64_fast pcg(pcg_extras::seed_seq_from<std::random_device>{});
  struct timespec start;
  size_t sum = 0;

  for (size_t iter = 0; iter < 20; iter++) {
    clock_gettime(CLOCK_REALTIME, &start);

    // Generate a 64-byte aligned address to read kReadSize bytes from
    size_t start_address = roundup<64>(pcg() % kPmemFileSize);
    if (start_address + kReadSize >= kPmemFileSize) {
      iter--;
      continue;
    }

    memcpy(buf, &pbuf[start_address], kReadSize);
    sum += buf[pcg() % kReadSize];

    double tot_sec = sec_since(start);
    printf("Thread %zu of %zu, seq read tput = %.2f GB/sec, sum = %zu\n",
           thread_id, num_threads, kReadSize * 1.0 / (GB(1) * tot_sec), sum);
  }

  delete[] buf;
}
