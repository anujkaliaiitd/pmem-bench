#include "../common.h"
#include "main.h"

void bench_rand_read_tput(uint8_t *pbuf, size_t thread_id, const size_t copy_sz,
                          size_t num_threads) {
  static constexpr size_t kNumIters = MB(4);
  assert(copy_sz == 64 || copy_sz == 256 || copy_sz == 512 || copy_sz == 1024);

  pcg64_fast pcg(pcg_extras::seed_seq_from<std::random_device>{});
  struct timespec start;
  size_t sum = 0;

  for (size_t iter = 0; iter < 5; iter++) {
    clock_gettime(CLOCK_REALTIME, &start);

    if (copy_sz == 64) {
      for (size_t i = 0; i < kNumIters; i++) {
        size_t offset = roundup<64>(pcg() % kPmemFileSize);
        sum += pbuf[offset];
      }
    } else if (copy_sz == 256) {
      for (size_t i = 0; i < kNumIters; i++) {
        size_t offset = roundup<64>(pcg() % kPmemFileSize);
        for (size_t cl = 0; cl < 4; cl++) {
          sum += pbuf[offset + cl * 64];
        }
      }
    } else if (copy_sz == 512) {
      for (size_t i = 0; i < kNumIters; i++) {
        size_t offset = roundup<64>(pcg() % kPmemFileSize);
        for (size_t cl = 0; cl < 8; cl++) {
          sum += pbuf[offset + cl * 64];
        }
      }
    } else if (copy_sz == 1024) {
      for (size_t i = 0; i < kNumIters; i++) {
        size_t offset = roundup<64>(pcg() % kPmemFileSize);
        for (size_t cl = 0; cl < 16; cl++) {
          sum += pbuf[offset + cl * 64];
        }
      }
    }

    double tot_sec = sec_since(start);
    double rate = kNumIters / tot_sec;
    printf(
        "Thread %zu of %zu, copy sz %zu: random read tput = %.2f M/sec, "
        "sum = %zu\n",
        thread_id, num_threads, copy_sz, rate / 1000000, sum);
  }
}
