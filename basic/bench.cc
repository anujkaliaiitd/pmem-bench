#include <errno.h>
#include <libpmem.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "../common.h"

static constexpr size_t kFileSizeGB = 64;  // The expected file size
static constexpr size_t kNumIters = 100000;

void bench_write_lat_byte(uint8_t *pbuf) {
  struct timespec start, end;
  clock_gettime(CLOCK_REALTIME, &start);

  for (size_t i = 0; i < kNumIters; i++) {
    pbuf[0] = 'A';
    pmem_persist(pbuf, 1);
  }

  clock_gettime(CLOCK_REALTIME, &end);
  double tot_ns = (end.tv_sec - start.tv_sec) * 1000000000.0 +
                  (end.tv_nsec - start.tv_nsec);

  printf("Latency of persistent writes to same byte = %.2f ns\n",
         tot_ns / kNumIters);
}

int main() {
  uint8_t *pbuf;
  size_t mapped_len;
  int is_pmem;

  pbuf = reinterpret_cast<uint8_t *>(
      pmem_map_file("/mnt/pmem12/src.txt", 0 /* length */, 0 /* flags */, 0666,
                    &mapped_len, &is_pmem));

  rt_assert(pbuf != nullptr,
            "pmem_map_file() failed. " + std::string(strerror(errno)));
  rt_assert(mapped_len == kFileSizeGB,
            "Incorrect file size " + std::to_string(mapped_len));
  rt_assert(is_pmem == 1, "File is not pmem");

  bench_write_lat_byte(pbuf);

  pmem_unmap(pbuf, mapped_len);
  exit(0);
}
