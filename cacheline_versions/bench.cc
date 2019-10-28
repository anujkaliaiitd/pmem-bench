#include <assert.h>
#include <libpmem.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include <algorithm>
#include <vector>
#include "../common.h"
#include "../utils/timer.h"
#include "config.h"

// Config parameters:

// static constexpr const char *kFileName = "/mnt/pmem12/raft_log";
static constexpr const char *kFileName = "/dev/dax0.0";
static constexpr bool kUsePmem = true;

int main() {
  rt_assert(getuid() == 0, "You need to be root to run this benchmark");

  uint8_t *pbuf;
  size_t mapped_len;

  if (kUsePmem) {
    printf("Using persistent memory buffer\n");
    int is_pmem;
    pbuf = reinterpret_cast<uint8_t *>(
        pmem_map_file(kFileName, 0, 0, 0666, &mapped_len, &is_pmem));

    rt_assert(pbuf != nullptr);
    rt_assert(mapped_len >= kBufSize);
  } else {
    printf("Using volatile memory buffer\n");
    pbuf = reinterpret_cast<uint8_t *>(malloc(kBufSize));
  }

  size_t iter = 0;
  while (true) {
    struct timespec bench_start;
    clock_gettime(CLOCK_REALTIME, &bench_start);

    for (size_t i = 0; i < kBufSize / 64; i++) pbuf[i * 64] = 0;
    memset(pbuf, iter, kBufSize);
    for (size_t i = 0; i < kBufSize / 64; i++) pbuf[i * 64] = 1;

    printf("Buf size %zu, time = %.2f us, bw = %.2f GB/s\n\n", kBufSize,
           sec_since(bench_start) * 1000000,
           kBufSize / (1024 * 1024 * 1024.0 * sec_since(bench_start)));
  }

  if (kUsePmem) pmem_unmap(pbuf, mapped_len);
}
