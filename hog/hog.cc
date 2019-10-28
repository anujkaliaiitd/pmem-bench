#include <assert.h>
#include <libpmem.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include "../common.h"

// static constexpr const char *kFileName = "/mnt/pmem12/raft_log";
static constexpr const char *kFileName = "/dev/dax0.0";
static constexpr size_t kPmemFileSize = GB(8);

int main() {
  rt_assert(getuid() == 0, "You need to be root to run this benchmark");
  uint8_t *pbuf;
  size_t mapped_len;

  int is_pmem;
  pbuf = reinterpret_cast<uint8_t *>(
      pmem_map_file(kFileName, 0, 0, 0666, &mapped_len, &is_pmem));

  rt_assert(pbuf != nullptr);
  rt_assert(mapped_len >= kPmemFileSize);

  size_t iter = 0;
  auto *buf = reinterpret_cast<uint8_t *>(malloc(kPmemFileSize));

  while (true) {
    struct timespec start;
    clock_gettime(CLOCK_REALTIME, &start);
    pmem_memcpy_persist(pbuf, buf, kPmemFileSize);
    printf("Hog: iter = %zu, bandwidth = %.2f GB/s\n", iter,
           (kPmemFileSize * 1.0 / GB(1)) / sec_since(start));
    iter++;
  }
}
