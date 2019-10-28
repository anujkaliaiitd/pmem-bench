#include <assert.h>
#include <gflags/gflags.h>
#include <libpmem.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include <pcg/pcg_random.hpp>
#include "../common.h"
#include "../utils/timer.h"
#include "config.h"

DEFINE_uint64(use_pmem, 1, "Use persistent memory");
DEFINE_uint64(object_size, KB(4), "Size of objects");

// static constexpr const char *kFileName = "/mnt/pmem12/raft_log";
static constexpr const char *kFileName = "/dev/dax0.0";
static constexpr bool kUsePmem = true;
static constexpr size_t kFileSize = GB(32);

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  rt_assert(getuid() == 0, "You need to be root to run this benchmark");

  uint8_t *pbuf;
  size_t mapped_len;

  if (FLAGS_use_pmem == 1) {
    printf("Using persistent memory buffer, size %zu\n", FLAGS_object_size);
    int is_pmem;
    pbuf = reinterpret_cast<uint8_t *>(
        pmem_map_file(kFileName, 0, 0, 0666, &mapped_len, &is_pmem));

    rt_assert(pbuf != nullptr);
    rt_assert(mapped_len >= kFileSize);
  } else {
    printf("Using volatile memory buffer\n");
    pbuf = reinterpret_cast<uint8_t *>(malloc(kFileSize));
  }

  size_t iter = 0;
  size_t sum = 0;
  pcg64_fast pcg(pcg_extras::seed_seq_from<std::random_device>{});

  while (true) {
    size_t rand = pcg();
    size_t offset = roundup<64>(rand % kFileSize);
    if (offset + FLAGS_object_size >= kFileSize) continue;

    uint8_t *obj = &pbuf[offset];
    for (size_t i = 0; i < FLAGS_object_size / 64; i++) sum += obj[i * 64];

    struct timespec bench_start;
    clock_gettime(CLOCK_REALTIME, &bench_start);

    for (size_t i = 0; i < FLAGS_object_size / 64; i++) obj[i * 64] = iter % 2;
    memset(obj, iter, FLAGS_object_size);
    for (size_t i = 0; i < FLAGS_object_size / 64; i++) obj[i * 64] = iter % 3;

    printf("Object size %zu, time = %.2f us, bw = %.2f GB/s, size %zu\n",
           FLAGS_object_size, sec_since(bench_start) * 1000000,
           FLAGS_object_size / (1024 * 1024 * 1024.0 * sec_since(bench_start)),
           FLAGS_object_size);

    iter++;
  }

  if (kUsePmem) pmem_unmap(pbuf, mapped_len);
}
