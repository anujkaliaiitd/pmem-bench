#include "bench.h"

// Benchmark impl
#include "rand_read_latency.h"
#include "rand_read_tput.h"
#include "rand_write_latency.h"
#include "rand_write_tput.h"
#include "seq_read_tput.h"
#include "seq_write_latency.h"
#include "seq_write_tput.h"

// Return true if kPmemFile is in devdax mode
static bool is_pmem_file_devdax() {
  if (std::string(kPmemFile).find("dax") != std::string::npos) return true;
  return false;
}

// Write to the whole buffer to "map it in", whatever that means
void map_in_buffer_whole(uint8_t *pbuf) {
  printf("Writing to the whole file for map-in...\n");
  const size_t chunk_sz = GB(16);
  rt_assert(kPmemFileSize % chunk_sz == 0, "Invalid chunk size for map-in");

  for (size_t i = 0; i < kPmemFileSize; i += chunk_sz) {
    struct timespec start;
    clock_gettime(CLOCK_REALTIME, &start);
    pmem_memset_persist(&pbuf[i], 3185, chunk_sz);  // nodrain performs similar
    printf("Fraction complete = %.2f. Took %.3f sec for %zu GB.\n",
           (i + 1) * 1.0 / kPmemFileSize, sec_since(start), chunk_sz / GB(1));
  }

  printf("Done writing.\n");
}

// Write to a byte in each page of the buffer, to map the pages in
void map_in_buffer_by_page(uint8_t *pbuf) {
  printf("Mapping-in file pages.\n");
  struct timespec start;
  clock_gettime(CLOCK_REALTIME, &start);

  for (size_t i = 0; i < kPmemFileSize; i += KB(4)) {
    pmem_memset_nodrain(&pbuf[i], 3185, 1);
    if (i % GB(32) == 0 && i > 0) {
      printf("Fraction complete = %.2f. Took %.3f sec for %u GB.\n",
             (i + 1) * 1.0 / kPmemFileSize, sec_since(start), 32);
      clock_gettime(CLOCK_REALTIME, &start);
    }
  }

  printf("Done mapping-in.\n");
}

// Map pmem file in devdax mode
uint8_t *map_pmem_file_devdax() {
  int fd = open(kPmemFile, O_RDWR);
  rt_assert(fd >= 0, "devdax open failed");
  rt_assert(kPmemFileSize % MB(2) == 0, "File size must be multiple of 2 MB");

  void *buf =
      mmap(nullptr, kPmemFileSize, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
  rt_assert(buf != MAP_FAILED, "mmap failed for devdax");
  rt_assert(reinterpret_cast<size_t>(buf) % 256 == 0);

  return reinterpret_cast<uint8_t *>(buf);
}

// Map pmem file in fsdax mode
uint8_t *map_pmem_file_fsdax() {
  uint8_t *pbuf;
  size_t mapped_len;
  int is_pmem;

  pbuf = reinterpret_cast<uint8_t *>(pmem_map_file(
      kPmemFile, 0 /* length */, 0 /* flags */, 0666, &mapped_len, &is_pmem));

  rt_assert(pbuf != nullptr,
            "pmem_map_file() failed. " + std::string(strerror(errno)));
  rt_assert(mapped_len >= kPmemFileSize,
            "pmem file too small " + std::to_string(mapped_len));
  rt_assert(reinterpret_cast<size_t>(pbuf) % 4096 == 0,
            "Mapped buffer isn't page-aligned");
  rt_assert(is_pmem == 1, "File is not pmem");
  printf("Mapped file of length %.2f GB\n", mapped_len * 1.0 / GB(1));

  return pbuf;
}

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  uint8_t *pbuf;

  freq_ghz = measure_rdtsc_freq();
  printf("RDTSC frequency = %.2f GHz\n", freq_ghz);

  pbuf = is_pmem_file_devdax() ? map_pmem_file_devdax() : map_pmem_file_fsdax();

  // Print some random file samples to check it's full of random contents
  printf("File contents sample: ");
  pcg64_fast pcg(pcg_extras::seed_seq_from<std::random_device>{});
  for (size_t i = 0; i < 10; i++) {
    printf("%zu ", *reinterpret_cast<size_t *>(&pbuf[pcg() % kPmemFileSize]));
  }
  printf("\n");

  // map_in_buffer_by_page(pbuf);
  // map_in_buffer_whole(pbuf);

  std::string bench_func;  // Last one wins
  bench_func = "bench_seq_write_tput";
  bench_func = "bench_seq_read_latency";
  bench_func = "bench_rand_write_latency";
  bench_func = "bench_rand_read_tput";
  bench_func = "bench_seq_write_tput";
  bench_func = "bench_seq_write_latency";
  bench_func = "bench_rand_read_latency";
  bench_func = "bench_seq_read_tput";
  bench_func = "bench_rand_write_tput";

  // Sequential write throughput
  if (bench_func == "bench_seq_write_tput") {
    printf("Sequential write throughput. %zu threads\n", FLAGS_num_threads);
    std::ostringstream dat_header;
    std::ostringstream dat_data;
    dat_header << "Threads ";
    dat_data << std::to_string(FLAGS_num_threads) << " ";

    for (size_t copy_sz = 64; copy_sz <= GB(1); copy_sz *= 2) {
      dat_header << std::to_string(copy_sz) << " ";
      std::vector<double> avg_tput_GBps(FLAGS_num_threads);

      std::vector<std::thread> threads(FLAGS_num_threads);
      for (size_t i = 0; i < FLAGS_num_threads; i++) {
        threads[i] = std::thread(bench_seq_write_tput, pbuf, i, copy_sz,
                                 &avg_tput_GBps[i]);
        bind_to_core(threads[i], kNumaNode, i);
      }
      for (auto &t : threads) t.join();

      double total_tput = 0.0;
      for (size_t i = 0; i < FLAGS_num_threads; i++)
        total_tput += avg_tput_GBps[i];
      dat_data << std::setprecision(2) << total_tput << " ";
    }

    printf("%s\n", dat_header.str().c_str());
    printf("%s\n", dat_data.str().c_str());
  }

  // Sequential write latency
  if (bench_func == "bench_seq_write_latency") {
    printf("Sequential write latency. One thread only!\n");
    bench_seq_write_latency(pbuf);
  }

  // Random write latency
  if (bench_func == "bench_rand_write_latency") {
    printf("Random write latency. One thread only!\n");
    bench_rand_write_latency(pbuf);
  }

  // Random read latency
  if (bench_func == "bench_rand_read_latency") {
    printf("Random read latency. One thread only!\n");
    bench_rand_read_latency(pbuf);
  }

  // Random write tput
  if (bench_func == "bench_rand_write_tput") {
    std::vector<size_t> thread_count = {1};
    std::vector<size_t> copy_sz_vec = {256};

    for (size_t copy_sz : copy_sz_vec) {
      for (size_t num_threads : thread_count) {
        printf("Rand write tput with %zu threads, copy_sz %zu\n", num_threads,
               copy_sz);
        std::vector<std::thread> threads(num_threads);

        for (size_t i = 0; i < num_threads; i++) {
          threads[i] =
              std::thread(bench_rand_write_tput, pbuf, i, copy_sz, num_threads);
        }

        for (size_t i = 0; i < num_threads; i++) threads[i].join();
      }
    }
  }

  // Random read throughput
  if (bench_func == "bench_rand_read_tput") {
    std::vector<size_t> thread_count = {1, 2, 4, 8, 16, 24, 48};
    std::vector<size_t> copy_sz_vec = {64, 256, 512, 1024};

    for (size_t copy_sz : copy_sz_vec) {
      for (size_t num_threads : thread_count) {
        printf("Rand read tput with %zu threads, copy_sz %zu\n", num_threads,
               copy_sz);
        std::vector<std::thread> threads(num_threads);

        for (size_t i = 0; i < num_threads; i++) {
          threads[i] =
              std::thread(bench_rand_read_tput, pbuf, i, copy_sz, num_threads);
        }

        for (size_t i = 0; i < num_threads; i++) threads[i].join();
      }
    }
  }

  // Sequential read throughput
  if (bench_func == "bench_seq_read_tput") {
    std::vector<size_t> thread_count = {1, 2, 4, 8, 16, 24, 48};

    for (size_t num_threads : thread_count) {
      printf("Seq read tput with %zu threads\n", num_threads);
      std::vector<std::thread> threads(num_threads);

      for (size_t i = 0; i < num_threads; i++) {
        threads[i] = std::thread(bench_seq_read_tput, pbuf, i, num_threads);
        bind_to_core(threads[i], kNumaNode, i);
      }

      for (size_t i = 0; i < num_threads; i++) threads[i].join();
    }
  }

  is_pmem_file_devdax() ? munmap(pbuf, kPmemFileSize)
                        : pmem_unmap(pbuf, kPmemFileSize);
  exit(0);
}
