#include "main.h"
#include "../common.h"

// Benchmark impl
#include "rand_read_latency.h"
#include "rand_write_latency.h"
#include "rand_write_tput.h"
#include "seq_write_latency.h"
#include "seq_write_tput.h"

/// Random read throughput
void bench_rand_read_tput(uint8_t *pbuf, size_t thread_id) {
  static constexpr size_t kBatchSize = 10;
  static constexpr size_t kNumIters = MB(4);

  pcg64_fast pcg(pcg_extras::seed_seq_from<std::random_device>{});
  struct timespec start;
  size_t sum = 0;

  while (true) {
    clock_gettime(CLOCK_REALTIME, &start);

    for (size_t i = 0; i < kNumIters / kBatchSize; i++) {
      size_t offset[kBatchSize];
      for (size_t j = 0; j < kBatchSize; j++) {
        offset[j] = pcg() % kPmemFileSize;
      }
      for (size_t j = 0; j < kBatchSize; j++) sum += pbuf[offset[j]];
    }

    double tot_sec = sec_since(start);
    printf("Thread %zu: random read tput = %.2f M/sec. Sum = %zu\n", thread_id,
           kNumIters / (tot_sec * 1000000), sum);
  }
}

// Write to the whole file to "map it in", whatever that means
void map_in_file_whole(uint8_t *pbuf) {
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

// Write to a byte in each page of the file, to map the pages in
void map_in_file_by_page(uint8_t *pbuf) {
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

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  uint8_t *pbuf;
  size_t mapped_len;
  int is_pmem;

  freq_ghz = measure_rdtsc_freq();
  printf("RDTSC frequency = %.2f GHz\n", freq_ghz);

  pbuf = reinterpret_cast<uint8_t *>(pmem_map_file(
      kPmemFile, 0 /* length */, 0 /* flags */, 0666, &mapped_len, &is_pmem));

  rt_assert(pbuf != nullptr,
            "pmem_map_file() failed. " + std::string(strerror(errno)));
  rt_assert(mapped_len >= kPmemFileSize,
            "pmem file too small " + std::to_string(mapped_len));
  rt_assert(reinterpret_cast<size_t>(pbuf) % 4096 == 0,
            "Mapped buffer isn't page-aligned");
  rt_assert(is_pmem == 1, "File is not pmem");

  // map_in_file_by_page(pbuf);
  // map_in_file_whole(pbuf);

  std::string bench_func;  // Last one wins
  bench_func = "bench_seq_write_tput";
  bench_func = "bench_seq_write_tput";
  bench_func = "bench_seq_write_latency";
  bench_func = "bench_rand_write_latency";
  bench_func = "bench_seq_read_latency";
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
      double avg_tput_GBps[FLAGS_num_threads];

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
    std::vector<size_t> thread_count = {1, 2, 4, 8, 16, 24};
    std::vector<size_t> copy_sz_vec = {64, 256};

    for (size_t num_threads : thread_count) {
      for (size_t copy_sz : copy_sz_vec) {
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

  pmem_unmap(pbuf, mapped_len);
  exit(0);
}
