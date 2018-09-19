#include "main.h"
#include "../common.h"

// Benchmark impl
#include "seq_write_tput.h"

void bench_seq_read_tput(uint8_t *pbuf, size_t thread_id) {
  _unused(pbuf);
  _unused(thread_id);
}

/// Random read latency
void bench_rand_read_lat(uint8_t *pbuf, size_t thread_id) {
  static constexpr size_t kNumIters = MB(1);

  pcg64_fast pcg(pcg_extras::seed_seq_from<std::random_device>{});
  struct timespec start;
  size_t sum = 0;

  while (true) {
    clock_gettime(CLOCK_REALTIME, &start);

    for (size_t i = 0; i < kNumIters; i++) {
      // Choose a random dependent byte
      size_t rand_addr = (pcg() + sum % 8) % kPmemFileSize;
      sum += pbuf[rand_addr];
    }

    double tot_ns = ns_since(start);
    printf("Thread %zu: Random read latency = %.2f ns. Sum = %zu\n", thread_id,
           tot_ns / kNumIters, sum);
  }
}

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

/// Random write throughput
void bench_rand_write_tput(uint8_t *pbuf, size_t thread_id) {
  static constexpr size_t kBatchSize = 10;
  static constexpr size_t kNumIters = MB(4);

  // Write to non-overlapping addresses
  const size_t bytes_per_thread = kPmemFileSize / FLAGS_num_threads;
  const size_t base_addr = thread_id * bytes_per_thread;

  pcg64_fast pcg(pcg_extras::seed_seq_from<std::random_device>{});
  struct timespec start;

  while (true) {
    clock_gettime(CLOCK_REALTIME, &start);

    for (size_t i = 0; i < kNumIters / kBatchSize; i++) {
      size_t offset[kBatchSize];
      for (size_t j = 0; j < kBatchSize; j++) {
        offset[j] = base_addr + (pcg() % bytes_per_thread);
        offset[j] = align64(offset[j]);
        pmem_memset_nodrain(&pbuf[offset[j]], i + j, 64);
      }

      pmem_drain();
    }

    double tot_sec = sec_since(start);
    double cacheline_rate = kNumIters / tot_sec;
    printf("Thread %zu: random write tput = %.2f M/sec, %.2f GB/s\n", thread_id,
           cacheline_rate / 1000000, (cacheline_rate * 64) / 1000000000);
  }
}

/// Random write latency
void bench_rand_write_lat(uint8_t *pbuf, size_t thread_id) {
  static constexpr size_t kNumIters = MB(2);
  pcg64_fast pcg(pcg_extras::seed_seq_from<std::random_device>{});

  // Write to non-overlapping addresses
  const size_t bytes_per_thread = kPmemFileSize / FLAGS_num_threads;
  const size_t base_addr = thread_id * bytes_per_thread;

  while (true) {
    size_t ticks_sum = 0;
    for (size_t i = 0; i < kNumIters; i++) {
      size_t ticks_st = rdtsc();
      pmem_memset_persist(&pbuf[base_addr + (pcg() % bytes_per_thread)], i, 64);
      ticks_sum += (rdtscp() - ticks_st);
    }

    printf("Thread %zu: Latency of persistent rand writes = %.2f ns.\n",
           thread_id, ticks_sum / (kNumIters * freq_ghz));
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

  //  nano_sleep(1000000000, 3.0);  // Assume TSC frequency = 3 GHz
  auto bench_func = bench_seq_write_tput;

  // Sequential write
  if (bench_func == bench_seq_write_tput) {
    printf("Sequential write bench. %zu threads\n", FLAGS_num_threads);
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

  pmem_unmap(pbuf, mapped_len);
  exit(0);
}
