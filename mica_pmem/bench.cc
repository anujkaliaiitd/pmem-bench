#include <gflags/gflags.h>
#include <condition_variable>
#include <map>
#include <mutex>
#include <pcg/pcg_random.hpp>
#include "../common.h"
#include "mica_pmem.h"

DEFINE_uint64(table_key_capacity, MB(1), "Number of keys in table per thread");
DEFINE_uint64(batch_size, pmica::kMaxBatchSize, "Batch size");
DEFINE_string(benchmark, "get", "Benchmark to run");
DEFINE_uint64(num_threads, 1, "Number of threads");

static constexpr double kDefaultOverhead = 0.2;
static constexpr double kNumaNode = 0;

// MICA's ``small'' workload: 16-byte keys and 64-byte values
class Key {
 public:
  size_t key_frag[2];
  bool operator==(const Key &rhs) { return memcmp(this, &rhs, sizeof(Key)); }
  bool operator!=(const Key &rhs) { return !memcmp(this, &rhs, sizeof(Key)); }
  Key() { memset(key_frag, 0, sizeof(Key)); }
};

class Value {
 public:
  size_t val_frag[8];
  Value() { memset(val_frag, 0, sizeof(Value)); }
};

// Used for threads to wait to begin work
// https://stackoverflow.com/questions/24465533/implementing-boostbarrier-in-c11
class Barrier {
 private:
  std::mutex mutex;
  std::condition_variable cv;
  std::size_t count;

 public:
  explicit Barrier(std::size_t count) : count{count} {}
  void wait() {
    std::unique_lock<std::mutex> lock{mutex};
    if (--count == 0) {
      cv.notify_all();
    } else {
      cv.wait(lock, [this] { return count == 0; });
    }
  }
};
Barrier *barrier;

/// Given a random number \p rand, return a random number
static inline uint64_t fastrange64(uint64_t rand, uint64_t n) {
  return static_cast<uint64_t>(
      static_cast<__uint128_t>(rand) * static_cast<__uint128_t>(n) >> 64);
}

typedef pmica::HashMap<Key, Value> HashMap;

size_t populate(HashMap *hashmap, size_t thread_id) {
  bool is_set_arr[pmica::kMaxBatchSize];
  Key key_arr[pmica::kMaxBatchSize];
  Value val_arr[pmica::kMaxBatchSize];
  bool success_arr[pmica::kMaxBatchSize];

  size_t num_success = 0;

  size_t progress_console_lim = FLAGS_table_key_capacity / 10;

  for (size_t i = 1; i <= FLAGS_table_key_capacity; i += pmica::kMaxBatchSize) {
    for (size_t j = 0; j < pmica::kMaxBatchSize; j++) {
      is_set_arr[j] = true;
      key_arr[j].key_frag[0] = i + j;
      val_arr[j].val_frag[0] = i + j;
    }

    hashmap->batch_op_drain(is_set_arr, key_arr, val_arr, success_arr,
                            pmica::kMaxBatchSize);

    if (i >= progress_console_lim) {
      printf("thread %zu: %.2f percent done\n", thread_id,
             i * 1.0 / FLAGS_table_key_capacity);
      progress_console_lim += FLAGS_table_key_capacity / 10;
    }

    for (size_t j = 0; j < pmica::kMaxBatchSize; j++) {
      num_success += success_arr[j];
      if (!success_arr[j]) return num_success;
    }
  }

  return FLAGS_table_key_capacity;  // All keys were added
}

enum class Workload { kGets, kSets, k5050 };
double batch_exp(HashMap *hashmap, size_t max_key, size_t batch_size,
                 Workload workload) {
  pcg64_fast pcg(pcg_extras::seed_seq_from<std::random_device>{});
  constexpr size_t kNumIters = MB(1);

  struct timespec start;
  bool is_set_arr[pmica::kMaxBatchSize];
  Key key_arr[pmica::kMaxBatchSize];
  Value val_arr[pmica::kMaxBatchSize];
  bool success_arr[pmica::kMaxBatchSize];
  clock_gettime(CLOCK_REALTIME, &start);

  size_t num_success = 0;
  for (size_t i = 1; i <= kNumIters; i += batch_size) {
    for (size_t j = 0; j < batch_size; j++) {
      key_arr[j].key_frag[0] = fastrange64(pcg(), max_key);
      val_arr[j].val_frag[0] = key_arr[j].key_frag[0];

      switch (workload) {
        case Workload::kGets: is_set_arr[j] = false; break;
        case Workload::kSets: is_set_arr[j] = true; break;
        case Workload::k5050: is_set_arr[j] = pcg() % 2 == 0; break;
      }
    }

    hashmap->batch_op_drain(is_set_arr, key_arr, val_arr, success_arr,
                            batch_size);

    for (size_t j = 0; j < batch_size; j++) num_success += success_arr[j];
  }

  double seconds = sec_since(start);
  double tput = kNumIters / (seconds * 1000000);
  return tput;
}

void thread_func(size_t thread_id) {
  size_t bytes_per_map =
      HashMap::get_required_bytes(FLAGS_table_key_capacity, kDefaultOverhead);
  bytes_per_map = roundup<256>(bytes_per_map);

  auto *hashmap = new HashMap("/dev/dax12.0", thread_id * bytes_per_map,
                              FLAGS_table_key_capacity, kDefaultOverhead);

  printf("thread %zu: Populating hashmap. Expected time = %.1f seconds\n",
         thread_id, FLAGS_table_key_capacity / (4.0 * 1000000));  // 4 M/s

  size_t max_key = populate(hashmap, thread_id);

  std::vector<double> tput_vec;
  Workload workload;
  if (FLAGS_benchmark == "set") workload = Workload::kSets;
  if (FLAGS_benchmark == "get") workload = Workload::kGets;
  if (FLAGS_benchmark == "5050") workload = Workload::k5050;

  printf("thread %zu, done populating. waiting for others.\n", thread_id);
  barrier->wait();
  printf("thread %zu, starting work.\n", thread_id);

  for (size_t i = 0; i < 10; i++) {
    double tput = batch_exp(hashmap, max_key, FLAGS_batch_size, workload);
    printf("thread %zu, iter %zu: tput = %.2f\n", thread_id, i, tput);
    tput_vec.push_back(tput);
  }

  double avg_tput =
      std::accumulate(tput_vec.begin(), tput_vec.end(), 0.0) / tput_vec.size();
  double _stddev = stddev(tput_vec);

  printf("thread %zu of %zu final M/s: %.2f avg, %.2f stddev\n", thread_id,
         FLAGS_num_threads, avg_tput, _stddev);

  delete hashmap;
}

void sweep_do_one(HashMap *hashmap, size_t max_key, size_t batch_size,
                  Workload workload) {
  std::vector<double> tput_vec;

  for (size_t i = 0; i < 10; i++) {
    double tput;
    tput = batch_exp(hashmap, max_key, batch_size, workload);
    tput_vec.push_back(tput);
  }

  double avg_tput =
      std::accumulate(tput_vec.begin(), tput_vec.end(), 0.0) / tput_vec.size();
  double _stddev = stddev(tput_vec);

  printf("  Tput (M/s) = %.2f avg, %.2f stddev\n", avg_tput, _stddev);
}

// Measure the effectiveness of optimizations with one thread
void sweep_optimizations() {
  auto *hashmap =
      new HashMap("/dev/dax0.0", 0, FLAGS_table_key_capacity, kDefaultOverhead);

  printf("Populating hashmap. Expected time = %.1f seconds\n",
         FLAGS_table_key_capacity / (4.0 * 1000000));  // 4 M/s

  size_t max_key = populate(hashmap, 0 /* thread_id */);

  std::vector<size_t> batch_size_vec = {1, 4, 8, 16};

  // GET batch sizes
  for (auto &batch_size : batch_size_vec) {
    printf("get. Batch size %zu\n", batch_size);
    sweep_do_one(hashmap, max_key, batch_size, Workload::kGets);
  }

  // SET batch sizes
  for (auto &batch_size : batch_size_vec) {
    printf("set. Batch size %zu\n", batch_size);
    sweep_do_one(hashmap, max_key, batch_size, Workload::kSets);
  }

  // 50/50 batch sizes
  for (auto &batch_size : batch_size_vec) {
    printf("50/50. Batch size %zu\n", batch_size);
    sweep_do_one(hashmap, max_key, batch_size, Workload::k5050);
  }

  // Optimizations for GETs
  hashmap->opts.prefetch = false;
  printf("get. Batch size 16, no prefetch.\n");
  sweep_do_one(hashmap, max_key, 16, Workload::kGets);
  hashmap->opts.reset();

  // Optimizations for SETs, and 50/50
  hashmap->opts.redo_batch = false;
  printf("set. Batch size 16, only redo batch disabled\n");
  sweep_do_one(hashmap, max_key, 16, Workload::kSets);
  printf("50/50. Batch size 16, only redo batch disabled\n");
  sweep_do_one(hashmap, max_key, 16, Workload::k5050);
  hashmap->opts.reset();

  hashmap->opts.prefetch = false;
  printf("set. Batch size 16, only prefetch disabled.\n");
  sweep_do_one(hashmap, max_key, 16, Workload::kSets);
  printf("50/50. Batch size 16, only prefetch disabled\n");
  sweep_do_one(hashmap, max_key, 16, Workload::k5050);
  hashmap->opts.reset();

  hashmap->opts.async_drain = false;
  printf("set. Batch size 16, only async slot drain disabled.\n");
  sweep_do_one(hashmap, max_key, 16, Workload::kSets);
  printf("50/50. Batch size 16, only async slot drain disabled.\n");
  sweep_do_one(hashmap, max_key, 16, Workload::k5050);
  hashmap->opts.reset();

  delete hashmap;
}

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  // sweep_optimizations();
  // exit(0);

  barrier = new Barrier(FLAGS_num_threads);
  std::vector<std::thread> threads(FLAGS_num_threads);

  printf("Launching %zu threads\n", FLAGS_num_threads);
  for (size_t i = 0; i < FLAGS_num_threads; i++) {
    threads[i] = std::thread(thread_func, i);
    bind_to_core(threads[i], kNumaNode, i);
  }

  for (size_t i = 0; i < FLAGS_num_threads; i++) {
    threads[i].join();
  }

  delete barrier;
}
