#pragma once
#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)

#include <random>
#include <stdexcept>
#include <string>

#define KB(x) (static_cast<size_t>(x) << 10)
#define MB(x) (static_cast<size_t>(x) << 20)
#define GB(x) (static_cast<size_t>(x) << 30)
#define TB(x) (static_cast<size_t>(x) << 40)

static void memory_barrier() { asm volatile("" ::: "memory"); }
static void lfence() { asm volatile("lfence" ::: "memory"); }
static void sfence() { asm volatile("sfence" ::: "memory"); }
static void mfence() { asm volatile("mfence" ::: "memory"); }
static void clflush(volatile void *p) { asm volatile("clflush (%0)" ::"r"(p)); }

class SlowRand {
  std::random_device rand_dev;  // Non-pseudorandom seed for twister
  std::mt19937_64 mt;
  std::uniform_int_distribution<uint64_t> dist;

 public:
  SlowRand() : mt(rand_dev()), dist(0, UINT64_MAX) {}

  inline uint64_t next_u64() { return dist(mt); }
};

class FastRand {
 public:
  uint64_t seed;

  /// Create a FastRand using a seed from SlowRand
  FastRand() {
    SlowRand slow_rand;
    seed = slow_rand.next_u64();
  }

  inline uint32_t next_u32() {
    seed = seed * 1103515245 + 12345;
    return static_cast<uint32_t>(seed >> 32);
  }
};

/// Check a condition at runtime. If the condition is false, throw exception.
static inline void rt_assert(bool condition, std::string throw_str, char *s) {
  if (unlikely(!condition)) {
    throw std::runtime_error(throw_str + std::string(s));
  }
}

/// Check a condition at runtime. If the condition is false, throw exception.
static inline void rt_assert(bool condition, std::string throw_str) {
  if (unlikely(!condition)) throw std::runtime_error(throw_str);
}

/// Check a condition at runtime. If the condition is false, throw exception.
/// This is faster than rt_assert(cond, str) as it avoids string construction.
static inline void rt_assert(bool condition) {
  if (unlikely(!condition)) throw std::runtime_error("Error");
}

/// Return the TSC
static inline size_t rdtsc() {
  uint64_t rax;
  uint64_t rdx;
  asm volatile("rdtsc" : "=a"(rax), "=d"(rdx));
  return static_cast<size_t>((rdx << 32) | rax);
}

static uint64_t rdtscp() {
  uint64_t rax;
  uint64_t rdx;
  uint32_t aux;
  asm volatile("rdtscp" : "=a"(rax), "=d"(rdx), "=c"(aux) : :);
  return (rdx << 32) | rax;
}

static void nano_sleep(size_t ns, double freq_ghz) {
  size_t start = rdtsc();
  size_t end = start;
  size_t upp = static_cast<size_t>(freq_ghz * ns);
  while (end - start < upp) end = rdtsc();
}

static double measure_rdtsc_freq() {
  struct timespec start, end;
  clock_gettime(CLOCK_REALTIME, &start);
  uint64_t rdtsc_start = rdtsc();

  // Do not change this loop! The hardcoded value below depends on this loop
  // and prevents it from being optimized out.
  uint64_t sum = 5;
  for (uint64_t i = 0; i < 1000000; i++) {
    sum += i + (sum + i) * (i % sum);
  }
  rt_assert(sum == 13580802877818827968ull, "Error in RDTSC freq measurement");

  clock_gettime(CLOCK_REALTIME, &end);
  uint64_t clock_ns =
      static_cast<uint64_t>(end.tv_sec - start.tv_sec) * 1000000000 +
      static_cast<uint64_t>(end.tv_nsec - start.tv_nsec);
  uint64_t rdtsc_cycles = rdtsc() - rdtsc_start;

  double _freq_ghz = rdtsc_cycles * 1.0 / clock_ns;
  rt_assert(_freq_ghz >= 0.5 && _freq_ghz <= 5.0, "Invalid RDTSC frequency");

  return _freq_ghz;
}

/// Convert cycles measured by rdtsc with frequence \p freq_ghz to seconds
static double to_sec(size_t cycles, double freq_ghz) {
  return (cycles / (freq_ghz * 1000000000));
}

/// Convert cycles measured by rdtsc with frequence \p freq_ghz to msec
static double to_msec(size_t cycles, double freq_ghz) {
  return (cycles / (freq_ghz * 1000000));
}

/// Convert cycles measured by rdtsc with frequence \p freq_ghz to usec
static double to_usec(size_t cycles, double freq_ghz) {
  return (cycles / (freq_ghz * 1000));
}

static size_t ms_to_cycles(double ms, double freq_ghz) {
  return static_cast<size_t>(ms * 1000 * 1000 * freq_ghz);
}

static size_t us_to_cycles(double us, double freq_ghz) {
  return static_cast<size_t>(us * 1000 * freq_ghz);
}

static size_t ns_to_cycles(double ns, double freq_ghz) {
  return static_cast<size_t>(ns * freq_ghz);
}

/// Convert cycles measured by rdtsc with frequence \p freq_ghz to nsec
static double to_nsec(size_t cycles, double freq_ghz) {
  return (cycles / freq_ghz);
}

/// Return seconds elapsed since timestamp \p t0
static double sec_since(const struct timespec &t0) {
  struct timespec t1;
  clock_gettime(CLOCK_REALTIME, &t1);
  return (t1.tv_sec - t0.tv_sec) + (t1.tv_nsec - t0.tv_nsec) / 1000000000.0;
}

/// Return nanoseconds elapsed since timestamp \p t0
static double ns_since(const struct timespec &t0) {
  struct timespec t1;
  clock_gettime(CLOCK_REALTIME, &t1);
  return (t1.tv_sec - t0.tv_sec) * 1000000000.0 + (t1.tv_nsec - t0.tv_nsec);
}

typedef unsigned long long ticks;
