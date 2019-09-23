/**
 * @file bench.h
 * @brief Common code shared by benchmark implementations in header files
 */

#pragma once

#include <errno.h>
#include <gflags/gflags.h>
#include <libpmem.h>
#include <malloc.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <iomanip>
#include <pcg/pcg_random.hpp>
#include <sstream>
#include <thread>

#include "../common.h"
#include "../utils/timer.h"

DEFINE_uint64(num_threads, 0, "Number of threads");

static constexpr const char *kPmemFile = "/mnt/pmem12/raft_log";
static constexpr size_t kPmemFileSizeGB = 512;  // The expected file size
static constexpr size_t kPmemFileSize = kPmemFileSizeGB * GB(1);

static constexpr bool kMeasureLatency = false;
double freq_ghz = 0.0;
static size_t align64(size_t x) { return x - x % 64; }

static constexpr int kHdrPrecision = 2;          // Precision for hdr histograms
static constexpr int kMinPmemLatCycles = 1;      // Min pmem latency in cycles
static constexpr int kMaxPmemLatCycles = MB(1);  // Max pmem latency in cycles

static constexpr size_t kNumaNode = 0;

/// Get a random offset in the file with at least \p space after it
size_t get_random_offset_with_space(pcg64_fast &pcg, size_t space) {
  size_t iters = 0;
  while (true) {
    size_t rand_offset = pcg() % kPmemFileSize;
    if (kPmemFileSize - rand_offset > space) return rand_offset;
    iters++;
    if (iters > 2) printf("Random offset took over 2 iters\n");
  }
}
