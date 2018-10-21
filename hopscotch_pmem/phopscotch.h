#pragma once

#include <city.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include <assert.h>
#include <city.h>
#include <libpmem.h>
#include <stdexcept>
#include <string>
#include <vector>
#include "huge_alloc.h"

namespace phopscotch {

static constexpr size_t kBitmapSize = 16;  // Neighborhood size

// During insert(), we will look for an empty slot at most kMaxDistance away
// from the key's hash bucket.
//
// We provision the table with kMaxDistance extra buckets at the end. These
// extra buckets are never direcly mapped, so their hopinfo is zero.
static constexpr size_t kMaxDistance = 1024;

static constexpr size_t kMaxBatchSize = 16;
static constexpr size_t kNumRedoLogEntries = kMaxBatchSize * 8;
static constexpr bool kVerbose = false;
static constexpr size_t kNumaNode = 0;

/// Check a condition at runtime. If the condition is false, throw exception.
static inline void rt_assert(bool condition, std::string throw_str) {
  if (!condition) throw std::runtime_error(throw_str);
}

// Aligns 64b input parameter to the next power of 2
static uint64_t rte_align64pow2(uint64_t v) {
  v--;
  v |= v >> 1;
  v |= v >> 2;
  v |= v >> 4;
  v |= v >> 8;
  v |= v >> 16;
  v |= v >> 32;

  return v + 1;
}

template <typename T>
static constexpr bool is_power_of_two(T x) {
  return x && ((x & T(x - 1)) == 0);
}

template <uint64_t PowerOfTwoNumber, typename T>
static constexpr T roundup(T x) {
  static_assert(is_power_of_two(PowerOfTwoNumber),
                "PowerOfTwoNumber must be a power of 2");
  return ((x) + T(PowerOfTwoNumber - 1)) & (~T(PowerOfTwoNumber - 1));
}

template <typename Key, typename Value>
class HashMap {
 public:
  class Bucket {
   public:
    Key key;
    Value value;

    // Bit i (i >= 0) in hopinfo is one iff the entry at distance i from this
    // bucket maps to this bucket.
    size_t hopinfo;

    Bucket(Key key, Value value) : key(key), value(value), hopinfo(0) {}
    Bucket() {}

    // Return true if bit #idx is set in hopinfo
    inline bool is_set(size_t idx) { return (hopinfo & (1ull << idx)) > 0; }

    std::string to_string() {
      char buf[1000];
      sprintf(buf, "[key %zu, value %zu, hopinfo 0x%x]", key, value, hopinfo);
      return std::string(buf);
    }
  };
  static_assert(sizeof(Bucket::hopinfo) * 8 >= kBitmapSize, "");

  // A redo log entry is committed iff its sequence number is less than or equal
  // to the committed_seq_num.
  class RedoLogEntry {
   public:
    size_t seq_num;  // Sequence number of this entry. Zero is invalid.
    Key key;
    Value value;

    char padding[128 - (sizeof(seq_num) + sizeof(key) + sizeof(value))];

    RedoLogEntry(size_t seq_num, const Key* key, const Value* value)
        : seq_num(seq_num), key(*key), value(*value) {}
    RedoLogEntry() {}
  };

  class RedoLog {
   public:
    RedoLogEntry entries[kNumRedoLogEntries];
    size_t committed_seq_num;
  };

  // Initialize the persistent buffer for this hash table. This modifies only
  // mapped_len.
  uint8_t* map_pbuf(size_t& _mapped_len) const {
    int is_pmem;
    uint8_t* pbuf = reinterpret_cast<uint8_t*>(
        pmem_map_file(pmem_file.c_str(), 0 /* length */, 0 /* flags */, 0666,
                      &_mapped_len, &is_pmem));

    rt_assert(pbuf != nullptr, "pmem_map_file() failed for " + pmem_file);
    rt_assert(reinterpret_cast<size_t>(pbuf) % 256 == 0, "pbuf not aligned");

    if (mapped_len - file_offset < reqd_space) {
      fprintf(stderr,
              "pmem file too small. %.2f GB required for hash table "
              "(%zu buckets, bucket size = %zu), but only %.2f GB available\n",
              reqd_space * 1.0 / (1ull << 30), num_buckets, sizeof(Bucket),
              mapped_len * 1.0 / (1ull << 30));
    }
    rt_assert(is_pmem == 1, "File is not pmem");

    return pbuf + file_offset;
  }

  HashMap(std::string pmem_file, size_t file_offset, size_t num_requested_keys)
      : pmem_file(pmem_file),
        file_offset(file_offset),
        num_requested_keys(num_requested_keys),
        num_buckets(rte_align64pow2(num_requested_keys)),
        reqd_space(get_required_bytes(num_requested_keys)),
        invalid_key(get_invalid_key()) {
    rt_assert(num_requested_keys >= 1, ">=1 buckets needed");
    rt_assert(file_offset % 256 == 0, "Unaligned file offset");

    pbuf = map_pbuf(mapped_len);

    // Set the committed seq num, and all redo log entry seq nums to zero.
    redo_log = reinterpret_cast<RedoLog*>(pbuf);
    pmem_memset_persist(redo_log, 0, sizeof(RedoLog));

    // Initialize buckets
    size_t bucket_offset = roundup<256>(sizeof(RedoLog));
    buckets = reinterpret_cast<Bucket*>(&pbuf[bucket_offset]);

    reset();
  }

  ~HashMap() {
    if (pbuf != nullptr) pmem_unmap(pbuf - file_offset, mapped_len);
  }

  // Initialize the contents of both regular and extra buckets
  void reset() {
    double GB_to_memset = num_buckets * sizeof(Bucket) * 1.0 / (1ull << 30);
    printf("Resetting hash table. This might take a while (~ %.1f seconds)\n",
           GB_to_memset / 3.0);

    pmem_memset_persist(&buckets[0], 0, num_buckets * sizeof(Bucket));
  }

  void prefetch(uint64_t key_hash) const {
    if (!opts.prefetch) return;

    size_t bucket_index = key_hash & (num_buckets - 1);
    const Bucket* bucket = &buckets[bucket_index];

    // Prefetching two cache lines seems to works best
    __builtin_prefetch(bucket, 0, 0);
    __builtin_prefetch(reinterpret_cast<const char*>(bucket) + 64, 0, 0);
  }

  // Batched operation that takes in both GETs and SETs. When this function
  // returns, all SETs are persistent in the log.
  //
  // For GETs, value_arr slots contain results. For SETs, they contain the value
  // to SET. This version of batch_op_drain assumes that the caller hash already
  // issued prefetches.
  void batch_op_drain_helper(bool* is_set, size_t* keyhash_arr,
                             const Key** key_arr, Value** value_arr,
                             bool* success_arr, size_t n) {
    for (size_t i = 0; i < n; i++) {
      if (is_set[i]) {
        success_arr[i] = set_nodrain(keyhash_arr[i], key_arr[i], value_arr[i]);
      } else {
        success_arr[i] = get(keyhash_arr[i], key_arr[i], value_arr[i]);
      }
    }
  }

  // Batched operation that takes in both GETs and SETs. When this function
  // returns, all SETs are persistent in the log.
  //
  // For GETs, value_arr slots contain results. For SETs, they contain the value
  // to SET. This version of batch_op_drain issues prefetches for the caller.
  inline void batch_op_drain(bool* is_set, const Key** key_arr,
                             Value** value_arr, bool* success_arr, size_t n) {
    size_t keyhash_arr[kMaxBatchSize];

    for (size_t i = 0; i < n; i++) {
      keyhash_arr[i] = get_hash(key_arr[i]);
      prefetch(keyhash_arr[i]);
    }

    batch_op_drain_helper(is_set, keyhash_arr, key_arr, value_arr, success_arr,
                          n);
  }

  bool get(const Key* key, Value* out_value) const {
    assert(*key != invalid_key);
    return get(get_hash(key), key, out_value);
  }

  bool get(size_t key_hash, const Key* key, Value* out_value) const {
    size_t bucket_idx = key_hash & (num_buckets - 1);

    if (kVerbose) {
      printf("get: key %zu, bucket_idx %zu\n", to_size_t_key(key), bucket_idx);
    }

    for (size_t i = bucket_idx; i < bucket_idx + kBitmapSize; i++) {
      if (buckets[bucket_idx].is_set(i - bucket_idx)) {
        if (memcmp(key, &buckets[i].key, sizeof(Key)) == 0) {
          if (kVerbose) printf("  found at bucket %zu\n", i);
          *out_value = buckets[i].value;
          return true;
        }
      }
    }
    return false;
  }

  // Set a key-value item without a final sfence
  bool set_nodrain(const Key* key, const Value* value) {
    assert(*key != invalid_key);
    return set_nodrain(get_hash(key), key, value);
  }

  bool set_nodrain(size_t keyhash, const Key* key, const Value* value) {
    const size_t start_bkt_idx = keyhash & (num_buckets - 1);
    Bucket* start_bkt = &buckets[start_bkt_idx];

    if (kVerbose) {
      printf("set: key %zu, value %zu, bucket %zu\n", to_size_t_key(key),
             to_size_t_val(value), start_bkt_idx);
    }

    // In-place update if the key exists already
    for (size_t i = 0; i < kBitmapSize; i++) {
      if (start_bkt->is_set(i)) {
        Bucket* test_bkt = (start_bkt + i);
        if (memcmp(key, &test_bkt->key, sizeof(Key)) == 0) {
          if (kVerbose) printf("  inserting at bucket %zu\n", start_bkt + i);
          test_bkt->value = *value;
          return true;
        }
      }
    }

    // Linear probing to find an empty bucket
    Bucket* free_bkt = start_bkt;
    for (size_t d_start_free = 0; d_start_free < kMaxDistance; d_start_free++) {
      if (free_bkt->key == invalid_key) break;
      free_bkt++;
    }

    if (free_bkt == start_bkt + kMaxDistance) {
      if (kVerbose) printf("  free bucket over max distance. failing.\n");
      return false;
    }

    while (true) {
      if (free_bkt - start_bkt < kBitmapSize) {
        if (kVerbose) {
          printf("  finally using bucket %zu\n", free_bkt - buckets);
        }

        start_bkt->hopinfo |= (1ull << (free_bkt - start_bkt));
        free_bkt->value = *value;
        free_bkt->key = *key;
        return true;
      }

      if (kVerbose) {
        printf("  free bucket %zu too far.\n", free_bkt - buckets);
      }

      // Else, try to move free_bkt closer
      Bucket* swap_bkt = nullptr;

      // d_pivot_free = distance of free_bkt from pivot_bkt
      for (size_t d_pivot_free = kBitmapSize - 1; d_pivot_free > 0;
           d_pivot_free--) {
        Bucket* pivot_bkt = free_bkt - d_pivot_free;

        // Check if any entry in [pivot_bkt, ..., free_bkt - 1] maps to
        // pivot_bkt. Such an entry can be moved to free_bkt.
        for (size_t d_pivot_swap = 0; d_pivot_swap < d_pivot_free;
             d_pivot_swap++) {
          if (pivot_bkt->is_set(d_pivot_swap)) {
            swap_bkt = pivot_bkt + d_pivot_swap;
            break;
          }
        }

        if (swap_bkt != nullptr) {
          if (kVerbose) printf("  swap with bkt %zu\n", (swap_bkt - buckets));

          // We found a swap
          free_bkt->key = swap_bkt->key;
          free_bkt->value = swap_bkt->value;

          swap_bkt->key = invalid_key;

          pivot_bkt->hopinfo |= (1ull << (free_bkt - pivot_bkt));
          pivot_bkt->hopinfo &= ~(1ull << (swap_bkt - pivot_bkt));

          free_bkt = swap_bkt;
          break;
        }
      }  // End search over pivot buckets

      if (swap_bkt == nullptr) {
        // If no pivot bucket found, insert failed
        if (kVerbose) printf("  no pivot bucket found\n");
        return false;
      }
    }
  }

  /// Return the total bytes required for a table with \p num_requested_keys
  /// keys. The returned space includes redo log. The returned space is aligned
  /// to 256 bytes.
  static size_t get_required_bytes(size_t num_requested_keys) {
    size_t num_buckets = rte_align64pow2(num_requested_keys);
    size_t tot_size =
        sizeof(RedoLog) + (num_buckets + kMaxDistance) * sizeof(Bucket);
    return roundup<256>(tot_size);
  }

  static size_t get_hash(const Key* k) {
    return CityHash64(reinterpret_cast<const char*>(k), sizeof(Key));
  }

  static Key get_invalid_key() {
    Key ret;
    memset(&ret, 0, sizeof(ret));
    return ret;
  }

  // Convert a key to a 64-bit value for printing
  static size_t to_size_t_key(const Key* k) {
    return *reinterpret_cast<const size_t*>(k);
  }

  // Convert a value to a 64-bit value for printing
  static size_t to_size_t_val(const Value* v) {
    return *reinterpret_cast<const size_t*>(v);
  }

  void print_buckets() const {
    for (size_t i = 0; i < num_buckets; i++) {
      printf("bucket %zu: %s\n", i, buckets[i].to_string().c_str());
    }
  }

  // Constructor args
  const std::string pmem_file;      // Name of the pmem file
  const size_t file_offset;         // Offset in file where the table is placed
  const size_t num_requested_keys;  // User's requested key capacity

  const size_t num_buckets;  // Total buckets
  const size_t reqd_space;   // Total bytes needed for the table
  const Key invalid_key;

  Bucket* buckets = nullptr;

  uint8_t* pbuf;      // The pmem buffer for this table
  size_t mapped_len;  // The length mapped by libpmem
  RedoLog* redo_log;
  size_t cur_sequence_number = 1;

  struct {
    bool prefetch = true;     // Software prefetching
    bool redo_batch = true;   // Redo log batching
    bool async_drain = true;  // Drain slot writes asynchronously

    void reset() {
      prefetch = true;
      redo_batch = true;
      async_drain = true;
    }
  } opts;
};

}  // namespace phopscotch
