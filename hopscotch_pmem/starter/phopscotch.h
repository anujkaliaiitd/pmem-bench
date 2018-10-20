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

static constexpr size_t kBitmapSize = 8;
static constexpr size_t kMaxBatchSize = 16;
static constexpr size_t kNumRedoLogEntries = kMaxBatchSize * 8;
static constexpr bool kVerbose = true;
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

    // Bit i (i > 0) in hopinfo is one iff the entry at distance i also maps
    // to this bucket. Bit 0 is unused.
    uint32_t hopinfo;

    Bucket(Key key, Value value) : key(key), value(value), hopinfo(0) {}
    Bucket() {}

    std::string to_string() {
      char buf[1000];
      sprintf(buf, "[key %zu, value %zu, hopinfo %x]", key, value, hopinfo);
      return std::string(buf);
    }
  };
  static_assert(sizeof(Bucket::hopinfo * 8 >= kBitmapSize), "");

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
              reqd_space * 1.0 / (1ull << 30), num_total_buckets,
              sizeof(Bucket), mapped_len * 1.0 / (1ull << 30));
    }
    rt_assert(is_pmem == 1, "File is not pmem");

    return pbuf + file_offset;
  }

  HashMap(std::string pmem_file, size_t file_offset, size_t num_requested_keys)
      : pmem_file(pmem_file),
        file_offset(file_offset),
        num_requested_keys(num_requested_keys),
        num_total_buckets(rte_align64pow2(num_requested_keys)),
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
    double GB_to_memset =
        num_total_buckets * sizeof(Bucket) * 1.0 / (1ull << 30);
    printf("Resetting hash table. This might take a while (~ %.1f seconds)\n",
           GB_to_memset / 3.0);

    pmem_memset_persist(&buckets[0], 0, num_total_buckets * sizeof(Bucket));
  }

  bool get(const Key* key, Value* out_value) {
    size_t keyhash =
        CityHash64(reinterpret_cast<const char*>(key), sizeof(Key));
    size_t bucket_idx = keyhash & (num_total_buckets - 1);

    if (kVerbose) {
      printf("get: key %zu, bucket_idx %zu\n", to_size_t_key(key), bucket_idx);
    }

    for (size_t i = bucket_idx; i < bucket_idx + kBitmapSize; i++) {
      if (i == bucket_idx || buckets[i].hopinfo & (1 << (i - bucket_idx))) {
        if (memcmp(key, &buckets[i].key, sizeof(Key)) == 0) {
          if (kVerbose) printf("  found at bucket %zu\n", i);
          *out_value = buckets[i].value;
          return true;
        }
      }
    }
    return false;
  }

  bool set(Key* key, Value* value) {
    const size_t keyhash =
        CityHash64(reinterpret_cast<const char*>(key), sizeof(Key));
    const size_t bucket_idx = keyhash & (num_total_buckets - 1);

    if (kVerbose) {
      printf("set: key %zu, value %zu, bucket_idx %zu\n", to_size_t_key(key),
             to_size_t_val(value), bucket_idx);
    }

    for (size_t i = bucket_idx; i < bucket_idx + kBitmapSize; i++) {
      if (i == bucket_idx || buckets[i].hopinfo & (1 << (i - bucket_idx))) {
        if (memcmp(key, &buckets[i].key, sizeof(Key)) == 0) {
          printf("  inserting at bucket %zu\n", i);
          buckets[i].value = *value;
          return true;
        }
      }
    }

    // Linear probing to find an empty bucket
    for (size_t i = bucket_idx; i < num_total_buckets; i++) {
      if (buckets[i].key == invalid_key) {
        if (kVerbose) printf("  bucket %zu is empty\n", i);

        // Found an available bucket
        while (i - bucket_idx >= kBitmapSize) {
          if (kVerbose) printf("    bucket %zu is too far\n", i);

          size_t dist;  // Distance from bucket i
          for (dist = 1; dist < kBitmapSize; dist++) {
            if (buckets[i - dist].hopinfo) {
              // If we are here, there is an additional entry that maps to
              // bucket (i - dist). (i - dist + off) is the closest entry that
              // maps to (i - dist).
              const size_t off =
                  static_cast<size_t>(__builtin_ctz(buckets[i - dist].hopinfo));
              assert(off > 0);

              if (off >= dist) continue;  // The closest entry is too far

              if (kVerbose) {
                printf("    moving to closer bucket\n", i - dist + off);
              }

              // If we are here, (i - dist + off) < i. We swap (i - dist + off)
              // with i. Since entry at (i - dist + off) maps to bucket
              // (i - dist), we need to update the bitmap at (i - dist).
              buckets[i].key = buckets[i - dist + off].key;
              buckets[i].value = buckets[i - dist + off].value;

              buckets[i - dist + off].key = invalid_key;

              // (i - dist + off) is now empty.
              // (i - dist + dist) is now full.
              buckets[i - dist].hopinfo &= ~(1ull << off);
              buckets[i - dist].hopinfo |= (1ull << dist);
              i = i - dist + off;
              break;
            }
          }
          if (dist >= kBitmapSize) {
            if (kVerbose) printf("    giving up\n");
            return false;
          }
        }

        // Here, i - bucket_idx < kBitmapSize
        if (kVerbose) printf("  finally using bucket %zu\n", i);
        buckets[i].key = *key;
        buckets[i].value = *value;

        const size_t off = i - bucket_idx;
        if (off != 0) buckets[bucket_idx].hopinfo |= (1ull << off);
        return true;
      }
    }

    // Found no empty bucket
    return 0;
  }

  /// Return the total bytes required for a table with \p num_requested_keys
  /// keys. The returned space includes redo log. The returned space is aligned
  /// to 256 bytes.
  static size_t get_required_bytes(size_t num_requested_keys) {
    size_t num_total_buckets = rte_align64pow2(num_requested_keys);
    size_t tot_size = sizeof(RedoLog) + num_total_buckets * sizeof(Bucket);
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
    for (size_t i = 0; i < num_total_buckets; i++) {
      printf("bucket %zu: %s\n", i, buckets[i].to_string().c_str());
    }
  }

  // Constructor args
  const std::string pmem_file;      // Name of the pmem file
  const size_t file_offset;         // Offset in file where the table is placed
  const size_t num_requested_keys;  // User's requested key capacity

  const size_t num_total_buckets;  // Total buckets
  const size_t reqd_space;         // Total bytes needed for the table
  const Key invalid_key;

  Bucket* buckets = nullptr;

  uint8_t* pbuf;      // The pmem buffer for this table
  size_t mapped_len;  // The length mapped by libpmem
  RedoLog* redo_log;
  size_t cur_sequence_number = 1;
};

}  // namespace phopscotch
