#pragma once

#include <assert.h>
#include <city.h>
#include <libpmem.h>
#include "../common.h"

namespace mica {

static constexpr size_t kSlotsPerBucket = 8;
static constexpr size_t kMaxBatchSize = 16;
static constexpr size_t kNumRedoLogEntries = kMaxBatchSize * 8;
static constexpr bool kVerbose = 16;

static constexpr bool kUsePmem = true;
static constexpr bool kEnablePrefetch = true;
static constexpr bool kEnableRedoLogging = true;

// Redo logging enabled => use pmem
static_assert(!kUsePmem || kEnableRedoLogging, "");

// These functions allow switching easily between pmem and DRAM
void maybe_pmem_drain() {
  if (kUsePmem) pmem_drain();
}

void maybe_pmem_memcpy_persist(void* dest, const void* src, size_t len) {
  kUsePmem ? pmem_memcpy_persist(dest, src, len) : memcpy(dest, src, len);
}

void maybe_pmem_memcpy_nodrain(void* dest, const void* src, size_t len) {
  kUsePmem ? pmem_memcpy_nodrain(dest, src, len) : memcpy(dest, src, len);
}

void maybe_pmem_memset_persist(void* dest, int c, size_t len) {
  kUsePmem ? pmem_memset_persist(dest, c, len) : memset(dest, c, len);
}

template <typename Key, typename Value>
class HashMap {
 public:
  enum class State : size_t { kEmpty = 0, kFull, kDelete };  // Slot state

  class Slot {
   public:
    Key key;
    Value value;

    Slot(Key key, Value value) : key(key), value(value) {}
    Slot() {}
  };

  struct Bucket {
    size_t next_extra_bucket_idx;  // 1-base; 0 = no extra bucket
    Slot slot_arr[kSlotsPerBucket];
  };

  // A redo log entry is committed iff its sequence number is less than or equal
  // to the committed_seq_num.
  class RedoLogEntry {
   public:
    size_t seq_num;  // Sequence number of this entry. Zero is invalid.
    Key key;
    Value value;

    char padding[128 - (sizeof(seq_num) + sizeof(key) + sizeof(value))];

    RedoLogEntry(size_t seq_num, Key key, Value value)
        : seq_num(seq_num), key(key), value(value) {}
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

    rt_assert(pbuf != nullptr, "pmem_map_file() failed");
    rt_assert(reinterpret_cast<size_t>(pbuf) % 256 == 0, "pbuf not aligned");

    if (mapped_len - file_offset < reqd_space) {
      fprintf(stderr,
              "pmem file too small. %.2f GB required for hash table "
              "(%zu buckets, bucket size = %zu), but only %.2f GB available\n",
              reqd_space * 1.0 / GB(1), num_total_buckets, sizeof(Bucket),
              mapped_len * 1.0 / GB(1));
    }
    rt_assert(is_pmem == 1, "File is not pmem");

    return pbuf + file_offset;
  }

  // Allocate a hash table with space for \p num_keys keys, and chain overflow
  // room for \p overhead_fraction of the keys
  //
  // The hash table is stored in pmem_file at \p file_offset
  HashMap(std::string pmem_file, size_t file_offset, size_t num_requested_keys,
          double overhead_fraction)
      : pmem_file(pmem_file),
        file_offset(file_offset),
        num_requested_keys(num_requested_keys),
        overhead_fraction(overhead_fraction),
        num_regular_buckets(
            rte_align64pow2(num_requested_keys / kSlotsPerBucket)),
        num_extra_buckets(num_regular_buckets * overhead_fraction),
        num_total_buckets(num_regular_buckets + num_extra_buckets),
        reqd_space(get_required_bytes(num_requested_keys, overhead_fraction)),
        invalid_key(get_invalid_key()) {
    rt_assert(num_requested_keys >= kSlotsPerBucket);  // At least one bucket
    rt_assert(file_offset % 256 == 0);                 // Aligned to pmem block

    printf("Space required = %.1f GB, key capacity = %.1f M\n",
           reqd_space * 1.0 / GB(1), get_key_capacity() / 1000000.0);

    if (kUsePmem) {
      maybe_pbuf = map_pbuf(mapped_len);
    } else {
      maybe_pbuf = reinterpret_cast<uint8_t*>(malloc(reqd_space));
    }

    // Set the committed seq num, and all redo log entry seq nums to zero.
    redo_log = reinterpret_cast<RedoLog*>(maybe_pbuf);
    maybe_pmem_memset_persist(redo_log, 0, sizeof(RedoLog));

    // Initialize buckets
    size_t bucket_offset = roundup<256>(sizeof(RedoLog));
    buckets_ = reinterpret_cast<Bucket*>(&maybe_pbuf[bucket_offset]);

    // extra_buckets_[0] is the actually the last regular bucket. extra_buckets_
    // is indexed starting from one, so the last regular bucket is never used
    // as an extra bucket.
    extra_buckets_ =
        reinterpret_cast<Bucket*>(reinterpret_cast<uint8_t*>(buckets_) +
                                  ((num_regular_buckets - 1) * sizeof(Bucket)));

    // Initialize the free list of extra buckets
    printf("Initializing extra buckets freelist (%zu buckets)\n",
           num_extra_buckets);
    extra_bucket_free_list.reserve(num_extra_buckets);
    for (size_t i = 0; i < num_extra_buckets; i++) {
      extra_bucket_free_list[i] = i + 1;
    }

    reset();
  }

  ~HashMap() {
    if (kUsePmem && maybe_pbuf != nullptr)
      pmem_unmap(maybe_pbuf - file_offset, mapped_len);
  }

  /// Return the total bytes required for a table with \p num_requested_keys
  /// keys and \p overhead_fraction extra buckets. The returned space includes
  /// redo log.
  static size_t get_required_bytes(size_t num_requested_keys,
                                   double overhead_fraction) {
    size_t num_regular_buckets =
        rte_align64pow2(num_requested_keys / kSlotsPerBucket);
    size_t num_extra_buckets = num_regular_buckets * overhead_fraction;
    size_t num_total_buckets = num_regular_buckets + num_extra_buckets;

    return sizeof(RedoLog) + num_total_buckets * sizeof(Bucket);
  }

  static size_t get_hash(const Key& k) {
    return CityHash64(reinterpret_cast<const char*>(&k), sizeof(Key));
  }

  static Key get_invalid_key() {
    Key ret;
    memset(&ret, 0, sizeof(ret));
    return ret;
  }

  // Initialize the contents of both regular and extra buckets
  void reset() {
    double GB_to_memset = num_total_buckets * sizeof(Bucket) * 1.0 / GB(1);
    printf("Resetting hash table. This might take a while (~ %.1f seconds)\n",
           GB_to_memset / 3.0);

    // We need to achieve the following:
    //  * bucket.slot[i].key = invalid_key;
    //  * bucket.next_extra_bucket_idx = 0;
    // pmem_memset_persist() uses SIMD, so it's faster
    maybe_pmem_memset_persist(&buckets_[0], 0,
                              num_total_buckets * sizeof(Bucket));
  }

  void prefetch(uint64_t key_hash) const {
    if (!kEnablePrefetch) return;

    size_t bucket_index = key_hash & (num_regular_buckets - 1);
    const Bucket* bucket = &buckets_[bucket_index];

    __builtin_prefetch(bucket, 0, 0);
    __builtin_prefetch(reinterpret_cast<const char*>(bucket) + 64, 0, 0);
    __builtin_prefetch(reinterpret_cast<const char*>(bucket) + 128, 0, 0);
  }

  // Find a bucket (\p located_bucket) and slot index (return value) in the
  // chain starting from \p bucket that contains \p key. If no such bucket is
  // found, return kSlotsPerBucket.
  size_t find_item_index(Bucket* bucket, const Key& key,
                         Bucket** located_bucket) const {
    Bucket* current_bucket = bucket;

    while (true) {
      for (size_t i = 0; i < kSlotsPerBucket; i++) {
        if (current_bucket->slot_arr[i].key != key) continue;

        *located_bucket = current_bucket;
        return i;
      }

      if (current_bucket->next_extra_bucket_idx == 0) break;
      current_bucket = &extra_buckets_[current_bucket->next_extra_bucket_idx];
    }

    return kSlotsPerBucket;
  }

  // Batched operation that takes in both GETs and SETs. When this function
  // returns, all SETs are persistent in the log.
  //
  // For GETs, value_arr slots contain results. For SETs, they contain the value
  // to SET.
  void batch_op_drain(bool* is_set, const Key* key_arr, Value* value_arr,
                      bool* success_arr, size_t n) {
    size_t keyhash_arr[kMaxBatchSize];

    bool all_gets = true;
    for (size_t i = 0; i < n; i++) {
      keyhash_arr[i] = get_hash(key_arr[i]);
      prefetch(keyhash_arr[i]);

      if (kEnableRedoLogging && is_set[i]) {
        all_gets = false;
        RedoLogEntry v_rle(cur_sequence_number, key_arr[i], value_arr[i]);

        // Drain all pending writes to the table when we reuse log entries
        if (cur_sequence_number % kNumRedoLogEntries == 0) maybe_pmem_drain();

        RedoLogEntry& p_rle =
            redo_log->entries[cur_sequence_number % kNumRedoLogEntries];
        maybe_pmem_memcpy_nodrain(&p_rle, &v_rle, sizeof(v_rle));

        cur_sequence_number++;  // Just the in-memory copy
      }
    }

    if (kEnableRedoLogging && !all_gets) {
      maybe_pmem_drain();  // Block until the redo log entries are persistent
      maybe_pmem_memcpy_persist(&redo_log->committed_seq_num,
                                &cur_sequence_number, sizeof(size_t));
    }

    for (size_t i = 0; i < n; i++) {
      if (is_set[i]) {
        success_arr[i] = set_nodrain(keyhash_arr[i], key_arr[i], value_arr[i]);
      } else {
        success_arr[i] = get(keyhash_arr[i], key_arr[i], value_arr[i]);
      }
    }
  }

  bool get(const Key& key, Value& out_value) const {
    assert(key != invalid_key);
    return get(get_hash(key), key, out_value);
  }

  bool get(uint64_t key_hash, const Key& key, Value& out_value) const {
    assert(key != invalid_key);

    size_t bucket_index = key_hash & (num_regular_buckets - 1);
    Bucket* bucket = &buckets_[bucket_index];

    Bucket* located_bucket;
    size_t item_index = find_item_index(bucket, key, &located_bucket);
    if (item_index == kSlotsPerBucket) return false;

    // printf("get key %zu, bucket %p, index %zu\n",
    //      key, located_bucket, item_index);

    out_value = located_bucket->slot_arr[item_index].value;
    return true;
  }

  bool alloc_extra_bucket(Bucket* bucket) {
    if (extra_bucket_free_list.empty()) return false;
    size_t extra_bucket_index = extra_bucket_free_list.back();
    assert(extra_bucket_index >= 1);
    extra_bucket_free_list.pop_back();

    // This is an eight-byte operation, so no need in redo log
    maybe_pmem_memcpy_persist(&bucket->next_extra_bucket_idx,
                              &extra_bucket_index, sizeof(extra_bucket_index));
    return true;
  }

  // Traverse the bucket chain starting at \p bucket, and fill in \p
  // located_bucket such that located_bucket contains an empty slot. The empty
  // slot index is the return value. This might add a new extra bucket to the
  // chain.
  //
  // Return kSlotsPerBucket if an empty slot is not possible
  size_t get_empty(Bucket* bucket, Bucket** located_bucket) {
    Bucket* current_bucket = bucket;
    while (true) {
      for (size_t i = 0; i < kSlotsPerBucket; i++) {
        if (current_bucket->slot_arr[i].key == invalid_key) {
          *located_bucket = current_bucket;
          return i;
        }
      }
      if (current_bucket->next_extra_bucket_idx == 0) break;
      current_bucket = &extra_buckets_[current_bucket->next_extra_bucket_idx];
    }

    // no space; alloc new extra_bucket
    if (alloc_extra_bucket(current_bucket)) {
      *located_bucket = &extra_buckets_[current_bucket->next_extra_bucket_idx];
      return 0;  // use the first slot (it should be empty)
    } else {
      return kSlotsPerBucket;  // No free extra bucket
    }
  }

  // Set a key-value item without a final sfence
  bool set_nodrain(const Key& key, const Value& value) {
    assert(key != invalid_key);
    return set_nodrain(get_hash(key), key, value);
  }

  // Set a key-value item without a final sfence. If redo logging is disabled, a
  // final sfence is used.
  bool set_nodrain(uint64_t key_hash, const Key& key, const Value& value) {
    assert(key != invalid_key);

    // printf("set key %zu, value %zu\n");

    // XXX Persist
    size_t bucket_index = key_hash & (num_regular_buckets - 1);
    Bucket* bucket = &buckets_[bucket_index];
    Bucket* located_bucket;
    size_t item_index = find_item_index(bucket, key, &located_bucket);

    if (item_index == kSlotsPerBucket) {
      // printf("  not found in bucket %p\n", bucket);
      item_index = get_empty(bucket, &located_bucket);
      if (item_index == kSlotsPerBucket) {
        // printf("  no empty bucket %p\n", bucket);
        return false;
      }
    }

    // printf("  set key %zu, value %zu success. bucket %p, index %zu\n",
    //      key, value, located_bucket, item_index);
    Slot s(key, value);

    if (kEnableRedoLogging) {
      maybe_pmem_memcpy_nodrain(&located_bucket->slot_arr[item_index], &s,
                                sizeof(s));
    } else {
      // For the DRAM case, this can be a bit faster
      maybe_pmem_memcpy_persist(&located_bucket->slot_arr[item_index], &s,
                                sizeof(s));
    }

    return true;
  }

  // Return the number of keys that can be stored in this table
  size_t get_key_capacity() const {
    return num_total_buckets * kSlotsPerBucket;
  };

  // Constructor args
  const std::string pmem_file;      // Name of the pmem file
  const size_t file_offset;         // Offset in file where the table is placed
  const size_t num_requested_keys;  // User's requested key capacity
  const double overhead_fraction;   // User's requested key capacity

  const size_t num_regular_buckets;  // Power-of-two number of main buckets
  const size_t num_extra_buckets;    // num_regular_buckets * overhead_fraction
  const size_t num_total_buckets;    // Sum of regular and extra buckets
  const size_t reqd_space;           // Total bytes needed for the table
  const Key invalid_key;
  Bucket* buckets_ = nullptr;

  // = (buckets + num_buckets); extra_buckets[0] is not used because index 0
  // indicates "no more extra buckets"
  Bucket* extra_buckets_ = nullptr;

  std::vector<size_t> extra_bucket_free_list;

  uint8_t* maybe_pbuf;  // The pmem or DRAM buffer for this table
  size_t mapped_len;    // The length mapped by libpmem
  RedoLog* redo_log;
  size_t cur_sequence_number = 1;
};

}  // namespace mica
