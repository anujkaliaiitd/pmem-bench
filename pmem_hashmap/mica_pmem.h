#pragma once

#include <assert.h>
#include <city.h>
#include <libpmem.h>
#include "../common.h"

namespace mica {

static constexpr size_t kSlotsPerBucket = 8;
static constexpr size_t kMaxBatchSize = 16;  // = number of redo log entries

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
    Key key_arr[kSlotsPerBucket];
    Value val_arr[kSlotsPerBucket];
  };

  class RedoLogEntry {
   public:
    static constexpr size_t kInvalidOperationNumber = 0;

    size_t operation_number;  // Operation number of this entry. Zero is invalid
    Key key;
    Value value;
    size_t valid;

    // Align to 256 bytes
    char padding[256 - (sizeof(size_t) + sizeof(Key) + sizeof(Value) +
                        sizeof(size_t))];

    RedoLogEntry(size_t operation_number, Key key, Value value)
        : operation_number(operation_number),
          key(key),
          value(value),
          valid(0) {}
    RedoLogEntry() {}
  };
  static_assert(sizeof(RedoLogEntry) == 256, "");

  // Allocate a hash table with space for \p num_keys keys, and chain overflow
  // room for \p overhead_fraction of the keys
  HashMap(std::string pmem_file, size_t num_keys, double overhead_fraction)
      : num_regular_buckets(rte_align64pow2(num_keys / kSlotsPerBucket)),
        num_extra_buckets(num_regular_buckets * overhead_fraction),
        num_total_buckets(num_regular_buckets + num_extra_buckets),
        invalid_key(get_invalid_key()) {
    rt_assert(num_keys >= kSlotsPerBucket);  // At least one bucket
    int is_pmem;
    uint8_t* pbuf = reinterpret_cast<uint8_t*>(
        pmem_map_file(pmem_file.c_str(), 0 /* length */, 0 /* flags */, 0666,
                      &mapped_len, &is_pmem));

    rt_assert(pbuf != nullptr,
              "pmem_map_file() failed. " + std::string(strerror(errno)));

    const size_t reqd_space = kMaxBatchSize * sizeof(RedoLogEntry) +
                              num_total_buckets * sizeof(Bucket);
    if (mapped_len < reqd_space) {
      fprintf(stderr,
              "pmem file too small. %.2f GB required for hash table "
              "(%zu buckets, bucket size = %zu), but only %.2f GB available\n",
              reqd_space * 1.0 / GB(1), num_total_buckets, sizeof(Bucket),
              mapped_len * 1.0 / GB(1));
    }
    rt_assert(is_pmem == 1, "File is not pmem");

    // Initialize redo log entries
    redo_log_entry_arr = reinterpret_cast<RedoLogEntry*>(pbuf);
    for (size_t i = 0; i < kMaxBatchSize; i++) {
      redo_log_entry_arr[i].operation_number =
          RedoLogEntry::kInvalidOperationNumber;
    }
    pmem_flush(&redo_log_entry_arr[0], kMaxBatchSize * sizeof(RedoLogEntry));

    // Initialize buckets
    size_t bucket_offset = roundup<256>(kMaxBatchSize * sizeof(RedoLogEntry));
    buckets_ = reinterpret_cast<Bucket*>(&pbuf[bucket_offset]);

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
      extra_bucket_free_list.push_back(i + 1);  // Extra-buckets are 1-based
    }

    reset();
  }

  ~HashMap() {
    // redo_log_entry_arr = pbuf
    if (redo_log_entry_arr != nullptr)
      pmem_unmap(redo_log_entry_arr, mapped_len);
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
    printf("Resetting hash table. This might take a while.\n");
    double print_fraction = 0.01;
    for (size_t bkt_i = 0; bkt_i < num_total_buckets; bkt_i++) {
      if (bkt_i >= num_extra_buckets * print_fraction) {
        printf("%.2f fraction done\n", print_fraction);
        print_fraction += 0.01;
      }

      Bucket& bucket = buckets_[bkt_i];

      for (size_t i = 0; i < kSlotsPerBucket; i++) {
        // XXX: Persist
        bucket.key_arr[i] = invalid_key;
        bucket.next_extra_bucket_idx = 0;
      }
    }
  }

  void prefetch_table(uint64_t key_hash) const {
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
        if (current_bucket->key_arr[i] != key) continue;

        *located_bucket = current_bucket;
        return i;
      }

      if (current_bucket->next_extra_bucket_idx == 0) break;
      current_bucket = &extra_buckets_[current_bucket->next_extra_bucket_idx];
    }

    return kSlotsPerBucket;
  }

  bool get(const Key& key, Value& out_value) const {
    assert(key != invalid_key);
    get(get_hash(key), key, out_value);
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

    out_value = located_bucket->val_arr[item_index];
    return true;
  }

  bool alloc_extra_bucket(Bucket* bucket) {
    if (extra_bucket_free_list.empty()) return false;
    size_t extra_bucket_index = extra_bucket_free_list.back();
    assert(extra_bucket_index >= 1);
    extra_bucket_free_list.pop_back();

    bucket->next_extra_bucket_idx = extra_bucket_index;
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
        if (current_bucket->key_arr[i] == invalid_key) {
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

  bool set(const Key& key, const Value& value) {
    assert(key != invalid_key);
    set(get_hash(key), key, value);
  }

  bool set(uint64_t key_hash, const Key& key, const Value& value) {
    assert(key != invalid_key);

    // XXX Persist
    size_t bucket_index = key_hash & (num_regular_buckets - 1);
    Bucket* bucket = &buckets_[bucket_index];
    Bucket* located_bucket;
    size_t item_index = find_item_index(bucket, key, &located_bucket);

    if (item_index == kSlotsPerBucket) {
      item_index = get_empty(bucket, &located_bucket);
      if (item_index == kSlotsPerBucket) return false;
    }

    // printf("set key %zu, value %zu, bucket %p, index %zu\n",
    //       key, value, located_bucket, item_index);
    located_bucket->key_arr[item_index] = key;
    located_bucket->val_arr[item_index] = value;

    return true;
  }

  void print_buckets() const;
  void print_stats() const;
  void reset_stats(bool reset_count);

  void print_bucket(const Bucket* bucket) const;
  void print_bucket_occupancy();

  // Return the number of keys that can be stored in this table
  size_t get_key_capacity() const {
    return num_total_buckets * kSlotsPerBucket;
  };

  std::string name;  // Name of the table
  const size_t num_regular_buckets;
  const size_t num_extra_buckets;
  const size_t num_total_buckets;  // Total of regular and extra buckets
  const Key invalid_key;
  Bucket* buckets_ = nullptr;

  // = (buckets + num_buckets); extra_buckets[0] is not used because index 0
  // indicates "no more extra buckets"
  Bucket* extra_buckets_ = nullptr;

  std::vector<size_t> extra_bucket_free_list;

  size_t mapped_len;
  RedoLogEntry* redo_log_entry_arr;
  size_t cur_operation_number = RedoLogEntry::kInvalidOperationNumber;
};

}  // namespace mica
