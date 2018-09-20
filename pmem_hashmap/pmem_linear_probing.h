#pragma once

#include <city.h>
#include <libpmem.h>
#include "../common.h"

template <typename Key, typename Value, size_t kNumSlots>
class HashMap {
  static_assert(is_power_of_two(kNumSlots), "");
  enum class State : size_t { kEmpty = 0, kFull, kDelete };  // Slot state

  class Slot {
   public:
    State state;
    Key key;
    Value value;

    Slot(State state, Key key, Value value)
        : state(state), key(key), value(value) {}
    Slot() {}
  };

  class RedoLogEntry {
    Key key;
    Value value;
  };

  static constexpr size_t kMaxBatchSize = 16;  // = number of redo log entries

 public:
  HashMap(std::string pmem_file) {
    int is_pmem;
    uint8_t *pbuf = reinterpret_cast<uint8_t *>(
        pmem_map_file(pmem_file.c_str(), 0 /* length */, 0 /* flags */, 0666,
                      &mapped_len, &is_pmem));

    rt_assert(pbuf != nullptr,
              "pmem_map_file() failed. " + std::string(strerror(errno)));
    rt_assert(mapped_len >= kMaxBatchSize * sizeof(RedoLogEntry) +
                                kNumSlots * sizeof(Slot),
              "pmem file too small " + std::to_string(mapped_len));
    rt_assert(is_pmem == 1, "File is not pmem");

    redo_log_entry_arr = reinterpret_cast<RedoLogEntry *>(pbuf);

    size_t slot_arr_offset = roundup<256>(kMaxBatchSize * sizeof(RedoLogEntry));
    slot_arr = reinterpret_cast<Slot *>(&pbuf[slot_arr_offset]);

    // This marks all slots as empty
    pmem_memset_persist(slot_arr, 0, kNumSlots * sizeof(Slot));
  }

  ~HashMap() {
    if (slot_arr != nullptr) pmem_unmap(slot_arr, mapped_len);
  }

  static size_t get_hash(const Key &k) {
    return CityHash64(reinterpret_cast<const char *>(&k), sizeof(Key));
  }

  void prefetch(const Key &key) {
    const size_t idx = get_hash(key) % kNumSlots;
    __builtin_prefetch(&slot_arr[idx]);
  }

  inline bool insert(const Key &key, const Key &value) {
    const size_t hash = get_hash(key);

    for (size_t offset = 0; offset < kNumSlots; offset++) {
      auto &slot = slot_arr[(hash + offset) % kNumSlots];

      if (slot.key == key) return true;

      if (slot.state != State::kFull) {
        Slot to_insert(State::kFull, key, value);
        slot = to_insert;
        return true;
      }
    }

    return false;
  }

  inline bool get(const Key &key, Value &value) {
    const size_t hash = get_hash(key);

    for (size_t offset = 0; offset < kNumSlots; offset++) {
      auto &slot = slot_arr[(hash + offset) % kNumSlots];

      if (slot.state == State::kFull && slot.key == key) {
        value = slot.value;
        return true;
      }

      if (slot.state == State::kEmpty) return false;
    }

    return false;
  }

 private:
  size_t mapped_len;
  RedoLogEntry *redo_log_entry_arr;
  Slot *slot_arr;
};
