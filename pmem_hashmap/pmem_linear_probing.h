/*
Copyright (c) 2017 Erik Rigtorp <erik@rigtorp.se>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
 */

#pragma once

#include <city.h>
#include <libpmem.h>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <vector>
#include "../common.h"

template <typename Key, typename Value, size_t num_slots>
class HashMap {
  static_assert(is_power_of_two(num_slots), "");
  enum class State : size_t { kEmpty = 0, kFull, kDelete };

  class Slot {
   public:
    State state;
    Key key;
    Value value;

    Slot(State state, Key key, Value value)
        : state(state), key(key), value(value) {}
    Slot() {}
  };

 public:
  HashMap(std::string pmem_file) {
    int is_pmem;
    slot_arr = reinterpret_cast<Slot *>(
        pmem_map_file(pmem_file.c_str(), 0 /* length */, 0 /* flags */, 0666,
                      &mapped_len, &is_pmem));

    rt_assert(slot_arr != nullptr,
              "pmem_map_file() failed. " + std::string(strerror(errno)));
    rt_assert(mapped_len >= num_slots * sizeof(Slot),
              "pmem file too small " + std::to_string(mapped_len));
    rt_assert(is_pmem == 1, "File is not pmem");

    // This marks all slots as empty
    pmem_memset_persist(slot_arr, 0, num_slots * sizeof(Slot));
  }

  ~HashMap() {
    if (slot_arr != nullptr) pmem_unmap(slot_arr, mapped_len);
  }

  static size_t get_hash(const Key &k) {
    return CityHash64(reinterpret_cast<const char *>(&k), sizeof(Key));
  }

  bool insert(const Key &key, const Key &value) {
    const size_t hash = get_hash(key);

    for (size_t offset = 0; offset < num_slots; offset++) {
      auto &slot = slot_arr[(hash + offset) % num_slots];

      if (slot.key == key) return true;

      if (slot.state != State::kFull) {
        Slot to_insert(State::kFull, key, value);
        slot = to_insert;
        return true;
      }
    }

    return false;
  }

  bool get(const Key &key, const Value &value) {
    const size_t hash = get_hash(key);

    for (size_t offset = 0; offset < num_slots; offset++) {
      auto &slot = slot_arr[(hash + offset) % num_slots];

      if (slot.state == State::kFull && slot.key == key) {
        value = slot.value;
        return true;
      }

      if (slot.state == State::kEmpty) return false;
    }
  }

 private:
  size_t mapped_len;
  Slot *slot_arr;
};
