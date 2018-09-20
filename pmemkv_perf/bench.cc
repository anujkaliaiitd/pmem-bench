#include <iostream>
#include "../common.h"
#include "/home/akalia/sandbox/pmemkv/src/pmemkv.h"

#define LOG(msg) std::cout << msg << "\n"

using namespace pmemkv;

int main() {
  LOG("Opening datastore");
  KVEngine* kv =
      KVEngine::Open("kvtree3", "/dev/dax0.0", 1073741824);  // 1 GB pool
  assert(kv != nullptr);

  struct timespec start;
  clock_gettime(CLOCK_REALTIME, &start);
  for (size_t i = 0; i < 100000; i++) {
    std::string k = std::to_string(i);
    std::string v = std::to_string(i);
    kv->Put(k, v);
    kv->Put(k, v);
    kv->Put(k, v);
  }

  double seconds = sec_since(start);
  printf("seconds = %.2f\n", seconds);

  LOG("Putting new key");
  KVStatus s = kv->Put("key1", "value1");
  assert(s == OK && kv->Count() == 1);

  LOG("Reading key back");
  string value;
  s = kv->Get("key1", &value);
  assert(s == OK && value == "value1");

  LOG("Iterating existing keys");
  kv->Put("key2", "value2");
  kv->Put("key3", "value3");
  kv->All([](int, const char* k) { LOG("  visited: " << k); });

  LOG("Removing existing key");
  s = kv->Remove("key1");
  assert(s == OK && !kv->Exists("key1"));

  LOG("Closing datastore");
  delete kv;
  return 0;
}
