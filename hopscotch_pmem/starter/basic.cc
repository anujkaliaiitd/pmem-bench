#include <stdio.h>
#include "hopscotch.h"

/* Macro for testing */
#define TEST_FUNC(str, func, ret) \
  do {                            \
    printf("%s: ", str);          \
    if (0 == func()) {            \
      printf("passed");           \
    } else {                      \
      printf("failed");           \
      ret = -1;                   \
    }                             \
    printf("\n");                 \
  } while (0)

#define TEST_PROGRESS() \
  do {                  \
    printf(".");        \
    fflush(stdout);     \
  } while (0)

/*
 * Initialization test
 */
static int test_init(void) {
  struct hopscotch::table_t *ht;

  ht = hopscotch::init(NULL, 8);
  if (NULL == ht) {
    return -1;
  }

  TEST_PROGRESS();

  hopscotch::release(ht);

  return 0;
}

/*
 * Lookup test
 */
static int test_lookup(void) {
  struct hopscotch::table_t *ht;
  uint8_t key0[] = {0x01, 0x23, 0x45, 0x67, 0x89, 0x01, 0x23, 0x45};
  uint8_t key1[] = {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08};
  int ret;
  void *val;

  /* Initialize */
  ht = hopscotch::init(NULL, 8);
  if (NULL == ht) {
    return -1;
  }

  /* No entry */
  val = hopscotch::lookup(ht, key0);
  if (NULL != val) {
    /* Failed */
    return -1;
  }

  TEST_PROGRESS();

  /* Insert */
  ret = hopscotch::update(ht, key1, key1);
  if (ret < 0) {
    /* Failed to insert */
    return -1;
  }

  /* Lookup */
  val = hopscotch::lookup(ht, key1);
  if (val != key1) {
    /* Failed */
    return -1;
  }

  /* Lookup */
  val = hopscotch::remove(ht, key1);
  if (val != key1) {
    /* Failed */
    return -1;
  }

  /* Lookup */
  val = hopscotch::lookup(ht, key1);
  if (NULL != val) {
    /* Failed */
    return -1;
  }

  /* Release */
  hopscotch::release(ht);

  return 0;
}

/*
 * Main routine for the basic test
 */
int main(int, const char *const[]) {
  int ret;

  /* Reset */
  ret = 0;

  /* Run tests */
  TEST_FUNC("init", test_init, ret);
  TEST_FUNC("lookup", test_lookup, ret);

  return 0;
}
