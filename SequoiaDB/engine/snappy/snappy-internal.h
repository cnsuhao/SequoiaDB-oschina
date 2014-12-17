
#ifndef UTIL_SNAPPY_SNAPPY_INTERNAL_H_
#define UTIL_SNAPPY_SNAPPY_INTERNAL_H_

#include "snappy-stubs-internal.h"

namespace snappy {
namespace internal {

class WorkingMemory {
 public:
  WorkingMemory() : large_table_(NULL) { }
  ~WorkingMemory() { delete[] large_table_; }

  uint16* GetHashTable(size_t input_size, int* table_size);

 private:
  uint16 small_table_[1<<10];    // 2KB
  uint16* large_table_;          // Allocated only when needed

  DISALLOW_COPY_AND_ASSIGN(WorkingMemory);
};

char* CompressFragment(const char* input,
                       size_t input_length,
                       char* op,
                       uint16* table,
                       const int table_size);

#if defined(ARCH_K8)
static inline int FindMatchLength(const char* s1,
                                  const char* s2,
                                  const char* s2_limit) {
  assert(s2_limit >= s2);
  int matched = 0;

  while (PREDICT_TRUE(s2 <= s2_limit - 8)) {
    if (PREDICT_FALSE(UNALIGNED_LOAD64(s2) == UNALIGNED_LOAD64(s1 + matched))) {
      s2 += 8;
      matched += 8;
    } else {
      uint64 x = UNALIGNED_LOAD64(s2) ^ UNALIGNED_LOAD64(s1 + matched);
      int matching_bits = Bits::FindLSBSetNonZero64(x);
      matched += matching_bits >> 3;
      return matched;
    }
  }
  while (PREDICT_TRUE(s2 < s2_limit)) {
    if (PREDICT_TRUE(s1[matched] == *s2)) {
      ++s2;
      ++matched;
    } else {
      return matched;
    }
  }
  return matched;
}
#else
static inline int FindMatchLength(const char* s1,
                                  const char* s2,
                                  const char* s2_limit) {
  assert(s2_limit >= s2);
  int matched = 0;

  while (s2 <= s2_limit - 4 &&
         UNALIGNED_LOAD32(s2) == UNALIGNED_LOAD32(s1 + matched)) {
    s2 += 4;
    matched += 4;
  }
  if (LittleEndian::IsLittleEndian() && s2 <= s2_limit - 4) {
    uint32 x = UNALIGNED_LOAD32(s2) ^ UNALIGNED_LOAD32(s1 + matched);
    int matching_bits = Bits::FindLSBSetNonZero(x);
    matched += matching_bits >> 3;
  } else {
    while ((s2 < s2_limit) && (s1[matched] == *s2)) {
      ++s2;
      ++matched;
    }
  }
  return matched;
}
#endif

}  // end namespace internal
}  // end namespace snappy

#endif  // UTIL_SNAPPY_SNAPPY_INTERNAL_H_
