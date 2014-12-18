
#ifndef UTIL_SNAPPY_SNAPPY_H__
#define UTIL_SNAPPY_SNAPPY_H__

#include <stddef.h>
#include <string>

#include "snappy-stubs-public.h"

namespace snappy {
  class Source;
  class Sink;


  size_t Compress(Source* source, Sink* sink);

  bool GetUncompressedLength(Source* source, uint32* result);


  size_t Compress(const char* input, size_t input_length, string* output);

  bool Uncompress(const char* compressed, size_t compressed_length,
                  string* uncompressed);



  void RawCompress(const char* input,
                   size_t input_length,
                   char* compressed,
                   size_t* compressed_length);

  bool RawUncompress(const char* compressed, size_t compressed_length,
                     char* uncompressed);

  bool RawUncompress(Source* compressed, char* uncompressed);

  size_t MaxCompressedLength(size_t source_bytes);

  bool GetUncompressedLength(const char* compressed, size_t compressed_length,
                             size_t* result);

  bool IsValidCompressedBuffer(const char* compressed,
                               size_t compressed_length);

  static const int kBlockLog = 16;
  static const size_t kBlockSize = 1 << kBlockLog;

  static const int kMaxHashTableBits = 14;
  static const size_t kMaxHashTableSize = 1 << kMaxHashTableBits;

}  // end namespace snappy


#endif  // UTIL_SNAPPY_SNAPPY_H__
