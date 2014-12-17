
#ifndef UTIL_SNAPPY_SNAPPY_SINKSOURCE_H_
#define UTIL_SNAPPY_SNAPPY_SINKSOURCE_H_

#include <stddef.h>


namespace snappy {

class Sink {
 public:
  Sink() { }
  virtual ~Sink();

  virtual void Append(const char* bytes, size_t n) = 0;

  virtual char* GetAppendBuffer(size_t length, char* scratch);


 private:
  Sink(const Sink&);
  void operator=(const Sink&);
};

class Source {
 public:
  Source() { }
  virtual ~Source();

  virtual size_t Available() const = 0;

  virtual const char* Peek(size_t* len) = 0;

  virtual void Skip(size_t n) = 0;

 private:
  Source(const Source&);
  void operator=(const Source&);
};

class ByteArraySource : public Source {
 public:
  ByteArraySource(const char* p, size_t n) : ptr_(p), left_(n) { }
  virtual ~ByteArraySource();
  virtual size_t Available() const;
  virtual const char* Peek(size_t* len);
  virtual void Skip(size_t n);
 private:
  const char* ptr_;
  size_t left_;
};

class UncheckedByteArraySink : public Sink {
 public:
  explicit UncheckedByteArraySink(char* dest) : dest_(dest) { }
  virtual ~UncheckedByteArraySink();
  virtual void Append(const char* data, size_t n);
  virtual char* GetAppendBuffer(size_t len, char* scratch);

  char* CurrentDestination() const { return dest_; }
 private:
  char* dest_;
};


}

#endif  // UTIL_SNAPPY_SNAPPY_SINKSOURCE_H_
