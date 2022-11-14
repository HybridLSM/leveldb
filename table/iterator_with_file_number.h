#ifndef STORAGE_LEVELDB_INCLUDE_ITERATOR_WITH_FILE_NUMBER_H_
#define STORAGE_LEVELDB_INCLUDE_ITERATOR_WITH_FILE_NUMBER_H_

#include "leveldb/iterator.h"
#include "util/coding.h"

#include <cstring>

namespace leveldb {

class IteratorWithFileNumber : public Iterator {
  public:
    IteratorWithFileNumber(const uint64_t file_number, Iterator* data_iter) 
                          : file_number_(file_number),
                            data_iter_(data_iter) {}
    ~IteratorWithFileNumber() { delete data_iter_; }

    bool Valid() const override { return data_iter_->Valid(); }
    void Seek(const Slice& target) override { data_iter_->Seek(target); }
    void SeekToFirst() override { data_iter_->SeekToFirst(); }
    void SeekToLast() override { data_iter_->SeekToLast(); }
    void Next() override { data_iter_->Next(); }
    void Prev() override { data_iter_->Prev(); }
    Status status() const override { return data_iter_->status(); }

    Slice key() const override {
      Slice data_key = data_iter_->key();
      memcpy(key_buf, data_key.data(), data_key.size());
      EncodeFixed64(key_buf + data_key.size(), file_number_);
      return Slice(key_buf, data_key.size() + 8);
    } 
    Slice value () const override { return data_iter_->value(); }

  private:
    const uint64_t file_number_;
    Iterator* data_iter_;
    mutable char key_buf[128];
}; 

Iterator* NewIteratorWithFileNumber(const uint64_t file_number, Iterator* data_iter) {
  return new IteratorWithFileNumber(file_number, data_iter);
}

}

#endif