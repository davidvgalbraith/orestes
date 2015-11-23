#pragma once


#include "bubo-types.h"

#define BLOB_SIZE (20 << 20)

class BlobStore {
public:
    BlobStore() : blob_size_(BLOB_SIZE),
                  blobs_(new Blob(blob_size_)),
                  curr_blob_(blobs_),
                  curr_blob_mem_end_(blobs_->mem_ + BLOB_SIZE),
                  curr_blob_mem_pos_(blobs_->mem_) {}

    BlobStore(size_t blob_size) : blob_size_(blob_size),
                                  blobs_(new Blob(blob_size_)),
                                  curr_blob_(blobs_),
                                  curr_blob_mem_end_(blobs_->mem_ + blob_size),
                                  curr_blob_mem_pos_(blobs_->mem_) {}
    virtual ~BlobStore() {

        remove_blob(blobs_);
        curr_blob_ = blobs_ = NULL;
        curr_blob_mem_pos_ = curr_blob_mem_end_ = NULL;
    }

    BYTE* add(const BYTE* seq_str, int len);

    void stats(uint64_t* allocated_bytes, uint64_t* used_bytes) const;

protected:
    struct Blob {
        BYTE* mem_;
        Blob* next_;
        Blob(size_t size) : mem_(new BYTE[size]), next_(NULL) {}
        ~Blob() {
            delete [] mem_;
        }
    };

    const size_t blob_size_;

    Blob *blobs_,
         *curr_blob_;

    BYTE *curr_blob_mem_end_,
         *curr_blob_mem_pos_;


    void remove_blob(Blob* p);
};




