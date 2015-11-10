#include "blob-store.h"
#include "utils.h"

BYTE* BlobStore::add(const BYTE* seq_str, int len) {
	if (curr_blob_mem_end_ - curr_blob_mem_pos_ < len ) {
		curr_blob_->next_ = new Blob(blob_size_);
		curr_blob_ = curr_blob_->next_;
		curr_blob_mem_pos_ = curr_blob_->mem_;
		curr_blob_mem_end_ = curr_blob_mem_pos_ + blob_size_;
	}
	BYTE* ret_ptr = curr_blob_mem_pos_;
	memcpy(curr_blob_mem_pos_, seq_str, len);
	curr_blob_mem_pos_ += len;

	return ret_ptr;
}

void BlobStore::remove_blob(Blob* p) {
	if (!p) {
		return;
	} else {
		remove_blob(p->next_);
		delete p;
	}
}

void BlobStore::stats(uint64_t* allocated_bytes, uint64_t* used_bytes) const {

	Blob* b = blobs_;
	size_t num_blobs = 0;
	while (b != NULL) {
		b = b->next_;
		num_blobs ++;
	}

	*allocated_bytes = num_blobs * blob_size_;
	*used_bytes = (num_blobs - 1) * blob_size_ + (size_t)(curr_blob_mem_pos_ - curr_blob_->mem_);
}