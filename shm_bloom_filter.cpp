#include "shm_bloom_filter.h"
#include <cassert>
#include <cmath>

namespace bf {

shm_bloom_filter::shm_bloom_filter(const void_allocator &void_alloc, size_t m,
                                   size_t k, size_t seed)
    : bits_(m, void_alloc), hasher_(k, void_alloc, seed) {}

shm_bloom_filter::shm_bloom_filter(shm_bloom_filter &&other)
    : hasher_(std::move(other.hasher_)), bits_(std::move(other.bits_)) {}

bool shm_bloom_filter::lookup(char *data, int data_len) const {
    // the size of a char is 1 byte
    auto digests = hasher_(data, data_len * sizeof(char));
    for (auto d : digests)
        if (!bits_[d % bits_.size()]) return false;
    return true;
}

void shm_bloom_filter::insert(char *data, int data_len) {
    auto digests = hasher_(data, data_len * sizeof(char));
    for (auto d : digests) bits_[d % bits_.size()] = true;
}

// Added for completeness, reset a bloom filter
void shm_bloom_filter::clear() { std::fill(bits_.begin(), bits_.end(), false); }

}  // namespace bf