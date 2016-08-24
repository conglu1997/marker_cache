#include <shmbloomfilter.h>

namespace bf {
shm_bloom_filter::shm_bloom_filter(const void_allocator& void_alloc, size_t m,
                                   size_t k)
    : bits_(m, false, void_alloc), num_hashes(k) {}

shm_bloom_filter::shm_bloom_filter(const void_allocator& void_alloc)
    : bits_(void_alloc) {}

bool shm_bloom_filter::lookup(hash128_t hash) const {
    for (int i = 0; i < num_hashes; ++i)
        if (!bits_[(hash.h1 + i * hash.h2) % bits_.size()]) return false;
    return true;
}

void shm_bloom_filter::insert(hash128_t hash) {
    for (int i = 0; i < num_hashes; ++i)
        bits_[(hash.h1 + i * hash.h2) % bits_.size()] = true;
}

hash128_t shm_bloom_filter::hash(char* data, int data_len) {
    return MurmurHash3_x64_128(data, data_len, 0);
}

void shm_bloom_filter::reset() { bits_.reset(); }

}  // namespace bf