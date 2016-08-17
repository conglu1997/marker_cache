#include <shmbloomfilter.h>

namespace bf {

shm_bloom_filter::shm_bloom_filter(const void_allocator &void_alloc, size_t m,
                                   size_t k)
    : bits_(m, false, void_alloc), num_hashes(k) {}

bool shm_bloom_filter::lookup(char *data, int data_len) const {
    uint64_t *hash = new uint64_t[2];
    MurmurHash3_x64_128(data, data_len, 0, hash);

    for (int i = 0; i < num_hashes; ++i)
        if (!bits_[(hash[0] + i * hash[1]) % bits_.size()]) {
            delete[] hash;
            return false;
        }

    delete[] hash;
    return true;
}

void shm_bloom_filter::insert(char *data, int data_len) {
    uint64_t *hash = new uint64_t[2];
    MurmurHash3_x64_128(data, data_len, 0, hash);

    for (int i = 0; i < num_hashes; ++i)
        bits_[(hash[0] + i * hash[1]) % bits_.size()] = true;

    delete[] hash;
}

void shm_bloom_filter::reset() { bits_.reset(); }

}  // namespace bf