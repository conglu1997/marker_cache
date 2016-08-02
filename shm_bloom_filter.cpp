#include "shm_bloom_filter.h"
#include <cassert>
#include <cmath>

namespace bf {

shm_bloom_filter::shm_bloom_filter(const void_allocator &void_alloc, hasher h,
                                   size_t cells, bool partition)
    : hasher_(std::move(h)), bits_(cells, void_alloc), partition_(partition) {}

shm_bloom_filter::shm_bloom_filter(const void_allocator &void_alloc, double fp,
                                   size_t capacity, size_t seed,
                                   bool double_hashing, bool partition)
    : bits_(void_alloc) {
    auto required_cells = m(fp, capacity);
    auto optimal_k = k(required_cells, capacity);
    bits_.resize(required_cells);
    hasher_ = make_hasher(optimal_k, seed, double_hashing);
}

shm_bloom_filter::shm_bloom_filter(shm_bloom_filter &&other)
    : hasher_(std::move(other.hasher_)), bits_(std::move(other.bits_)) {}

bool shm_bloom_filter::lookup(char *data, int data_len) const {
    // the size of a char is 1 byte
    auto digests = hasher_({data, data_len / sizeof(char)});
    // assert(bits_.size() % digests.size() == 0);
    if (partition_) {
        auto parts = bits_.size() / digests.size();
        for (auto i = 0; i < digests.size(); ++i)
            if (!bits_[i * parts + (digests[i] % parts)]) return false;
    } else {
        for (auto d : digests)
            if (!bits_[d % bits_.size()]) return false;
    }

    return true;
}

void shm_bloom_filter::add(char *data, int data_len) {
    auto digests = hasher_({data, data_len * sizeof(char)});
    // assert(bits_.size() % digests.size() == 0);
    if (partition_) {
        auto parts = bits_.size() / digests.size();
        for (auto i = 0; i < digests.size(); ++i)
            bits_[i * parts + (digests[i] % parts)] = true;
    } else {
        for (auto d : digests) bits_[d % bits_.size()] = true;
    }
}

void shm_bloom_filter::clear() { std::fill(bits_.begin(), bits_.end(), false); }

size_t shm_bloom_filter::m(double fp, size_t capacity) {
    auto ln2 = std::log(2);
    return std::ceil(-(capacity * std::log(fp) / ln2 / ln2));
}

size_t shm_bloom_filter::k(size_t cells, size_t capacity) {
    auto frac = static_cast<double>(cells) / static_cast<double>(capacity);
    return std::ceil(frac * std::log(2));
}
}
