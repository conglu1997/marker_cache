#include "hash.h"
#include <cassert>

namespace bf {

hash_function::hash_function(size_t seed) : h3_(seed) {}

size_t hash_function::operator()(char *data, int size) const {
    // Current max is 36 bytes - settable in hash.h
    if (size > max_obj_size) throw std::runtime_error("Object too large!");
    return size == 0 ? 0 : h3_(data, size);
}

hasher::hasher(size_t k, const void_allocator &void_alloc, size_t seed)
    : fns_(void_alloc) {
    assert(k > 0);
    std::minstd_rand0 prng(seed);
	fns_.reserve(k);
    for (size_t i = 0; i < k; ++i) fns_.emplace_back(prng());
}

std::vector<digest> hasher::operator()(char *data, int size) const {
    std::vector<digest> d(fns_.size());
    for (size_t i = 0; i < fns_.size(); ++i) d[i] = fns_[i](data, size);
    return d;
}

}  // namespace bf