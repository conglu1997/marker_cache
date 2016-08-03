#ifndef BF_HASH_POLICY_H
#define BF_HASH_POLICY_H

#define BOOST_DATE_TIME_NO_LIB
#include <boost/interprocess/allocators/allocator.hpp>
#include <boost/interprocess/managed_shared_memory.hpp>
#include <functional>
#include "h3.h"

namespace bf {

/// The hash digest type.
typedef size_t digest;

// Allocate our hasher in shared memory as well
typedef boost::interprocess::managed_shared_memory::segment_manager
    segment_manager_t;
typedef boost::interprocess::allocator<void, segment_manager_t> void_allocator;

class hash_function {
   public:
    // Max object size in bytes that we will receive
    constexpr static size_t max_obj_size = 36;

    hash_function(size_t seed);

    size_t operator()(char *data, int size) const;

   private:
    h3<size_t, max_obj_size> h3_;
};

typedef boost::interprocess::allocator<hash_function, segment_manager_t>
    hash_fn_allocator;

/// A hasher which hashes a (*, count) pair *k* times.
class hasher {
   public:
    hasher(size_t k, const void_allocator &void_alloc, size_t seed);
    std::vector<digest> operator()(char *data, int size) const;

   private:
    // Each hash function occupies 72KB
    std::vector<hash_function, hash_fn_allocator> fns_;
};

}  // namespace bf

#endif