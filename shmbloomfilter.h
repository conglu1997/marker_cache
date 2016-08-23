#ifndef BF_BLOOM_FILTER_SHM_H
#define BF_BLOOM_FILTER_SHM_H

#define BOOST_DATE_TIME_NO_LIB
#include <mmh3.h>
#include <boost/dynamic_bitset.hpp>
#include <boost/interprocess/allocators/allocator.hpp>
#include <boost/interprocess/managed_shared_memory.hpp>

namespace bf {

typedef boost::interprocess::managed_shared_memory::segment_manager
    segment_manager_t;
typedef boost::interprocess::allocator<void, segment_manager_t> void_allocator;

typedef size_t block_t;
// Do rebinds to allow changing the base void_allocator
typedef void_allocator::rebind<block_t>::other block_allocator;
typedef boost::dynamic_bitset<block_t, block_allocator> bitset;

class shm_bloom_filter {
   public:
    shm_bloom_filter(const void_allocator& void_alloc, size_t m, size_t k);

    bool lookup(hash_t hash) const;
    void insert(hash_t hash);
    static hash_t hash(char* data, int data_len);

    void reset();

   private:
    bitset bits_;
    int num_hashes;
};

}  // namespace bf

#endif
