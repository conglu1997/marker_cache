#ifndef BF_BLOOM_FILTER_SHM_H
#define BF_BLOOM_FILTER_SHM_H

#define BOOST_DATE_TIME_NO_LIB
#include <boost/interprocess/allocators/allocator.hpp>
#include <boost/interprocess/containers/map.hpp>
#include <boost/interprocess/managed_shared_memory.hpp>
#include <vector>
#include "hash.h"

namespace bf {

typedef boost::interprocess::managed_shared_memory::segment_manager
    segment_manager_t;
typedef boost::interprocess::allocator<void, segment_manager_t> void_allocator;
typedef boost::interprocess::allocator<bool, segment_manager_t> bool_allocator;
// Use the vector<bool> 1-bit per bool optimisation
// May want to consider boost::dynamic_bitset as well
typedef std::vector<bool, bool_allocator> bitset;

class shm_bloom_filter {
   public:
    shm_bloom_filter(const void_allocator &void_alloc, hasher h, size_t cells,
                     bool partition = false);
    shm_bloom_filter(const void_allocator &void_alloc, double fp,
                     size_t capacity, size_t seed, bool double_hashing,
                     bool partition);
    shm_bloom_filter(shm_bloom_filter &&);

    bool lookup(char *data, int data_len) const;
    void add(char *data, int data_len);
    void clear();

   private:
    static size_t m(double fp, size_t capacity);
    static size_t k(size_t cells, size_t capacity);
    hasher hasher_;
    bool partition_;
    bitset bits_;
};

typedef std::pair<const int, shm_bloom_filter> bf_pair;
typedef boost::interprocess::allocator<bf_pair, segment_manager_t>
    bf_pair_allocator;
typedef boost::interprocess::map<int, shm_bloom_filter, std::less<int>,
                                 bf_pair_allocator>
    id_bf_map;

}  // namespace bf

#endif
