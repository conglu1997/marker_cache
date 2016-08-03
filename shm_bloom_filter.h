#ifndef BF_BLOOM_FILTER_SHM_H
#define BF_BLOOM_FILTER_SHM_H

#include <boost/interprocess/containers/map.hpp>
#include "hash.h"

namespace bf {

typedef boost::interprocess::allocator<bool, segment_manager_t> bool_allocator;
// Use the vector<bool> 1-bit per bool optimisation
// May want to consider boost::dynamic_bitset as well
typedef std::vector<bool, bool_allocator> bitset;

class shm_bloom_filter {
   public:
    shm_bloom_filter::shm_bloom_filter(const void_allocator &void_alloc,
                                       size_t m, size_t k, size_t seed);
    shm_bloom_filter(shm_bloom_filter &&);

    bool lookup(char *data, int data_len) const;
    void add(char *data, int data_len);
    void clear();

   private:
    hasher hasher_;
    bitset bits_;
};

}  // namespace bf

#endif
