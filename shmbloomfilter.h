#ifndef BF_BLOOM_FILTER_SHM_H
#define BF_BLOOM_FILTER_SHM_H

#define BOOST_DATE_TIME_NO_LIB
#include <mmh3.h>
#include <boost/dynamic_bitset.hpp>
#include <boost/interprocess/allocators/allocator.hpp>
#include <boost/interprocess/managed_shared_memory.hpp>
#include <boost/serialization/vector.hpp>

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
    shm_bloom_filter(const void_allocator& void_alloc);

    bool lookup(hash128_t hash) const;
    void insert(hash128_t hash);
    static hash128_t hash(char* data, int data_len);

    void reset();

   private:
    bitset bits_;
    int num_hashes;

    friend class boost::serialization::access;
    template <class Archive>
    void serialize(Archive& ar, const unsigned int version) {
        ar& bits_;
        ar& num_hashes;
    }
};

}  // namespace bf

// Serialization support for dynamic_bitset
namespace boost {
namespace serialization {

template <typename Ar, typename Block, typename Alloc>
void save(Ar& ar, dynamic_bitset<Block, Alloc> const& bs, unsigned) {
    size_t num_bits = bs.size();
    std::vector<Block> blocks(bs.num_blocks());
    to_block_range(bs, blocks.begin());

    ar& num_bits& blocks;
}

template <typename Ar, typename Block, typename Alloc>
void load(Ar& ar, dynamic_bitset<Block, Alloc>& bs, unsigned) {
    size_t num_bits;
    std::vector<Block> blocks;
    ar& num_bits& blocks;

    bs.resize(num_bits);
    from_block_range(blocks.begin(), blocks.end(), bs);
    bs.resize(num_bits);
}

template <typename Ar, typename Block, typename Alloc>
void serialize(Ar& ar, dynamic_bitset<Block, Alloc>& bs, unsigned version) {
    split_free(ar, bs, version);
}
}  // namespace serialization
}  // namespace boost

#endif
