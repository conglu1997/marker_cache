#ifndef MARKER_CACHE_H
#define MARKER_CACHE_H
#include "shm_bloom_filter.h"

class marker_cache {
    // Suitable identification scheme for subtable division here
    // To replace the id scheme, simply change this typedef and provide a
    // comparator for the map, currently std::less<int>
    typedef int bloom_filter_id;
    typedef std::less<int> id_comparator;

    typedef std::pair<const bloom_filter_id, bf::shm_bloom_filter> bf_pair;
    typedef boost::interprocess::allocator<bf_pair, bf::segment_manager_t>
        bf_pair_allocator;
    typedef boost::interprocess::map<bloom_filter_id, bf::shm_bloom_filter,
                                     id_comparator, bf_pair_allocator>
        id_bf_map;

    // API for managing shared memory and retrieving handles to data
    // Takes identifier of the bloom filter (per subtable, per marker type)
    // For our purposes, identifier is just the marker type (int32)
   public:
    // max 4.2 billion bytes (unsigned integer)
    marker_cache(size_t bytes);
    ~marker_cache();
    // Create new cache, size bits, fp false-positive rate
    // max 4.2 billion items
    void create(bloom_filter_id id, double fp, size_t capacity, size_t seed = 0,
                bool double_hashing = true, bool partition = true);

    // data_len is num of chars (bytes)
    bool lookup_from(bloom_filter_id id, char *data, int data_len) const;

    void insert_into(bloom_filter_id id, char *data, int data_len);

    void remove(bloom_filter_id id);

   private:
    boost::interprocess::managed_shared_memory segment_;
    id_bf_map *data_;
    bf::void_allocator get_allocator();
};

#endif